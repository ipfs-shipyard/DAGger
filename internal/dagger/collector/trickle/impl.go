package trickle

import (
	"math"

	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
)

type config struct {
	UnixfsNulLeaf       bool `getopt:"--unixfs-nul-leaf-compat Flag to force convergence with go-ipfs *specifically* when encoding a 0-length stream (override encoder leaf-type)"`
	MaxDirectLeaves     int  `getopt:"--max-direct-leaves      Maximum leaves per node (IPFS default: 174)"`          // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/helpers/helpers.go#L26
	MaxSiblingSubgroups int  `getopt:"--max-sibling-subgroups  Maximum same-depth-groups per node (IPFS default: 4)"` // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/trickle/trickledag.go#L34
	descentPrealloc     int
}
type state struct {
	leafCount    int
	levelCutoffs []int
	tail         *trickleNode
}
type trickleNode struct {
	depth        int
	directLeaves []*dgrblock.Header
	parent       *trickleNode
}
type collector struct {
	config
	*dgrcollector.DaggerConfig
	state
}

func (co *collector) FlushState() *dgrblock.Header {
	if co.tail == nil {
		return nil
	}

	// it is critical to reset the collector state when we are done - we reuse the object!
	defer func() { co.state = state{} }()

	// special case to match go-ipfs on zero-length streams
	if co.UnixfsNulLeaf &&
		co.leafCount == 1 &&
		co.state.tail.directLeaves[0].SizeCumulativePayload() == 0 {
		// convergence requires a pb-unixfs-file leaf/link regardless of how the encoder is setup, go figure...
		return co.NodeEncoder.IpfsCompatibleNulLink(dgrencoder.NodeOrigin{OriginatorIndex: co.IndexInChain})
	}

	co.sealToLevel(0)
	return co.tail.directLeaves[0]
}

func (co *collector) AppendLeaf(ls dgrblock.LeafSource) (hdr *dgrblock.Header) {
	hdr = co.NodeEncoder.NewLeaf(ls)
	co.AppendBlock(hdr)
	return
}

func (co *collector) AppendBlock(hdr *dgrblock.Header) {
	// most calcs below rely on leaf-count-before-us
	// therefore we only increment when we return
	defer func() { co.leafCount++ }()

	// 2 SHORTCUTS
	if co.tail == nil {
		// 1) We are just starting: fill in a new tail node with a synthetic parent, and other inits
		//

		co.levelCutoffs = make([]int, 1, co.descentPrealloc)
		co.levelCutoffs[0] = co.MaxDirectLeaves

		co.tail = &trickleNode{
			depth:        0,
			directLeaves: make([]*dgrblock.Header, 1, co.MaxDirectLeaves+co.descentPrealloc),
			// this is a synthetic parent to hold the final-most-est digest CID
			parent: &trickleNode{
				depth:        -1,
				directLeaves: make([]*dgrblock.Header, 0, 1),
			},
		}
		co.tail.directLeaves[0] = hdr
		return
	} else if (co.leafCount % co.MaxDirectLeaves) != 0 {
		// 2) we are not yet at a node boundary
		//
		co.tail.directLeaves = append(co.tail.directLeaves, hdr)
		return
	}

	// if we got that far we are going to experience a node change
	// let's find out where the puck would go next
	var nextNodeDepth int

	if co.leafCount == co.levelCutoffs[len(co.levelCutoffs)-1] {
		// we have enough members to trigger the next descent-level-group: calculate and cache its size
		//
		co.levelCutoffs = append(
			co.levelCutoffs,
			co.MaxDirectLeaves*int(math.Pow(
				float64(co.MaxSiblingSubgroups+1),
				float64(len(co.levelCutoffs)),
			)),
		)

		nextNodeDepth = 1
	} else {
		// otherwise just find where we'd land
		//
		remainingLeaves := co.leafCount
		for level := len(co.levelCutoffs) - 1; level >= 0; level-- {
			if remainingLeaves >= co.levelCutoffs[level] {
				nextNodeDepth++
			}
			remainingLeaves %= co.levelCutoffs[level]
		}
	}

	newNode := &trickleNode{
		depth:        nextNodeDepth,
		directLeaves: make([]*dgrblock.Header, 1, co.MaxDirectLeaves+co.descentPrealloc),
	}
	newNode.directLeaves[0] = hdr

	// either backtrack "up the tree"
	// or just reiterate current step, pushing the sibling into the parent's "direct leaves"
	if co.tail.depth >= nextNodeDepth {
		co.sealToLevel(nextNodeDepth)
	}

	// now descend one step down for the final already-containing-a-leaf node
	newNode.parent = co.tail
	co.tail = newNode
}

func (co *collector) sealToLevel(toDepth int) {
	for co.tail.depth >= toDepth {
		co.tail.parent.directLeaves = append(co.tail.parent.directLeaves, co.NodeEncoder.NewLink(
			dgrencoder.NodeOrigin{
				OriginatorIndex: co.IndexInChain,
				LocalSubLayer:   -co.tail.depth, // negative local-layer signals "reverse-builder"
			},
			co.tail.directLeaves,
		))
		co.tail = co.tail.parent
	}
}

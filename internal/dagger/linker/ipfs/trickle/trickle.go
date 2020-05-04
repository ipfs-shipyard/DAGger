package trickle

import (
	"fmt"
	"math"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/enc/dagpb"
	linkerbase "github.com/ipfs-shipyard/DAGger/internal/dagger/linker"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

type config struct {
	MaxDirectLeaves       int  `getopt:"--max-direct-leaves       Maximum leaves per node (IPFS default: 174)"`          // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/helpers/helpers.go#L26
	MaxSiblingSubgroups   int  `getopt:"--max-sibling-subgroups   Maximum same-depth-groups per node (IPFS default: 4)"` // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/trickle/trickledag.go#L34
	LegacyDecoratedLeaves bool `getopt:"--unixfs-leaves           Generate leaves as full UnixFS file nodes"`
	LegacyCIDv0Links      bool `getopt:"--cidv0                   Generate compat-mode CIDv0 links"`
	NonstandardLeanLinks  bool `getopt:"--non-standard-lean-links Omit dag-size and offset information from all links. While IPFS likely will render the result, one voids all warranties"`
	newLeaf               func(block.DataSource) *block.Header
}
type linker struct {
	config
	*linkerbase.CommonConfig
	state
}
type state struct {
	leafCount    int
	levelCutoffs []int
	tail         *trickleNode
}
type trickleNode struct {
	depth        int
	directLeaves []*block.Header
	parent       *trickleNode
}

// at `7 = log( X/174 ) / log( 1+4 )` X comes out to 13,593,750 blocks to link before we need more allocs
const descentPrealloc = 7

func (l *linker) NewLeafBlock(ds block.DataSource) *block.Header { return l.newLeaf(ds) }

func (l *linker) NewLinkBlock(blocks []*block.Header) *block.Header {
	return dagpb.UnixFSv1LinkNode(
		blocks,
		l.BlockMaker,
		l.LegacyCIDv0Links,
		l.NonstandardLeanLinks,
	)
}

func (l *linker) AppendBlock(hdr *block.Header) {
	// most calcs below rely on leaf-count-before-us
	// therefore we only increment when we return
	defer func() { l.leafCount++ }()

	// 2 SHORTCUTS
	if l.tail == nil {
		// 1) We are just starting: fill in a new tail node with a synthetic parent, and other inits
		//

		l.levelCutoffs = make([]int, 1, descentPrealloc)
		l.levelCutoffs[0] = l.MaxDirectLeaves

		l.tail = &trickleNode{
			depth:        0,
			directLeaves: make([]*block.Header, 1, l.MaxDirectLeaves+descentPrealloc),
			// this is a synthetic parent to hold the final-most-est digest CID
			parent: &trickleNode{
				depth:        -1,
				directLeaves: make([]*block.Header, 0, 1),
			},
		}
		l.tail.directLeaves[0] = hdr
		return
	} else if (l.leafCount % l.MaxDirectLeaves) != 0 {
		// 2) we are not yet at a node boundary
		//
		l.tail.directLeaves = append(l.tail.directLeaves, hdr)
		return
	}

	// if we got that far we are going to experience a node change
	// let's find out where the puck would go next
	var nextNodeDepth int

	if l.leafCount == l.levelCutoffs[len(l.levelCutoffs)-1] {
		// we have enough members to trigger the next descent-level-group: calculate and cache its size
		//
		l.levelCutoffs = append(
			l.levelCutoffs,
			l.MaxDirectLeaves*int(math.Pow(
				float64(l.MaxSiblingSubgroups+1),
				float64(len(l.levelCutoffs)),
			)),
		)

		nextNodeDepth = 1
	} else {
		// otherwise just find where we'd land
		//
		remainingLeaves := l.leafCount
		for level := len(l.levelCutoffs) - 1; level >= 0; level-- {
			if remainingLeaves >= l.levelCutoffs[level] {
				nextNodeDepth++
			}
			remainingLeaves %= l.levelCutoffs[level]
		}
	}

	newNode := &trickleNode{
		depth:        nextNodeDepth,
		directLeaves: make([]*block.Header, 1, l.MaxDirectLeaves+descentPrealloc),
	}
	newNode.directLeaves[0] = hdr

	// either backtrack "up the tree"
	// or just reiterate current step, pushing the sibling into the parent's "direct leaves"
	if l.tail.depth >= nextNodeDepth {
		l.sealToLevel(nextNodeDepth)
	}

	// now descend one step down for the final already-containing-a-leaf node
	newNode.parent = l.tail
	l.tail = newNode
}

func (l *linker) sealToLevel(toDepth int) {

	for l.tail.depth >= toDepth {

		subHdr := l.NewLinkBlock(l.tail.directLeaves)

		if l.NewLinkBlockCallback != nil {
			l.NewLinkBlockCallback(subHdr, -l.tail.depth, l.tail.directLeaves)
		}

		l.tail.parent.directLeaves = append(
			l.tail.parent.directLeaves,
			subHdr,
		)
		l.tail = l.tail.parent
	}

}

func (l *linker) DeriveRoot() *block.Header {
	if l.tail == nil {
		return nil
	}

	// it is critical to reset the linker state when we are done - we reuse the object!
	defer func() { l.state = state{} }()

	// special case to match go-ipfs on zero-length streams
	if l.leafCount == 1 &&
		l.state.tail.directLeaves[0].SizeCumulativePayload() == 0 {
		// convergence requires a pb leaf regardless of what "LegacyDecoratedLeaves" is set to, go figure...
		zeroRoot := dagpb.UnixFSv1Leaf(
			block.DataSource{},
			l.BlockMaker,
			dagpb.UnixFsTypeRaw,
		)
		l.NewLinkBlockCallback(zeroRoot, 0, nil)
		return zeroRoot
	}

	l.sealToLevel(0)
	return l.tail.directLeaves[0]
}

func NewLinker(args []string, commonCfg *linkerbase.CommonConfig) (_ linkerbase.Linker, initErrs []string) {

	l := &linker{
		CommonConfig: commonCfg,
	}

	optSet := getopt.New()
	if err := options.RegisterSet("", &l.config, optSet); err != nil {
		// A panic as this should not be possible
		commonCfg.InternalPanicf(
			"option set registration failed: %s",
			err,
		)
	}

	if args == nil {
		return nil, util.SubHelp(
			"Produces a \"side-balanced\" DAG optimized for streaming. Data blocks further\n"+
				"away from the stream start are arranged in nodes at increasing depth away\n"+
				"from the root. The rough \"placement group\" for a particular node LeafIndex\n"+
				"away from the stream start can be derived numerically via:\n"+
				"int( log( LeafIndex / MaxDirectLeaves ) / log( 1 + MaxSiblingSubgroups ) )\n"+
				"See the example program in trickle.go for more info\n"+
				"First argument must be a version specifier 'vN'. Currently only supports\n"+
				"'v0', generating go-ipfs-standard, inefficient, 'Tsize'-full linknodes.",
			optSet,
		)
	}

	// bail early if version unset
	if len(args) < 2 || args[1] != "--v0" {
		return nil, []string{"first linker option must be a version - currently only 'v0' is supported"}
	}

	if l.NextLinker != nil {
		initErrs = append(
			initErrs,
			"v0 linker must appear last in chain",
		)
	}

	args = append(
		[]string{args[0]},
		// drop the v0
		args[2:]...,
	)

	// bail early if getopt fails
	if err := optSet.Getopt(args, nil); err != nil {
		return nil, []string{err.Error()}
	}

	args = optSet.Args()
	if len(args) != 0 {
		initErrs = append(initErrs, fmt.Sprintf(
			"unexpected parameter(s): %s...",
			args[0],
		))
	}

	if l.LegacyCIDv0Links &&
		(l.HasherName != "sha2-256" ||
			l.HasherBits != 256) {
		initErrs = append(
			initErrs,
			"legacy CIDv0 linking requires --hash=sha2-256 and --hash-bits=256",
		)
	}

	if l.MaxDirectLeaves < 1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value of 'max-direct-leaves' %d is out of range [1:...]",
			l.MaxDirectLeaves,
		))
	}

	if l.MaxSiblingSubgroups < 1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value of 'max-sibling-subgroups' %d is out of range [1:...]",
			l.MaxSiblingSubgroups,
		))
	}

	if l.LegacyDecoratedLeaves {
		l.newLeaf = func(ds block.DataSource) *block.Header {
			return dagpb.UnixFSv1Leaf(ds, l.BlockMaker, dagpb.UnixFsTypeRaw)
		}
	} else {
		l.newLeaf = func(ds block.DataSource) *block.Header {
			return block.RawDataLeaf(ds, l.BlockMaker)
		}
	}

	return l, initErrs
}

// Complete CLI program demonstrating node placement
//
/*

package main

import (
	"fmt"
	"math"
)

const (
	totalLeaves         = 200
	maxDirectLeaves     = 4 // ipfs default is 174
	maxSiblingSubgroups = 2 // ipfs default is 4
)

func main() {
	for existingLeafCount := 0; existingLeafCount <= totalLeaves; existingLeafCount++ {

		// displaying direct leaf population is not interesting
		if (existingLeafCount % maxDirectLeaves) != 0 {
			continue
		}

		// all calculations below rely on the pre-existing leaf count ( 0-based index if you will )
		remainingLeaves := existingLeafCount
		fmt.Printf("%5s", fmt.Sprintf("#%d", remainingLeaves))

		for remainingLeaves >= maxDirectLeaves {

			descentLevel := int(
				(math.Log(float64((remainingLeaves) / maxDirectLeaves))) /
					math.Log(float64((1 + maxSiblingSubgroups))),
			)

			descentLevelMembers := maxDirectLeaves * int(math.Pow(
				float64(maxSiblingSubgroups+1),
				float64(descentLevel),
			))

			if remainingLeaves >= descentLevelMembers {
				subGroupNodeIndexAtLevel := (remainingLeaves / descentLevelMembers) + (descentLevel * maxSiblingSubgroups)
				fmt.Printf("\t%d", subGroupNodeIndexAtLevel)
			}

			remainingLeaves %= descentLevelMembers
		}
		fmt.Print("\n")
	}
}

*/

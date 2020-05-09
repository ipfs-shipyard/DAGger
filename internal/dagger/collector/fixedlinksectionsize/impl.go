package fixedlinksectionsize

import (
	"log"

	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

type config struct {
	MaxLinksectionSize int `getopt:"--max-linksection-size   Maximum size of a node's linksection"`
}
type state struct {
	stack []layer
}
type layer struct {
	linkframesSize int
	nodes          []*dgrblock.Header
}
type collector struct {
	config
	*dgrcollector.DaggerConfig
	state
}

func (co *collector) FlushState() *dgrblock.Header {
	if len(co.stack[len(co.stack)-1].nodes) == 0 {
		return nil
	}

	// it is critical to reset the collector state when we are done - we reuse the object!
	defer func() { co.state = state{stack: []layer{{}}} }()

	co.compactLayers(true) // merge everything
	return co.stack[len(co.stack)-1].nodes[0]
}

func (co *collector) AppendLeaf(ls dgrblock.LeafSource) (hdr *dgrblock.Header) {
	hdr = co.NodeEncoder.NewLeaf(ls)
	co.AppendBlock(hdr)
	return
}

func (co *collector) AppendBlock(hdr *dgrblock.Header) {
	sz := co.NodeEncoder.LinkframeSize(hdr)
	if sz > co.MaxLinksectionSize {
		log.Panicf(
			"linkframe for block %s is %s bytes long, exceeding the max-linksection-size of %s",
			hdr.String(),
			util.Commify(sz),
			util.Commify(co.MaxLinksectionSize),
		)
	}

	co.stack[0].linkframesSize += sz
	co.stack[0].nodes = append(co.stack[0].nodes, hdr)

	// if co.stack[0].linkframesSize >= co.MaxLinksectionSize {
	// 	co.compactLayers(false) // do not proceed beyond already-full nodes
	// }
}

func (co *collector) compactLayers(fullMergeRequested bool) {

	for stackLayerIdx := range co.stack {
		curStack := &co.stack[stackLayerIdx] // shortcut

		if len(curStack.nodes) == 1 && len(co.stack)-1 == stackLayerIdx ||
			!fullMergeRequested && curStack.linkframesSize < co.MaxLinksectionSize {
			break
		}

		// we got work to do - instantiate next stack if needed
		if len(co.stack)-1 == stackLayerIdx {
			co.stack = append(co.stack, layer{})
		}

		var curIdx, lastIdx, curSize, runningSize int
		for curIdx < len(co.stack) {
			curSize = co.NodeEncoder.LinkframeSize(curStack.nodes[curIdx])
			if runningSize+curSize >= co.MaxLinksectionSize {

				// fmt.Println("comp")
				linkHdr := co.NodeEncoder.NewLink(
					dgrencoder.NodeOrigin{
						OriginatorIndex: co.IndexInChain,
						LocalSubLayer:   stackLayerIdx,
					},
					curStack.nodes[lastIdx:curIdx],
				)

				co.stack[stackLayerIdx+1].nodes = append(co.stack[stackLayerIdx+1].nodes, linkHdr)
				co.stack[stackLayerIdx+1].linkframesSize += co.NodeEncoder.LinkframeSize(linkHdr)

				runningSize = 0
				lastIdx = curIdx
			}

			runningSize += curSize
			curIdx++
		}

		if fullMergeRequested && lastIdx != len(co.stack) {

			linkHdr := co.NodeEncoder.NewLink(
				dgrencoder.NodeOrigin{
					OriginatorIndex: co.IndexInChain,
					LocalSubLayer:   stackLayerIdx,
				},
				curStack.nodes[lastIdx:],
			)

			co.stack[stackLayerIdx+1].nodes = append(co.stack[stackLayerIdx+1].nodes, linkHdr)
			co.stack[stackLayerIdx+1].linkframesSize += co.NodeEncoder.LinkframeSize(linkHdr)

			lastIdx = len(co.stack)
		}

		co.stack[stackLayerIdx].nodes = co.stack[stackLayerIdx].nodes[lastIdx:]
	}
}

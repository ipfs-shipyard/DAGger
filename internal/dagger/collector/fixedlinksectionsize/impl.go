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

	frameSize := co.NodeEncoder.LinkframeSize(hdr)

	// FIXME A clash of max-linksection with max-inline-cid sizing
	if frameSize > co.MaxLinksectionSize {
		log.Panicf(
			"linkframe for block %s is %s bytes long, exceeding the max-linksection-size of %s",
			hdr.String(),
			util.Commify(frameSize),
			util.Commify(co.MaxLinksectionSize),
		)
	}

	co.stack[0].linkframesSize += frameSize
	co.stack[0].nodes = append(co.stack[0].nodes, hdr)

	// Compact every time we reach enough nodes on the entry layer
	// Helps relieve memory pressure/consumption on very large DAGs
	// if co.stack[0].linkframesSize >= co.MaxLinksectionSize {
	// 	co.compactLayers(false) // do not proceed beyond already-full nodes
	// }
}

func (co *collector) compactLayers(fullMergeRequested bool) {

	for stackLayerIdx := 0; stackLayerIdx < len(co.stack); stackLayerIdx++ {
		curStack := &co.stack[stackLayerIdx] // shortcut

		if len(curStack.nodes) == 1 && len(co.stack)-1 == stackLayerIdx ||
			!fullMergeRequested && curStack.linkframesSize < co.MaxLinksectionSize {
			break
		}

		// we got work to do - instantiate next stack if needed
		if len(co.stack)-1 == stackLayerIdx {
			co.stack = append(co.stack, layer{})
		}

		var lastCutIdx int
		for curStack.linkframesSize > co.MaxLinksectionSize ||
			fullMergeRequested && lastCutIdx < len(curStack.nodes) {

			var runninglinksectionSize, overflowingNodeFramesize int
			curIdx := lastCutIdx
			for curIdx < len(curStack.nodes) {
				overflowingNodeFramesize = co.NodeEncoder.LinkframeSize(curStack.nodes[curIdx])
				if runninglinksectionSize+overflowingNodeFramesize > co.MaxLinksectionSize {
					break
				}
				runninglinksectionSize += overflowingNodeFramesize
				curIdx++
			}

			linkHdr := co.NodeEncoder.NewLink(
				dgrencoder.NodeOrigin{
					OriginatingLayer: co.ChainPosition,
					LocalSubLayer:    stackLayerIdx,
				},
				curStack.nodes[lastCutIdx:curIdx],
			)
			co.stack[stackLayerIdx+1].nodes = append(co.stack[stackLayerIdx+1].nodes, linkHdr)
			co.stack[stackLayerIdx+1].linkframesSize += co.NodeEncoder.LinkframeSize(linkHdr)

			curStack.linkframesSize -= runninglinksectionSize
			runninglinksectionSize = overflowingNodeFramesize
			lastCutIdx = curIdx
		}

		// shift everything to the last cut, without realloc
		curStack.nodes = curStack.nodes[:copy(
			curStack.nodes,
			curStack.nodes[lastCutIdx:],
		)]
	}
}

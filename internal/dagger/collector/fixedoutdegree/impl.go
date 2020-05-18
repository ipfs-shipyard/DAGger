package fixedoutdegree

import (
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
)

type config struct {
	MaxOutdegree int `getopt:"--max-outdegree   Maximum outdegree (children) for a node (IPFS default: 174)"` // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/helpers/helpers.go#L26
}
type state struct {
	stack [][]*dgrblock.Header
}
type collector struct {
	config
	*dgrcollector.DaggerConfig
	state
}

func (co *collector) FlushState() *dgrblock.Header {
	if len(co.stack[len(co.stack)-1]) == 0 {
		return nil
	}

	// it is critical to reset the collector state when we are done - we reuse the object!
	defer func() { co.stack = [][]*dgrblock.Header{{}} }()

	co.compactLayers(true) // merge everything
	return co.stack[len(co.stack)-1][0]
}

func (co *collector) AppendData(ds dgrblock.DataSource) (hdr *dgrblock.Header) {
	hdr = co.NodeEncoder.NewLeaf(ds)
	co.AppendBlock(hdr)
	return
}

func (co *collector) AppendBlock(hdr *dgrblock.Header) {
	co.stack[0] = append(co.stack[0], hdr)

	// Compact every time we reach enough nodes on the entry layer
	// Helps relieve memory pressure/consumption on very large DAGs
	if len(co.stack[0]) >= co.MaxOutdegree {
		co.compactLayers(false) // do not proceed beyond already-full nodes
	}
}

func (co *collector) compactLayers(fullMergeRequested bool) {

	for stackLayerIdx := 0; stackLayerIdx < len(co.stack); stackLayerIdx++ {
		curLayer := &co.stack[stackLayerIdx] // shortcut

		if len(*curLayer) == 1 && len(co.stack)-1 == stackLayerIdx ||
			!fullMergeRequested && len(*curLayer) < co.MaxOutdegree {
			break
		}

		// we got work to do - instantiate next stack if needed
		if len(co.stack)-1 == stackLayerIdx {
			co.stack = append(co.stack, []*dgrblock.Header{})
		}

		var lastCutIdx, nextCutIdx int
		for len(*curLayer)-lastCutIdx >= co.MaxOutdegree ||
			fullMergeRequested && lastCutIdx < len(*curLayer) {

			nextCutIdx = lastCutIdx + co.MaxOutdegree
			if nextCutIdx > len(*curLayer) {
				nextCutIdx = len(*curLayer)
			}

			co.stack[stackLayerIdx+1] = append(co.stack[stackLayerIdx+1], co.NodeEncoder.NewLink(
				dgrencoder.NodeOrigin{
					OriginatingLayer: co.ChainPosition,
					LocalSubLayer:    stackLayerIdx,
				},
				(*curLayer)[lastCutIdx:nextCutIdx],
			))

			lastCutIdx = nextCutIdx
		}

		// shift everything to the last cut, without realloc
		co.stack[stackLayerIdx] = (*curLayer)[:copy(
			*curLayer,
			(*curLayer)[lastCutIdx:],
		)]
	}
}

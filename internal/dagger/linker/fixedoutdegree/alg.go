package fixedoutdegree

import (
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/enc/dagpb"
)

type config struct {
	MaxOutdegree          int  `getopt:"--max-outdegree           Maximum outdegree (children) for a node (IPFS default: 174)"` // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/helpers/helpers.go#L26
	LegacyDecoratedLeaves bool `getopt:"--unixfs-leaves           Generate leaves as full UnixFS file nodes"`
	LegacyCIDv0Links      bool `getopt:"--cidv0                   Generate compat-mode CIDv0 links"`
	NonstandardLeanLinks  bool `getopt:"--non-standard-lean-links Omit dag-size and offset information from all links. While IPFS likely will render the result, one voids all warranties"`
	newLeaf               func(block.LeafSource) *block.Header
}
type state struct {
	stack [][]*block.Header
}

func (l *linker) NewLeafBlock(ls block.LeafSource) *block.Header { return l.newLeaf(ls) }

func (l *linker) NewLinkBlock(blocks []*block.Header) *block.Header {
	return dagpb.UnixFSv1LinkNode(
		blocks,
		l.BlockMaker,
		l.LegacyCIDv0Links,
		l.NonstandardLeanLinks,
	)
}

func (l *linker) AppendBlock(hdr *block.Header) {
	l.stack[0] = append(l.stack[0], hdr)

	// Compact every time we reach enough loose nodes on the leaf-layer
	// Helps relieve memory pressure/consumption on very large DAGs
	if len(l.stack[0]) >= l.MaxOutdegree {
		l.compactLayers(false) // do not proceed beyond already-full nodes
	}
}

func (l *linker) compactLayers(fullMergeRequested bool) {

	for stackLayerIdx := range l.stack {
		curStack := &l.stack[stackLayerIdx] // shortcut

		if len(*curStack) == 1 && len(l.stack) == stackLayerIdx+1 ||
			!fullMergeRequested && len(*curStack) < l.MaxOutdegree {
			break
		}

		// we got work to do - instantiate next stack if needed
		if len(l.stack) == stackLayerIdx+1 {
			l.stack = append(l.stack, []*block.Header{})
		}

		var curIdx int
		for len(*curStack)-curIdx >= l.MaxOutdegree ||
			fullMergeRequested && curIdx < len(*curStack) {

			cutoffIdx := curIdx + l.MaxOutdegree
			if cutoffIdx > len(*curStack) {
				cutoffIdx = len(*curStack)
			}

			hdr := l.NewLinkBlock((*curStack)[curIdx:cutoffIdx])

			if l.NewLinkBlockCallback != nil {
				l.NewLinkBlockCallback(hdr, stackLayerIdx, (*curStack)[curIdx:cutoffIdx])
			}

			l.stack[stackLayerIdx+1] = append(l.stack[stackLayerIdx+1], hdr)
			curIdx = cutoffIdx
		}
		l.stack[stackLayerIdx] = l.stack[stackLayerIdx][curIdx:]
	}
}

func (l *linker) DeriveRoot() *block.Header {
	if len(l.stack[len(l.stack)-1]) == 0 {
		return nil
	}

	// it is critical to reset the linker state when we are done - we reuse the object!
	defer func() { l.stack = [][]*block.Header{{}} }()

	l.compactLayers(true) // merge everything
	return l.stack[len(l.stack)-1][0]
}

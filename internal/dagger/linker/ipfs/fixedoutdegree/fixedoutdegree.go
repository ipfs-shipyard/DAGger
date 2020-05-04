package fixedoutdegree

import (
	"fmt"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/enc/dagpb"
	linkerbase "github.com/ipfs-shipyard/DAGger/internal/dagger/linker"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

type config struct {
	MaxOutdegree          int  `getopt:"--max-outdegree           Maximum outdegree (children) for a node (IPFS default: 174)"` // https://github.com/ipfs/go-unixfs/blob/v0.2.4/importer/helpers/helpers.go#L26
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
	stack [][]*block.Header
}

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

func NewLinker(args []string, commonCfg *linkerbase.CommonConfig) (_ linkerbase.Linker, initErrs []string) {

	l := &linker{
		CommonConfig: commonCfg,
		state:        state{stack: [][]*block.Header{{}}},
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
			"Produces a DAG with every node having a fixed outdegree (amount of children)\n"+
				"First argument must be a version specifier 'vN'. Currently only supports\n"+
				"'v0' generating go-ipfs-standard, inefficient, 'Tsize'-full linknodes.",
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

	if l.MaxOutdegree < 2 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value of 'max-outdegree' %d is out of range [2:...]",
			l.MaxOutdegree,
		))
	}

	if l.LegacyDecoratedLeaves {
		l.newLeaf = func(ds block.DataSource) *block.Header {
			return dagpb.UnixFSv1Leaf(ds, l.BlockMaker, dagpb.UnixFsTypeFile)
		}
	} else {
		l.newLeaf = func(ds block.DataSource) *block.Header {
			return block.RawDataLeaf(ds, l.BlockMaker)
		}
	}

	return l, initErrs
}

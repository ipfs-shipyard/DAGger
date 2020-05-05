package linker

import (
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

type Linker interface {
	NewLeafBlock(leafData block.DataSource) (leafBlock *block.Header)
	NewLinkBlock(blocksToLink []*block.Header) (linkBlock *block.Header)
	AppendBlock(blockToAppendToStream *block.Header)
	DeriveRoot() (rootBlockAfterReducingAndDestroyingLinkerState *block.Header)
}

type DaggerConfig struct {
	IndexInChain         int
	LastChainIndex       int
	GlobalMaxBlockSize   int
	HasherBits           int
	HasherName           string
	BlockMaker           block.Maker
	NewLinkBlockCallback NewLinkBlockCallback
	NextLinker           Linker
	InternalPanicf       func(format string, args ...interface{})
}

type NewLinkBlockCallback func(
	hdr *block.Header,
	linkerLayer int,
	links []*block.Header,
)

type Initializer func(
	linkerCLISubArgs []string,
	cfg *DaggerConfig,
) (instance Linker, initErrorStrings []string)

//
// Implement the "none" linker here directly, without a separate package
//
type nulLinker struct{ *DaggerConfig }

func (l *nulLinker) NewLeafBlock(ds block.DataSource) *block.Header {
	return block.RawDataLeaf(ds, l.BlockMaker)
}
func (l *nulLinker) NewLinkBlock([]*block.Header) *block.Header { return nil }
func (l *nulLinker) AppendBlock(*block.Header)                  { return }
func (l *nulLinker) DeriveRoot() *block.Header                  { return nil }
func NewNulLinker(args []string, dgrCfg *DaggerConfig) (_ Linker, initErrs []string) {
	if args == nil {
		return nil, util.SubHelp(
			"Does not form a DAG, nor emits a root CID. Simply gathers all chunked data\n"+
				"and passes to the stats tracker. Takes no arguments.",
			nil,
		)
	}

	if len(args) > 1 {
		initErrs = append(initErrs, "linker takes no arguments")
	}
	if dgrCfg.NextLinker != nil {
		initErrs = append(initErrs, "linker must appear last in chain")
	}

	return &nulLinker{dgrCfg}, initErrs
}

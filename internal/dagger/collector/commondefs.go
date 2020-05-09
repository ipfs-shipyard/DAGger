package dgrcollector

import (
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
)

type Collector interface {
	AppendLeaf(dgrblock.LeafSource) (resultingLeafBlock *dgrblock.Header)
	AppendBlock(blockToAppendToStream *dgrblock.Header)
	FlushState() (rootBlockAfterReducingAndDestroyingObjectState *dgrblock.Header)
}

type DaggerConfig struct {
	IndexInChain  int
	NodeEncoder   dgrencoder.NodeEncoder
	NextCollector Collector
}

type Initializer func(
	collectorCLISubArgs []string,
	cfg *DaggerConfig,
) (instance Collector, initErrorStrings []string)

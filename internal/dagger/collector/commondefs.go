package dgrcollector

import (
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
)

type Collector interface {
	AppendData(formLeafBlockAndAppend dgrblock.DataSource) (resultingLeafBlock *dgrblock.Header)
	AppendBlock(blockToAppendToStream *dgrblock.Header)
	FlushState() (rootBlockAfterReducingAndDestroyingObjectState *dgrblock.Header)
}

type DaggerConfig struct {
	ChunkerChainMaxResult int // used for initialization sanity checks
	ChainPosition         int // used for DAG-stats layering
	NodeEncoder           dgrencoder.NodeEncoder
	NextCollector         Collector
}

type Initializer func(
	collectorCLISubArgs []string,
	cfg *DaggerConfig,
) (instance Collector, initErrorStrings []string)

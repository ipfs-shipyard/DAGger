package dgrchunker

import "github.com/ipfs-shipyard/DAGger/chunker"

type InstanceConstants struct {
	MinChunkSize int
	MaxChunkSize int
}

type DaggerConfig struct {
	IsLastInChain bool
}

type Initializer func(
	chunkerCLISubArgs []string,
	cfg *DaggerConfig,
) (
	instance chunker.Chunker,
	constants InstanceConstants,
	initErrorStrings []string,
)

package dgrchunker

import "github.com/ipfs-shipyard/DAGger/chunker"

type InstanceConstants struct {
	MinChunkSize int
}

type DaggerConfig struct {
	IndexInChain       int
	LastChainIndex     int
	GlobalMaxChunkSize int
}

type Initializer func(
	chunkerCLISubArgs []string,
	cfg *DaggerConfig,
) (
	instance chunker.Chunker,
	constants InstanceConstants,
	initErrorStrings []string,
)

package dagger

import (
	"sync"

	"github.com/ipfs-shipyard/DAGger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	dgrlinker "github.com/ipfs-shipyard/DAGger/internal/dagger/linker"
	"github.com/ipfs/go-qringbuf"
)

type dgrChunkerUnit struct {
	instance  chunker.Chunker
	constants dgrchunker.InstanceConstants
}

type Dagger struct {
	curStreamOffset     int64
	cfg                 config
	statSummary         statSummary
	chainedChunkers     []dgrChunkerUnit
	chainedLinkers      []dgrlinker.Linker
	qrb                 *qringbuf.QuantizedRingBuffer
	uniqueBlockCallback func(hdr *block.Header) *blockPostProcessResult
	asyncWG             sync.WaitGroup
	asyncHasherBus      block.AsyncHashBus
	mu                  sync.Mutex
	seenBlocks          seenBlocks
	seenRoots           seenRoots
}

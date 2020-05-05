package dagger

import (
	"sync"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/linker"
	"github.com/ipfs/go-qringbuf"
)

type Dagger struct {
	curStreamOffset     int64
	cfg                 config
	statSummary         statSummary
	chainedChunkers     []chunker.Chunker
	chainedLinkers      []linker.Linker
	qrb                 *qringbuf.QuantizedRingBuffer
	uniqueBlockCallback func(hdr *block.Header) *blockPostProcessResult
	asyncWG             sync.WaitGroup
	asyncHasherBus      block.AsyncHashBus
	mu                  sync.Mutex
	seenBlocks          seenBlocks
	seenRoots           seenRoots
}

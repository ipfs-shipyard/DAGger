package dagger

import (
	"sync"

	"github.com/ipfs/go-qringbuf"

	"github.com/ipfs-shipyard/DAGger/chunker"
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
)

type dgrChunkerUnit struct {
	instance  chunker.Chunker
	constants dgrchunker.InstanceConstants
}

type Dagger struct {
	// speederization shortcut flags for internal logic
	generateRoots bool
	emitChunks    bool

	latestLeafInlined   bool
	curStreamOffset     int64
	cfg                 config
	statSummary         statSummary
	chainedChunkers     []dgrChunkerUnit
	chainedCollectors   []dgrcollector.Collector
	externalEventBus    chan<- IngestionEvent
	qrb                 *qringbuf.QuantizedRingBuffer
	uniqueBlockCallback func(hdr *dgrblock.Header) *blockPostProcessResult
	asyncWG             sync.WaitGroup
	asyncHashingBus     dgrblock.AsyncHashingBus
	mu                  sync.Mutex
	seenBlocks          seenBlocks
	seenRoots           seenRoots
}

package dagger

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/ipfs-shipyard/DAGger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/constants"
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	"github.com/ipfs-shipyard/DAGger/internal/zcpstring"
	"github.com/ipfs/go-qringbuf"
)

// SANCHECK: not sure if any of these make sense, nor have I measured the cost
const (
	chunkQueueSizeTop      = 256
	chunkQueueSizeSubchunk = 32
)

const (
	ErrorString = IngestionEventType(iota)
	NewChunkJsonl
	NewRootJsonl
)

type IngestionEvent struct {
	Type IngestionEventType
	Body string
}
type IngestionEventType int

// SANCHECK - we probably want some sort of timeout or somesuch here...
func (dgr *Dagger) maybeSendEvent(t IngestionEventType, s string) {
	if dgr.externalEventBus != nil {
		dgr.externalEventBus <- IngestionEvent{Type: t, Body: s}
	}
}

var preProcessTasks, postProcessTasks func(dgr *Dagger)

func (dgr *Dagger) ProcessReader(inputReader io.Reader, optionalEventChan chan<- IngestionEvent) (err error) {

	var t0 time.Time

	defer func() {
		// we need to finish all emissions to complete before we measure/return
		dgr.asyncWG.Wait()
		if postProcessTasks != nil {
			postProcessTasks(dgr)
		}
		dgr.qrb = nil
		if dgr.externalEventBus != nil {
			close(dgr.externalEventBus)
		}
		dgr.statSummary.SysStats.ElapsedNsecs = time.Since(t0).Nanoseconds()
	}()

	dgr.externalEventBus = optionalEventChan
	defer func() {
		if err != nil {

			var buffered int
			if dgr.qrb != nil {
				dgr.qrb.Lock()
				buffered = dgr.qrb.Buffered()
				dgr.qrb.Unlock()
			}

			err = fmt.Errorf(
				"failure at byte offset %s of sub-stream #%d with %s bytes buffered/unprocessed: %s",
				util.Commify64(dgr.curStreamOffset),
				dgr.statSummary.Streams,
				util.Commify(buffered),
				err,
			)

			dgr.maybeSendEvent(ErrorString, err.Error())
		}
	}()

	if preProcessTasks != nil {
		preProcessTasks(dgr)
	}
	t0 = time.Now()

	var initErr error
	dgr.qrb, initErr = qringbuf.NewFromReader(inputReader, qringbuf.Config{
		// MinRegion must be twice the maxchunk, otherwise chunking chains won't work (hi, Claude Shannon)
		MinRegion:   2 * dgr.cfg.GlobalMaxChunkSize,
		MinRead:     dgr.cfg.RingBufferMinRead,
		MaxCopy:     2 * dgr.cfg.GlobalMaxChunkSize, // SANCHECK having it equal to the MinRegion may be daft...
		BufferSize:  dgr.cfg.RingBufferSize,
		SectorSize:  dgr.cfg.RingBufferSectSize,
		Stats:       &dgr.statSummary.SysStats.Stats,
		TrackTiming: ((dgr.cfg.StatsActive & statsRingbuf) == statsRingbuf),
	})
	if initErr != nil {
		return initErr
	}

	if (dgr.cfg.StatsActive & statsBlocks) == statsBlocks {
		dgr.seenBlocks = make(seenBlocks, 1024) // SANCHECK: somewhat arbitrary, but eh...
		dgr.seenRoots = make(seenRoots, 32)
	}

	// use 64bits everywhere
	var substreamSize int64

	// outer stream loop: read() syscalls happen only here and in the qrb.collector()
	for {
		if dgr.cfg.MultipartStream {

			err := binary.Read(
				inputReader,
				binary.BigEndian,
				&substreamSize,
			)
			dgr.statSummary.SysStats.ReadCalls++

			if err == io.EOF {
				// no new multipart coming - bail
				break
			} else if err != nil {
				return fmt.Errorf(
					"error reading next 8-byte multipart substream size: %s",
					err,
				)
			}

			if substreamSize == 0 && !dgr.cfg.ProcessNulInputs {
				continue
			}

			dgr.statSummary.Streams++
			dgr.curStreamOffset = 0
			dgr.latestLeafInlined = false
		}

		if dgr.cfg.MultipartStream && substreamSize == 0 {
			// If we got here: cfg.ProcessNulInputs is true
			// Special case for a one-time zero-CID emission
			dgr.appendLeaf(nil)
		} else if err := dgr.processStream(substreamSize); err != nil {
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf(
					"unexpected end of substream #%s after %s bytes (stream expected to be %s bytes long)",
					util.Commify64(dgr.statSummary.Streams),
					util.Commify64(dgr.curStreamOffset+int64(dgr.qrb.Buffered())),
					util.Commify64(substreamSize),
				)
			} else if err != io.EOF {
				return err
			} else if dgr.curStreamOffset == 0 && dgr.cfg.ProcessNulInputs {
				// we did try to process a stream and ended up with an EOF + 0
				// emit a zero-CID like above
				dgr.appendLeaf(nil)
			}
		}

		if dgr.generateRoots || dgr.seenRoots != nil || dgr.externalEventBus != nil {

			// cascading flush across the chain
			var rootBlock *dgrblock.Header
			for _, c := range dgr.chainedCollectors {
				rootBlock = c.FlushState()
			}

			var rootPayloadSize, rootDagSize uint64
			if rootBlock != nil {
				rootPayloadSize = rootBlock.SizeCumulativePayload()
				rootDagSize = rootBlock.SizeCumulativeDag()

				if dgr.seenRoots != nil {
					dgr.mu.Lock()

					var rootSeen bool
					if sk := seenKey(rootBlock); sk != nil {
						if _, rootSeen = dgr.seenRoots[*sk]; !rootSeen {
							dgr.seenRoots[*sk] = rootBlock.Cid()
						}
					}

					dgr.statSummary.Roots = append(dgr.statSummary.Roots, rootStats{
						Cid:         rootBlock.CidBase32(),
						SizePayload: rootBlock.SizeCumulativePayload(),
						SizeDag:     rootBlock.SizeCumulativeDag(),
						Dup:         rootSeen,
					})

					dgr.mu.Unlock()
				}
			}

			jsonl := fmt.Sprintf(
				"{\"event\":   \"root\", \"payload\":%12d, \"stream\":%7d, %-67s, \"wiresize\":%12d }\n",
				rootPayloadSize,
				dgr.statSummary.Streams,
				fmt.Sprintf(`"cid":"%s"`, rootBlock.CidBase32()),
				rootDagSize,
			)
			dgr.maybeSendEvent(NewRootJsonl, jsonl)
			if rootBlock != nil && dgr.cfg.emitters[emRootsJsonl] != nil {
				if _, err := io.WriteString(dgr.cfg.emitters[emRootsJsonl], jsonl); err != nil {
					return fmt.Errorf("emitting '%s' failed: %s", emRootsJsonl, err)
				}
			}
		}

		// we are in EOF-state: if we are not expecting multiparts - we are done
		if !dgr.cfg.MultipartStream {
			break
		}
	}

	return
}

// This is essentially a union:
// - either subSplits will be provided for recursion
// - or a chunk with its region will be sent
type recursiveSplitResult struct {
	subSplits      <-chan *recursiveSplitResult
	chunkBufRegion *qringbuf.Region
	chunk          chunker.Chunk
}

type chunkingInconsistencyHandler func(
	chunkerIdx int,
	workRegionStreamOffset int64,
	workRegionSize,
	workRegionPos int,
	errStr string,
)

func (dgr *Dagger) processStream(streamLimit int64) error {

	// begin reading and filling buffer
	if err := dgr.qrb.StartFill(streamLimit); err != nil {
		return err
	}

	var streamEndInView bool
	var availableFromReader, processedFromReader int
	var streamOffset int64

	chunkingErr := make(chan error)

	// this callback is passed through the recursive chain instead of a bare channel
	// providing reasonable diag context
	errHandler := func(chunkerIdx int, wrOffset int64, wrSize, wrPos int, errStr string) {
		chunkingErr <- fmt.Errorf(`

chunking error
--------------
StreamEndInView:     %17t
MainRegionSize:      %17s
SubRegionSize:       %17s
ErrorAtSubRegionPos: %17s
SubRegionRemaining:  %17s
StreamOffset:        %17s
SubBufOffset:        %17s
ErrorOffset:         %17s
chunker: #%d %T
--------------
chunking error: %s%s`,
			streamEndInView,
			util.Commify(availableFromReader),
			util.Commify(wrSize),
			util.Commify(wrPos),
			util.Commify(wrSize-wrPos),
			util.Commify64(streamOffset),
			util.Commify64(wrOffset),
			util.Commify64(wrOffset+int64(wrPos)),
			chunkerIdx,
			dgr.chainedChunkers[chunkerIdx],
			errStr,
			"\n\n",
		)
	}

	for {

		// next 2 lines evaluate processedInRound and availableForRound from *LAST* iteration
		streamOffset += int64(processedFromReader)
		workRegion, readErr := dgr.qrb.NextRegion(availableFromReader - processedFromReader)

		if workRegion == nil || (readErr != nil && readErr != io.EOF) {
			return readErr
		}

		availableFromReader = workRegion.Size()
		processedFromReader = 0
		streamEndInView = (readErr == io.EOF)

		rescursiveSplitResults := make(chan *recursiveSplitResult, chunkQueueSizeTop)
		go dgr.recursivelySplitBuffer(
			// The entire reserved buffer to split recursively
			// Guaranteed to be left intact until we call NextSlice
			workRegion,
			// Where we are (for error reporting)
			streamOffset,
			// We need to tell the top chunker when nothing else is coming, to prevent the entire chain repeating work otherwise
			streamEndInView,
			// Index of top chunker
			0,
			// the channel for the chunking results
			rescursiveSplitResults,
			// func() instead of a channel, closes over the common error channel and several stream position vars
			errHandler,
		)

	receiveChunks:
		for {
			select {
			case err := <-chunkingErr:
				return err
			case res, chanOpen := <-rescursiveSplitResults:
				if !chanOpen {
					break receiveChunks
				}
				processedFromReader += dgr.gatherRecursiveResults(res)
			}
		}

		dgr.statSummary.Dag.Payload += int64(processedFromReader)
	}
}

func (dgr *Dagger) gatherRecursiveResults(result *recursiveSplitResult) int {
	if result.subSplits != nil {
		var substreamSize int
		for {
			subRes, channelOpen := <-result.subSplits
			if !channelOpen {
				return substreamSize
			}
			substreamSize += dgr.gatherRecursiveResults(subRes)
		}
	}

	result.chunkBufRegion.Reserve()
	dgr.appendLeaf(result)
	return result.chunk.Size
}

func (dgr *Dagger) recursivelySplitBuffer(
	workRegion *qringbuf.Region,
	workRegionStreamOffset int64,
	useEntireRegion bool,
	chunkerIdx int,
	recursiveResultsReturn chan<- *recursiveSplitResult,
	errHandler chunkingInconsistencyHandler,
) {
	var processedBytes int

	// SANCHECK
	// It is not clear whether it is better to give this as a callback to
	// a chunker directly, INSTEAD of the separate goroutine/channel we have below
	var chunkProcessor chunker.SplitResultCallback
	chunkProcessor = func(c chunker.Chunk) error {
		if c.Size <= 0 ||
			c.Size > workRegion.Size()-processedBytes {
			err := fmt.Errorf("returned chunk size %s out of bounds", util.Commify(c.Size))
			errHandler(chunkerIdx, workRegionStreamOffset, workRegion.Size(), processedBytes,
				err.Error(),
			)
			return err
		}

		if len(dgr.chainedChunkers) > chunkerIdx+1 &&
			!(c.Meta != nil && c.Meta["nosubchunking"] != nil && c.Meta["nosubchunking"].(bool)) {
			// we are not last in the chain - subchunk
			subSplits := make(chan *recursiveSplitResult, chunkQueueSizeSubchunk)
			go dgr.recursivelySplitBuffer(
				workRegion.SubRegion(
					processedBytes,
					c.Size,
				),
				workRegionStreamOffset+int64(processedBytes),
				true, // subchunkers always "use entire region" by definition
				chunkerIdx+1,
				subSplits,
				errHandler,
			)
			recursiveResultsReturn <- &recursiveSplitResult{subSplits: subSplits}
		} else {
			recursiveResultsReturn <- &recursiveSplitResult{
				chunk: c,
				chunkBufRegion: workRegion.SubRegion(
					processedBytes,
					c.Size,
				),
			}
		}

		processedBytes += c.Size
		return nil
	}

	chanSize := chunkQueueSizeSubchunk
	if chunkerIdx == 0 {
		chanSize = chunkQueueSizeTop
	}
	chunkRetChan := make(chan chunker.Chunk, chanSize)

	go func() {
		dgr.chainedChunkers[chunkerIdx].instance.Split(
			workRegion.Bytes(),
			useEntireRegion,
			func(c chunker.Chunk) error { chunkRetChan <- c; return nil },
		)
		close(chunkRetChan)
	}()

	for {
		c, chanOpen := <-chunkRetChan
		if !chanOpen {
			break
		}
		chunkProcessor(c)
	}

	if processedBytes == 0 &&
		len(dgr.chainedChunkers) > chunkerIdx+1 {
		// We didn't manage to find *anything*, and there is a subsequent chunker
		// Pass it the entire frame in a "tail-call" fashion, have *them* close
		// the result channel when done
		dgr.recursivelySplitBuffer(
			workRegion,
			workRegionStreamOffset,
			useEntireRegion,
			chunkerIdx+1,
			recursiveResultsReturn,
			errHandler,
		)
		return
	}

	if processedBytes <= 0 || processedBytes > workRegion.Size() {
		errHandler(
			chunkerIdx,
			workRegionStreamOffset,
			workRegion.Size(),
			processedBytes,
			fmt.Sprintf(
				"returned %s bytes as chunks for a buffer of %s bytes",
				util.Commify(processedBytes),
				util.Commify(workRegion.Size()),
			),
		)
	}

	close(recursiveResultsReturn)
}

func (dgr *Dagger) appendLeaf(res *recursiveSplitResult) {

	var ls dgrblock.LeafSource
	var dr *qringbuf.Region
	var leafLevel int
	if res != nil {
		dr = res.chunkBufRegion
		ls.Chunk = res.chunk
		ls.Content = zcpstring.NewFromSlice(dr.Bytes())

		if ls.Chunk.Meta != nil {
			if sp, exists := ls.Chunk.Meta["sparse"]; exists && sp.(bool) {
				leafLevel = 1
			}
		}
	}

	hdr := dgr.chainedCollectors[0].AppendLeaf(ls)

	if dgr.emitChunks {
		miniHash := " "
		if sk := seenKey(hdr); sk != nil {
			miniHash = fmt.Sprintf(`, "minihash":"%x"`, *sk)
		}

		size := int64(ls.Size)
		if leafLevel > 0 {
			size *= -1
		}

		jsonl := fmt.Sprintf(
			"{\"event\":  \"chunk\",  \"offset\":%12d, \"length\":%7d, %-67s%s }\n",
			dgr.curStreamOffset,
			size,
			fmt.Sprintf(`"cid":"%s"`, hdr.CidBase32()),
			miniHash,
		)
		dgr.maybeSendEvent(NewChunkJsonl, jsonl)

		if _, err := io.WriteString(dgr.cfg.emitters[emChunksJsonl], jsonl); err != nil {
			dgr.maybeSendEvent(ErrorString, err.Error())
			log.Fatalf("Emitting '%s' failed: %s", emChunksJsonl, err)
		}
	}

	// FIXME - I can't actually turn this on and keep convergence...
	// if hdr.IsInlined() {
	// 	if dgr.latestLeafInlined {
	// 		log.Fatalf(
	// 			"Two consecutive inlined leaves in stream - this indicates a logic error in chunkers: enable chunk listing to see offending sequence. Exact arguments were: %s",
	// 			dgr.statSummary.SysStats.ArgvInitial,
	// 		)
	// 	}
	// 	dgr.latestLeafInlined = true
	// } else {
	// 	dgr.latestLeafInlined = false
	// }

	dgr.curStreamOffset += int64(ls.Size)

	// The leaf block processing is entirely decoupled from the collector chain
	// Collectors call that same processor on intermediate link nodes they produce
	dgr.asyncWG.Add(1)
	go dgr.registerNewBlock(
		dgrencoder.NodeOrigin{
			OriginatorIndex: -1,
			LocalSubLayer:   leafLevel,
		},
		hdr,
		dr,
	)
}

// This function is called as multiple "fire and forget" goroutines
// It may only try to send an error event, and it should(?) probably log.Fatal on its own
func (dgr *Dagger) registerNewBlock(
	origin dgrencoder.NodeOrigin,
	hdr *dgrblock.Header,
	dataRegion *qringbuf.Region,
) {
	defer dgr.asyncWG.Done()

	if constants.PerformSanityChecks {
		if hdr == nil {
			log.Panic("block registration of a nil block header reference")
		} else if hdr.SizeBlock() != 0 && hdr.SizeCumulativeDag() == 0 {
			log.Panic("block header with dag-size of 0 encountered")
		}
	}

	atomic.AddInt64(&dgr.statSummary.Dag.Size, int64(hdr.SizeBlock()))
	atomic.AddInt64(&dgr.statSummary.Dag.Nodes, 1)

	if hdr.SizeBlock() > 0 && dgr.seenBlocks != nil {
		if k := seenKey(hdr); k != nil {
			dgr.mu.Lock()

			if s, exists := dgr.seenBlocks[*k]; exists {
				s.seenAt[origin]++
			} else {
				var inblockDataSize int
				if hdr.SizeLinkSection() == 0 {
					inblockDataSize = int(hdr.SizeCumulativePayload())
				}
				s := uniqueBlockStats{
					sizeData:  inblockDataSize, // currently not using this for any stats, but one day...
					sizeBlock: hdr.SizeBlock(),
					seenAt:    seenTimesAt{origin: 1},
				}

				if dgr.uniqueBlockCallback != nil {
					// Due to how we lock the stats above, we will effectively serialize the callbacks
					// This is *exactly* what we want
					s.blockPostProcessResult = dgr.uniqueBlockCallback(hdr)
				}

				dgr.seenBlocks[*k] = s
			}

			dgr.mu.Unlock()
		}
	}

	// Ensure we are finished generating a CID before eviction
	// FIXME - there should be a mechanism to signal we will never
	// need the CID in the first place, so that no effort is spent
	hdr.Cid()

	// If we are holding parts of the qringbuf - we can drop them now
	if dataRegion != nil {
		dataRegion.Release()
	}

	// Once we processed a block in this function - dump all of its content too
	hdr.EvictContent()
}

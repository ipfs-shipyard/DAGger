package dagger

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
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
	carQueueSize           = 2048
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

		// a little helper to deal with error stack craziness
		deferErrors := make(chan error, 1)

		// if we are already in error - just put it on the channel
		// we already sent the event earlier
		if err != nil {
			deferErrors <- err
		}

		// keep sending out events but keep only 1 error to return synchronously
		addErr := func(e error) {
			if e != nil {
				dgr.maybeSendEvent(ErrorString, e.Error())
				select {
				case deferErrors <- e:
				default:
				}
			}
		}

		// we need to wait all crunching to complete, then shutdown emitter, then measure/return
		dgr.asyncWG.Wait()

		// we are writing data: need to wait/close things
		if dgr.carDataQueue != nil {

			close(dgr.carDataQueue)     // signal data-write stop
			addErr(<-dgr.carWriteError) // wait for data-write stop

			// This means we are using fifos, which in turn
			// means we got to close them in sequence and cleanup
			// (we got to close only things we opened, not STDOUT/ERR)
			if dgr.carFifoPins != nil {

				addErr(dgr.carFifoData.Close())

				// If we are already in error, or there are no cars: write the dummy header
				// ( and hope for the best / that we won't hang ... )
				if len(deferErrors) > 0 || len(dgr.seenRoots) == 0 {
					// FIXME - go-ipfs should accept a zero-len without an error...?
					// i.e. just closing should be ok some day
					io.WriteString(dgr.carFifoPins, dgrblock.NulRootCarHeader)
				} else {
					addErr(dgr.writeoutCarPins())
				}

				addErr(dgr.carFifoPins.Close())
				addErr(os.RemoveAll(dgr.carFifoDirectory))
			}
		}

		if err == nil && len(deferErrors) > 0 {
			err = <-deferErrors
		}

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

	dgr.qrb, err = qringbuf.NewFromReader(inputReader, qringbuf.Config{
		// MinRegion must be twice the maxchunk, otherwise chunking chains won't work (hi, Claude Shannon)
		MinRegion:   2 * constants.MaxLeafPayloadSize,
		MinRead:     dgr.cfg.RingBufferMinRead,
		MaxCopy:     2 * constants.MaxLeafPayloadSize, // SANCHECK having it equal to the MinRegion may be daft...
		BufferSize:  dgr.cfg.RingBufferSize,
		SectorSize:  dgr.cfg.RingBufferSectSize,
		Stats:       &dgr.statSummary.SysStats.Stats,
		TrackTiming: ((dgr.cfg.StatsActive & statsRingbuf) == statsRingbuf),
	})
	if err != nil {
		return
	}

	// Spew the nul-delimited names and close
	if dgr.cfg.emitters[emCarV0Fifos] != nil {
		if _, err = fmt.Fprintf(
			dgr.cfg.emitters[emCarV0Fifos],
			"%s\x00%s\x00",
			dgr.carFifoData.Name(),
			dgr.carFifoPins.Name(),
		); err != nil {
			return
		}

		if err = dgr.cfg.emitters[emCarV0Fifos].(*os.File).Close(); err != nil {
			return
		}
	}

	// We got that far - got to write out the data portion prequel
	// .oO( The machine of a dream, such a clean machine
	//      With the pistons a pumpin', and the hubcaps all gleam )
	if dgr.carDataWriter != nil {
		if _, err = io.WriteString(dgr.carDataWriter, dgrblock.NulRootCarHeader); err != nil {
			return
		}

		// start the async writer here, once we know nothing errorred
		dgr.carDataQueue = make(chan carUnit, carQueueSize)
		dgr.carWriteError = make(chan error, 1)
		go dgr.backgroundCarDataWriter()
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
			dgr.streamAppend(nil)
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
				dgr.streamAppend(nil)
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

func (dgr *Dagger) backgroundCarDataWriter() {
	defer close(dgr.carWriteError)

	var err error
	var cid, sizeVI []byte

	for {
		carUnit, chanOpen := <-dgr.carDataQueue
		if !chanOpen {
			return
		}

		cid = carUnit.hdr.Cid()
		sizeVI = util.AppendVarint(
			sizeVI[:0],
			uint64(len(cid)+carUnit.hdr.SizeBlock()),
		)

		if _, err = dgr.carDataWriter.Write(sizeVI); err == nil {
			if _, err = dgr.carDataWriter.Write(cid); err == nil {
				_, err = carUnit.hdr.Content().WriteTo(dgr.carDataWriter)
			}
		}

		carUnit.hdr.EvictContent()
		if carUnit.region != nil {
			carUnit.region.Release()
		}

		if err != nil {
			dgr.maybeSendEvent(ErrorString, err.Error())
			dgr.carWriteError <- err
			return
		}
	}
}

func (dgr *Dagger) writeoutCarPins() (err error) {
	if len(dgr.seenRoots) == 0 {
		log.Panic("called with 0 seen roots, not possible")
	}

	var cidCumulativeLength uint64

	sortedCids := make([][]byte, 0, len(dgr.seenRoots))

	for _, c := range dgr.seenRoots {
		sortedCids = append(sortedCids, c)
		cidCumulativeLength += uint64(
			2 + // prefixed tag 42
				util.CborHeaderWiresize(uint64(len(c))+1) + // that many raw bytes follow
				1 + // the \x00 cid prefix
				len(c),
		)
	}

	sort.Slice(sortedCids, func(i, j int) bool {
		return (bytes.Compare(sortedCids[i], sortedCids[j]) < 0)
	})

	//
	// Writeout starts here
	//

	// Roots preamble as varint length
	if _, err = dgr.carFifoPins.Write(util.VarintSlice(
		7 +
			uint64(util.CborHeaderWiresize(uint64(len(sortedCids)))) +
			cidCumulativeLength +
			9,
	)); err != nil {
		return
	}

	// See definition of NulRootCarHeader for detailed description of each byte
	if _, err = io.WriteString(dgr.carFifoPins, "\xA2\x65\x72\x6F\x6F\x74\x73"); err != nil {
		return
	}

	// Writeout root array header
	if err = util.CborHeaderWrite(
		dgr.carFifoPins,
		4,
		uint64(len(sortedCids)),
	); err != nil {
		return
	}

	for i := range sortedCids {
		if _, err = dgr.carFifoPins.Write([]byte{0xd8, 0x2a}); err != nil {
			return
		}

		if err = util.CborHeaderWrite(
			dgr.carFifoPins,
			2,
			uint64(1+len(sortedCids[i])),
		); err != nil {
			return
		}

		if _, err = dgr.carFifoPins.Write([]byte{0}); err != nil {
			return
		}
		if _, err = dgr.carFifoPins.Write(sortedCids[i]); err != nil {
			return
		}
	}

	// Version header outro
	_, err = io.WriteString(dgr.carFifoPins, "\x67\x76\x65\x72\x73\x69\x6F\x6E\x01")
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
			dgr.chainedChunkers[chunkerIdx].instance,
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
	dgr.streamAppend(result)
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

		if len(dgr.chainedChunkers) > chunkerIdx+1 && !c.Meta.Bool("no-subchunking") {
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

func (dgr *Dagger) streamAppend(res *recursiveSplitResult) {

	var ds dgrblock.DataSource
	var dr *qringbuf.Region
	var leafLevel int
	if res != nil {
		dr = res.chunkBufRegion
		ds.Chunk = res.chunk
		ds.Content = zcpstring.NewFromSlice(dr.Bytes())

		if ds.Meta.Bool("is-padding") {
			leafLevel = 1
		}
	}

	hdr := dgr.chainedCollectors[0].AppendData(ds)

	if dgr.emitChunks {
		miniHash := " "
		if sk := seenKey(hdr); sk != nil {
			miniHash = fmt.Sprintf(`, "minihash":"%x"`, *sk)
		}

		size := int64(ds.Size)
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

	dgr.curStreamOffset += int64(ds.Size)

	// The leaf block processing is entirely decoupled from the collector chain,
	// in order to not leak the Region lifetime management outside the framework
	// Collectors call that same processor on intermediate link nodes they produce
	dgr.asyncWG.Add(1)
	go dgr.postProcessBlock(
		dgrencoder.NodeOrigin{
			OriginatingLayer: -1,
			LocalSubLayer:    leafLevel,
		},
		hdr,
		dr,
	)
}

// This function is called as multiple "fire and forget" goroutines
// It may only try to send an error event, and it should(?) probably log.Fatal on its own
func (dgr *Dagger) postProcessBlock(
	blockOrigin dgrencoder.NodeOrigin,
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

		if blockOrigin.OriginatingLayer == 0 {
			log.Panicf("Unexpected origin spec: %#v", blockOrigin)
		}
	}

	atomic.AddInt64(&dgr.statSummary.Dag.Size, int64(hdr.SizeBlock()))
	atomic.AddInt64(&dgr.statSummary.Dag.Nodes, 1)

	if hdr.SizeBlock() > 0 && dgr.seenBlocks != nil {
		if k := seenKey(hdr); k != nil {

			var postprocSlot *blockPostProcessResult

			dgr.mu.Lock()

			if s, exists := dgr.seenBlocks[*k]; exists {
				s.seenAt[blockOrigin]++
			} else {
				postprocSlot = &blockPostProcessResult{}
				dgr.seenBlocks[*k] = uniqueBlockStats{
					sizeBlock:              hdr.SizeBlock(),
					seenAt:                 seenTimesAt{blockOrigin: 1},
					blockPostProcessResult: postprocSlot,
				}
			}

			dgr.mu.Unlock()

			if postprocSlot != nil {

				//
				// FIXME compressor stuff goes here
				//

				if dgr.carDataQueue != nil {
					dgr.carDataQueue <- carUnit{hdr: hdr, region: dataRegion}
					return
				}
			}
		}
	}

	// NOTE - these 3 steps will be done by the car emitter ( early return above )
	// if that's what the options ask for
	{
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
}

package dagger

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	"github.com/ipfs-shipyard/DAGger/internal/zcpstring"
	"github.com/ipfs/go-qringbuf"
	"golang.org/x/sys/unix"
)

// SANCHECK: not sure if any of these make sense, nor have I measured the cost
const (
	chunkQueueSizeTop      = 256
	chunkQueueSizeSubchunk = 32
)

func (dgr *Dagger) ProcessReader(inputReader io.Reader, optionalRootsReceiver chan<- *block.Header) error {

	var initErr error
	dgr.qrb, initErr = qringbuf.NewFromReader(inputReader, qringbuf.Config{
		// MinRegion must be twice the maxchunk, otherwise chunking chains won't work (hi, Claude Shannon)
		// SANCHECK: this may not necessarily be true...
		MinRegion:   2 * dgr.cfg.GlobalMaxChunkSize,
		MinRead:     dgr.cfg.RingBufferMinRead,
		MaxCopy:     2 * dgr.cfg.GlobalMaxChunkSize, // SANCHECK goes with above
		BufferSize:  dgr.cfg.RingBufferSize,
		SectorSize:  dgr.cfg.RingBufferSectSize,
		Stats:       &dgr.statSummary.SysStats.Stats,
		TrackTiming: ((dgr.cfg.StatsEnabled & statsRingbuf) == statsRingbuf),
	})
	if initErr != nil {
		return initErr
	}

	if (dgr.cfg.StatsEnabled & statsBlocks) == statsBlocks {
		dgr.seenBlocks = make(seenBlocks, 1024) // SANCHECK: somewhat arbitrary, but eh...
		dgr.seenRoots = make(seenRoots, 32)
	}

	// use 64bits everywhere
	var substreamSize int64

	var t0 time.Time
	var r0, r1 unix.Rusage

	defer func() {
		sys := &dgr.statSummary.SysStats

		// we need to finish all emissions to complete before we measure/return
		dgr.asyncWG.Wait()

		sys.ElapsedNsecs = time.Since(t0).Nanoseconds()
		unix.Getrusage(unix.RUSAGE_SELF, &r1) // ignore errors

		if dgr.asyncHasherBus != nil {
			gr := runtime.NumGoroutine()

			// signal the hashers to shut down
			close(dgr.asyncHasherBus)

			if constants.PerformSanityChecks {
				// we will be checking for leaked goroutines - wait a bit for hashers to shut down
				for {
					time.Sleep(10 * time.Millisecond)
					if runtime.NumGoroutine() <= gr-dgr.cfg.AsyncHashers {
						break
					}
				}
			}
		}

		sys.CpuUserNsecs = unix.TimevalToNsec(r1.Utime) - unix.TimevalToNsec(r0.Utime)
		sys.CpuSysNsecs = unix.TimevalToNsec(r1.Stime) - unix.TimevalToNsec(r0.Stime)
		sys.MinFlt = r1.Minflt - r0.Minflt
		sys.MajFlt = r1.Majflt - r0.Majflt
		sys.BioRead = r1.Inblock - r0.Inblock
		sys.BioWrite = r1.Oublock - r0.Oublock
		sys.Sigs = r1.Nsignals - r0.Nsignals
		sys.CtxSwYield = r1.Nvcsw - r0.Nvcsw
		sys.CtxSwForced = r1.Nivcsw - r0.Nivcsw

		sys.MaxRssBytes = r1.Maxrss
		if runtime.GOOS != "darwin" {
			// anywhere but mac maxrss is actually KiB
			sys.MaxRssBytes *= 1024
		}
	}()

	unix.Getrusage(unix.RUSAGE_SELF, &r0) // ignore errors
	t0 = time.Now()

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
		}

		if dgr.cfg.MultipartStream && substreamSize == 0 {
			// If we got here: cfg.ProcessNulInputs is true
			// Special case for a one-time zero-CID emission
			dgr.injectZeroLeaf()
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
				dgr.injectZeroLeaf()
			}
		}

		if dgr.cfg.generateRoots || dgr.seenRoots != nil || optionalRootsReceiver != nil {

			// pull the root out of the last member of the linker chain
			rootBlock := dgr.chainedLinkers[len(dgr.chainedLinkers)-1].DeriveRoot()

			// send a struct, nil or not
			if optionalRootsReceiver != nil {
				optionalRootsReceiver <- rootBlock
			}

			if rootBlock != nil {

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

				if dgr.cfg.emitters[emRootsJsonl] != nil {
					if _, err := fmt.Fprintf(
						dgr.cfg.emitters[emRootsJsonl],
						"{\"event\":   \"root\", \"payload\":%12d, \"stream\":%7d, \"cid\":\"%s\", \"wiresize\":%12d }\n",
						rootBlock.SizeCumulativePayload(),
						dgr.statSummary.Streams,
						rootBlock.CidBase32(),
						rootBlock.SizeCumulativeDag(),
					); err != nil {
						log.Fatalf("Emitting '%s' failed: %s", emRootsJsonl, err)
					}
				}
			}
		}

		// we are in EOF-state: if we are not expecting multiparts - we are done
		if !dgr.cfg.MultipartStream {
			break
		}
	}

	return nil
}

// We have too many special cases around zero-length streams: handle them
// with a made up leaf
func (dgr *Dagger) injectZeroLeaf() {
	dgr.appendLeaf(
		dgr.chainedLinkers[0].NewLeafBlock(block.DataSource{}),
		nil,
	)
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

	var availableFromReader int
	var streamOffset int64

	// this callback is passed through the recursive chain instead of a bare channel
	// providing reasonable diag context
	chunkingErr := make(chan error, 1)

	errHandler := func(chunkerIdx int, wrOffset int64, wrSize, wrPos int, errStr string) {
		chunkingErr <- fmt.Errorf(`

chunking error
--------------
MainRegionSize:      %17s
SubRegionSize:       %17s
ErrorAtSubRegionPos: %17s
StreamOffset:        %17s
SubBufOffset:        %17s
ErrorOffset:         %17s
chunker: #%d %T
--------------
%s%s`,
			util.Commify(availableFromReader),
			util.Commify(wrSize),
			util.Commify(wrPos),
			util.Commify64(streamOffset),
			util.Commify64(wrOffset),
			util.Commify64(wrOffset+int64(wrPos)),
			chunkerIdx,
			dgr.chainedChunkers[chunkerIdx],
			errStr,
			"\n\n",
		)
	}

	var processedFromReader int
	for {

		// next 2 lines evaluate processedInRound and availableForRound from *LAST* iteration
		streamOffset += int64(processedFromReader)
		workRegion, readErr := dgr.qrb.NextRegion(availableFromReader - processedFromReader)

		if workRegion == nil || (readErr != nil && readErr != io.EOF) {
			return readErr
		}

		availableFromReader = workRegion.Size()
		processedFromReader = 0

		rescursiveSplitResults := make(chan *recursiveSplitResult, chunkQueueSizeTop)
		go dgr.recursivelySplitBuffer(
			// The entire reserved buffer to split recursively
			// Guaranteed to be left intact until we call NextSlice
			workRegion,
			// Where we are (for error reporting)
			streamOffset,
			// We need to tell the top chunker that potentially more is coming, so that the entire chain won't repeat work
			(readErr == nil),
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
				processedFromReader += dgr.reassembleRecursiveResults(res)
			}
		}

		dgr.statSummary.Dag.Payload += int64(processedFromReader)
	}
}

func (dgr *Dagger) reassembleRecursiveResults(result *recursiveSplitResult) int {

	if result.subSplits != nil {
		var substreamSize int
		for {
			subRes, channelOpen := <-result.subSplits
			if !channelOpen {
				return substreamSize
			}
			substreamSize += dgr.reassembleRecursiveResults(subRes)
		}
	}

	result.chunkBufRegion.Reserve()

	// FIXME - need to validate there are indeed no errors to check ( something with car writing...? )
	dgr.appendLeaf(
		dgr.chainedLinkers[0].NewLeafBlock(block.DataSource{
			Chunk:   result.chunk,
			Content: zcpstring.NewFromSlice(result.chunkBufRegion.Bytes()),
		}),
		result.chunkBufRegion,
	)

	return result.chunk.Size
}

func (dgr *Dagger) recursivelySplitBuffer(
	workRegion *qringbuf.Region,
	workRegionStreamOffset int64,
	moreDataIsComing bool,
	chunkerIdx int,
	recursiveResultsReturn chan<- *recursiveSplitResult,
	errHandler chunkingInconsistencyHandler,
) {
	defer close(recursiveResultsReturn)

	var processedBytes int

	// SANCHECK
	// It is not clear whether it is better to give this as a callback to
	// a chunker directly, INSTEAD of the separate goroutine we have below
	chunkProcessor := func(c chunker.Chunk) {
		if c.Size <= 0 || c.Size > workRegion.Size()-processedBytes {
			errHandler(chunkerIdx, workRegionStreamOffset, workRegion.Size(), processedBytes,
				fmt.Sprintf("returned invalid chunk with size %d", c.Size),
			)
			return
		}

		if len(dgr.chainedChunkers) > chunkerIdx+1 {
			// we are not last in the chain - subchunk
			subSplits := make(chan *recursiveSplitResult, chunkQueueSizeSubchunk)
			go dgr.recursivelySplitBuffer(
				workRegion.SubRegion(
					processedBytes,
					c.Size,
				),
				workRegionStreamOffset+int64(processedBytes),
				false, // no "more is coming": when we are sub-chunking we always provide *all* the data
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
	}

	chanSize := chunkQueueSizeSubchunk
	if chunkerIdx == 0 {
		chanSize = chunkQueueSizeTop
	}
	chunkRetChan := make(chan chunker.Chunk, chanSize)

	go func() {
		dgr.chainedChunkers[chunkerIdx].Split(
			workRegion.Bytes(),
			moreDataIsComing,
			func(c chunker.Chunk) { chunkRetChan <- c },
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

	if processedBytes > workRegion.Size() ||
		(!moreDataIsComing && processedBytes != workRegion.Size()) {
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
		return
	}
}

func (dgr *Dagger) appendLeaf(hdr *block.Header, dr *qringbuf.Region) {

	dgr.chainedLinkers[0].AppendBlock(hdr)

	if dgr.cfg.emitChunks {
		var miniHash string
		if sk := seenKey(hdr); sk != nil {
			miniHash = fmt.Sprintf("%x", *sk)
		}

		// this is a leaf - the total payload is the block data itself
		size := int64(hdr.SizeCumulativePayload())
		if hdr.IsSparse() {
			size *= -1
		}
		if _, err := fmt.Fprintf(
			dgr.cfg.emitters[emChunksJsonl],
			"{\"event\":  \"chunk\",  \"offset\":%12d, \"length\":%7d, \"cid\":\"%s\", \"minihash\":\"%s\" }\n",
			dgr.curStreamOffset,
			size,
			hdr.CidBase32(),
			miniHash,
		); err != nil {
			log.Fatalf("Emitting '%s' failed: %s", emChunksJsonl, err)
		}
	}

	dgr.curStreamOffset += int64(hdr.SizeCumulativePayload())

	// The leaf block processing is entirely decoupled from the linker chain
	// Linkers call the same processor on intermediate link blocks they produce
	dgr.asyncWG.Add(1)
	if hdr.IsSparse() {
		go dgr.registerNewBlock(hdr, generatedBy{0, 1}, nil, dr)
	} else {
		go dgr.registerNewBlock(hdr, generatedBy{0, 0}, nil, dr)
	}
}

// This function is called as "fire and forget" goroutines
// It may only panic, no error handling
func (dgr *Dagger) registerNewBlock(
	hdr *block.Header,
	gen generatedBy,
	links []*block.Header,
	dataRegion *qringbuf.Region,
) {
	defer dgr.asyncWG.Done()

	if constants.PerformSanityChecks {
		if hdr == nil {
			util.InternalPanicf("processor called with a nil block header reference")
		} else if hdr.SizeBlock() != 0 && hdr.SizeCumulativeDag() == 0 {
			util.InternalPanicf("block header with dag-size of 0 encountered")
		}
	}

	atomic.AddInt64(&dgr.statSummary.Dag.Size, int64(hdr.SizeBlock()))
	atomic.AddInt64(&dgr.statSummary.Dag.Nodes, 1)

	if dgr.seenBlocks != nil {
		if k := seenKey(hdr); k != nil {
			dgr.mu.Lock()

			if s, exists := dgr.seenBlocks[*k]; exists {
				s.seenAt[gen]++
			} else {
				var inblockDataSize int
				if hdr.SizeLinkSection() == 0 {
					inblockDataSize = int(hdr.SizeCumulativePayload())
				}
				s := uniqueBlockStats{
					// currently not using this for any stats, but one day...
					sizeData:  inblockDataSize,
					sizeBlock: hdr.SizeBlock(),
					seenAt:    seenTimesAt{gen: 1},
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

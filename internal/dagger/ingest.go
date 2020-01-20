package dagger

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"sync/atomic"
	"time"

	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	"github.com/ipfs-shipyard/DAGger/internal/zcpstring"
	"github.com/ipfs/go-qringbuf"
)

// SANCHECK: not sure if any of these make sense, nor have I measured the cost
const (
	subChunksQueueSizeLevel0 = 256
	subChunksQueueSizeLevelN = 16
)

func ProcessReader(cfg *config, inputReader io.Reader, optionalRootsReceiver chan<- *block.Header) error {

	var initErr error
	cfg.qrb, initErr = qringbuf.NewFromReader(inputReader, qringbuf.Config{
		// min-emit must be twice the maxchunk, otherwise chunking chains won't work (hi, Claude Shannon)
		// SANCHECK: this may not necessarily be true...
		MinRegion:  2 * cfg.GlobalMaxChunkSize,
		MinRead:    cfg.RingBufferMinRead,
		MaxCopy:    2 * cfg.GlobalMaxChunkSize, // SANCHECK goes with above
		BufferSize: cfg.RingBufferSize,
		SectorSize: cfg.RingBufferSectSize,
		Stats:      &cfg.stats.summary.SysStats.Stats,
	})

	if initErr != nil {
		return initErr
	}

	// use 64bits everywhere
	var substreamBytes int64

	t0 := time.Now()
	defer func() {
		// we need to finish all stat writes before we return
		defer cfg.asyncWG.Wait()

		cfg.stats.summary.SysStats.ElapsedNsecs = time.Since(t0).Nanoseconds()
	}()

	// outer stream loop: read() syscalls happen only here and in the qrb.collector()
	for {
		if cfg.MultipartStream {

			err := binary.Read(
				inputReader,
				binary.BigEndian,
				&substreamBytes,
			)
			cfg.stats.summary.SysStats.ReadCalls++

			if err == io.EOF {
				// no new multipart coming - bail
				break
			} else if err != nil {
				return fmt.Errorf(
					"error reading next 8-byte multipart substream size: %s",
					err,
				)
			}

			if substreamBytes == 0 && !cfg.ProcessNulInputs {
				continue
			}

			cfg.stats.totalStreams++
			cfg.stats.curStreamOffset = 0
		}

		if cfg.MultipartStream && substreamBytes == 0 {
			// If we got here: cfg.ProcessNulInputs is true
			// Special case for a one-time zero-CID emission
			_injectZeroLeaf(cfg)
		} else if err := processStream(cfg, substreamBytes); err != nil {
			if err == io.ErrUnexpectedEOF {
				return fmt.Errorf(
					"unexpected end of substream #%s after %s bytes (stream expected to be %s bytes long)",
					util.Commify64(cfg.stats.totalStreams),
					util.Commify64(cfg.stats.curStreamOffset+int64(cfg.qrb.Buffered())),
					util.Commify64(substreamBytes),
				)
			} else if err != io.EOF {
				return err
			} else if cfg.stats.curStreamOffset == 0 && cfg.ProcessNulInputs {
				// we did try to process a stream and ended up with an EOF + 0
				// emit a zero-CID like above
				_injectZeroLeaf(cfg)
			}
		}

		if cfg.generateRoots || cfg.stats.seenBlocks != nil || optionalRootsReceiver != nil {

			// pull the root out of the last member of the linker chain
			rootBlock := cfg.chainedLinkers[len(cfg.chainedLinkers)-1].DeriveRoot()

			// send a struct, nil or not
			if optionalRootsReceiver != nil {
				optionalRootsReceiver <- rootBlock
			}

			if rootBlock != nil {

				if cfg.stats.seenRoots != nil {
					var rootSeen bool

					if sk := seenKey(rootBlock); sk != nil {
						if _, rootSeen = cfg.stats.seenRoots[*sk]; !rootSeen {
							cfg.stats.seenRoots[*sk] = rootBlock.Cid()
						}
					}

					cfg.stats.summary.Roots = append(cfg.stats.summary.Roots, rootStats{
						Cid:         rootBlock.CidBase32(),
						SizePayload: rootBlock.SizeCumulativePayload(),
						SizeDag:     rootBlock.SizeCumulativeDag(),
						Dup:         rootSeen,
					})
				}

				if cfg.emitters[emRootsJsonl] != nil {
					if _, err := fmt.Fprintf(
						cfg.emitters[emRootsJsonl],
						"{\"event\":   \"root\", \"payload\":%12d, \"stream\":%7d, \"cid\":\"%s\", \"wiresize\":%12d }\n",
						rootBlock.SizeCumulativePayload(),
						cfg.stats.totalStreams,
						rootBlock.CidBase32(),
						rootBlock.SizeCumulativeDag(),
					); err != nil {
						log.Fatalf("Emitting '%s' failed: %s", emRootsJsonl, err)
					}
				}
			}
		}

		// we are in EOF-state: if we are not expecting multiparts - we are done
		if !cfg.MultipartStream {
			break
		}
	}

	return nil
}

// We have too many special cases around zero-length streams: handle them
// with a made up leaf
func _injectZeroLeaf(cfg *config) {
	appendLeaf(
		cfg,
		cfg.chainedLinkers[0].NewLeafBlock(block.DataSource{}),
		nil,
	)
}

// This is essentially a union:
// - either subSplits will be provided for recursion
// - or a chunk with its region will be sent
type recursiveSplitResult struct {
	subSplits      <-chan *recursiveSplitResult
	chunkBufRegion *qringbuf.Region
	chunk          chunker.SplitResult
}

type chunkingInconsistencyHandler func(
	chunkerIdx int,
	workRegionStreamOffset int64,
	workRegionSize,
	workRegionPos int,
	errStr string,
)

func processStream(cfg *config, streamLimit int64) error {

	// begin reading and filling buffer
	if err := cfg.qrb.StartFill(streamLimit); err != nil {
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
MainBufSize:       %17s
SubBufSize:        %17s
ErrorAtSubBufPos:  %17s
StreamOffset:      %17s
SubBufOffset:      %17s
ErrorOffset:       %17s
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
			cfg.chainedChunkers[chunkerIdx],
			errStr,
			"\n\n",
		)
	}

	var processedFromReader int
	for {

		// next 2 lines evaluate processedInRound and availableForRound from *LAST* iteration
		streamOffset += int64(processedFromReader)
		workRegion, readErr := cfg.qrb.NextRegion(availableFromReader - processedFromReader)

		if workRegion == nil || (readErr != nil && readErr != io.EOF) {
			return readErr
		}

		availableFromReader = workRegion.Size()
		processedFromReader = 0

		rescursiveSplitResults := make(chan *recursiveSplitResult, subChunksQueueSizeLevel0)
		go recursivelySplitBuffer(
			cfg,
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
				processedFromReader += reassembleRecursiveResults(cfg, res)
			}
		}

		cfg.stats.summary.Dag.Payload += int64(processedFromReader)
	}
}

func reassembleRecursiveResults(cfg *config, result *recursiveSplitResult) int {

	if result.subSplits != nil {
		var substreamSize int
		for {
			subRes, channelOpen := <-result.subSplits
			if !channelOpen {
				return substreamSize
			}
			substreamSize += reassembleRecursiveResults(cfg, subRes)
		}
	}

	result.chunkBufRegion.Reserve()

	// FIXME - need to validate there are indeed no errors to check ( something with car writing...? )
	appendLeaf(
		cfg,
		cfg.chainedLinkers[0].NewLeafBlock(block.DataSource{
			SplitResult: result.chunk,
			Content:     zcpstring.NewFromSlice(result.chunkBufRegion.Bytes()),
		}),
		result.chunkBufRegion,
	)

	return result.chunk.Size
}

func recursivelySplitBuffer(
	cfg *config,
	workRegion *qringbuf.Region,
	workRegionStreamOffset int64,
	moreDataIsComing bool,
	chunkerIdx int,
	recursiveResultsReturn chan<- *recursiveSplitResult,
	errHandler chunkingInconsistencyHandler,
) {
	defer close(recursiveResultsReturn)

	var resultingChunks chan chunker.SplitResult
	if chunkerIdx == 0 {
		resultingChunks = make(chan chunker.SplitResult, subChunksQueueSizeLevel0)
	} else {
		resultingChunks = make(chan chunker.SplitResult, subChunksQueueSizeLevelN)
	}

	go cfg.chainedChunkers[chunkerIdx].Split(
		workRegion.Bytes(),
		moreDataIsComing,
		resultingChunks,
	)

	var processedBytes int
	for {
		chunk, chanOpen := <-resultingChunks
		if !chanOpen {
			break
		}

		if chunk.Size <= 0 || chunk.Size > workRegion.Size()-processedBytes {
			errHandler(chunkerIdx, workRegionStreamOffset, workRegion.Size(), processedBytes,
				fmt.Sprintf("returned invalid chunk with size %d", chunk.Size),
			)
			return
		}

		if len(cfg.chainedChunkers) > chunkerIdx+1 {
			// we are not last in the chain - subchunk
			subSplits := make(chan *recursiveSplitResult, subChunksQueueSizeLevelN)
			go recursivelySplitBuffer(
				cfg,
				workRegion.SubRegion(
					processedBytes,
					chunk.Size,
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
				chunk: chunk,
				chunkBufRegion: workRegion.SubRegion(
					processedBytes,
					chunk.Size,
				),
			}
		}

		processedBytes += chunk.Size
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

func appendLeaf(cfg *config, hdr *block.Header, dr *qringbuf.Region) {

	cfg.chainedLinkers[0].AppendBlock(hdr)

	if cfg.emitChunks {
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
			cfg.emitters[emChunksJsonl],
			"{\"event\":  \"chunk\",  \"offset\":%12d, \"length\":%7d, \"cid\":\"%s\", \"minihash\":\"%s\" }\n",
			cfg.stats.curStreamOffset,
			size,
			hdr.CidBase32(),
			miniHash,
		); err != nil {
			log.Fatalf("Emitting '%s' failed: %s", emChunksJsonl, err)
		}
	}

	cfg.stats.curStreamOffset += int64(hdr.SizeCumulativePayload())

	// The leaf block processing is entirely decoupled from the linker chain
	// Linkers call the same processor on intermediate link blocks they produce
	cfg.asyncWG.Add(1)
	if hdr.IsSparse() {
		go registerNewBlock(cfg, hdr, generatedBy{0, 1}, nil, dr)
	} else {
		go registerNewBlock(cfg, hdr, generatedBy{0, 0}, nil, dr)
	}
}

// This function is called as "fire and forget" goroutines
// It may only panic, no error handling
func registerNewBlock(
	cfg *config,
	hdr *block.Header,
	gen generatedBy,
	links []*block.Header,
	dataRegion *qringbuf.Region,
) {
	defer cfg.asyncWG.Done()

	if constants.PerformSanityChecks {
		if hdr == nil {
			util.InternalPanicf("processor called with a nil block header reference")
		} else if hdr.SizeBlock() != 0 && hdr.SizeCumulativeDag() == 0 {
			util.InternalPanicf("block header with dag-size of 0 encountered")
		}
	}

	atomic.AddInt64(&cfg.stats.summary.Dag.Size, int64(hdr.SizeBlock()))
	atomic.AddInt64(&cfg.stats.summary.Dag.Nodes, 1)

	if cfg.stats.seenBlocks != nil {
		if k := seenKey(hdr); k != nil {
			cfg.stats.mu.Lock()

			if s, exists := cfg.stats.seenBlocks[*k]; exists {
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

				if cfg.uniqueBlockCallback != nil {
					// Due to how we lock the stats above, we will effectively serialize the callbacks
					// This is *exactly* what we want
					s.blockPostProcessResult = cfg.uniqueBlockCallback(hdr)
				}

				cfg.stats.seenBlocks[*k] = s
			}

			cfg.stats.mu.Unlock()
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

	// Once we processed a block in this function - we can dump all of its content too
	// Helps with general memory pressure on large DAGs
	hdr.EvictContent()
}

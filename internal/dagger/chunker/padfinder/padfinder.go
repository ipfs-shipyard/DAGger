package sparserun

import (
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

type sparseRunChunker struct {
	size int
}

func (c *sparseRunChunker) MinChunkSize() int { return c.size }

func (c *sparseRunChunker) Split(buf []byte, moreDataNextInvocation bool, slices chan<- chunker.SplitResult) {
	defer close(slices)

	/*
		-			var sparseStartPos, sparseRunSize int
		-
		-			// sparseness-search takes precedence when requested
		-			if findSparseRuns &&
		-				cfg.rb.Buffered() >= cfg.SparseRunMinSize {
		-
		-				sparseSearchSize := cfg.rb.Buffered()
		-				if sparseSearchSize > 2*cfg.MaxChunkSize {
		-					sparseSearchSize = 2 * cfg.MaxChunkSize
		-				}
		-
		-				firstImpossiblePos := sparseSearchSize - cfg.SparseRunMinSize
		-				sparseSearchBuf := cfg.rb.Peek(sparseSearchSize)
		-
		-				for bufPos := 0; bufPos < firstImpossiblePos && sparseRunSize < cfg.SparseRunMinSize; bufPos++ {
		-
		-					if sparseSearchBuf[bufPos] != 0 {
		-						// boyer-moore-ish-like long jump
		-						// FIXME - this can be done even more efficient... I think
		-						if sparseSearchBuf[bufPos+cfg.SparseRunMinSize] != 0 {
		-							bufPos += cfg.SparseRunMinSize - 1
		-						}
		-						sparseStartPos = bufPos + 1
		-					} else if sparseSearchBuf[sparseStartPos] != sparseSearchBuf[bufPos] {
		-						sparseStartPos = bufPos
		-					}
		-
		-					sparseRunSize = bufPos - sparseStartPos + 1
		-				}
		-
		-				// we found something - go forward as much as we can
		-				if sparseRunSize >= cfg.SparseRunMinSize {
		-					for (sparseRunSize < cfg.MaxChunkSize) &&
		-						(sparseStartPos+sparseRunSize < sparseSearchSize) &&
		-						(sparseSearchBuf[sparseStartPos+sparseRunSize-1] == sparseSearchBuf[sparseStartPos+sparseRunSize]) {
		-						sparseRunSize++
		-					}
		-				}
		-			}

	*/

}

func NewChunker(args []string, cfg *chunker.CommonConfig) (_ chunker.Chunker, initErrs []string) {

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		return nil, util.SubHelp(
			"Splits buffer into equally sized chunks. Requires a single parameter: the\n"+
				"size of each chunk in bytes (IPFS default: 262144)",
			nil,
		)
	}

	c := sparseRunChunker{}

	// if len(args) != 2 {
	// 	initErrs = append(initErrs, "chunker requires an integer argument, the size of each chunk in bytes")
	// } else {
	// 	sizearg, err := strconv.ParseUint(
	// 		args[1][2:], // stripping off '--'
	// 		10,
	// 		25, // 25bits == 32 * 1024 * 1024 == 32MiB
	// 	)
	// 	if err != nil {
	// 		initErrs = append(initErrs, fmt.Sprintf("argument parse failed: %s", err))
	// 	} else {
	// 		c.size = int(sizearg)
	// 	}
	// }

	// if c.size > cfg.GlobalMaxChunkSize {
	// 	initErrs = append(initErrs, fmt.Sprintf(
	// 		"provided chunk size '%s' exceeds specified maximum payload size '%s",
	// 		util.Commify(c.size),
	// 		util.Commify(cfg.GlobalMaxChunkSize),
	// 	))
	// }

	return &c, initErrs
}

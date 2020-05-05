package padfinder

import (
	"fmt"
	"regexp"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

type config struct {
	Re2Longest string `getopt:"--re2-longest Stuff"`
}

type padfinderPreChunker struct {
	maxChunkSize int
	re2          *regexp.Regexp
	config
}

// anything goes in this round
func (c *padfinderPreChunker) MinChunkSize() int { return 0 }

func (c *padfinderPreChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb chunker.SplitResultCallback,
) (err error) {

	return nil
}

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

func NewChunker(args []string, dgrCfg *chunker.DaggerConfig) (_ chunker.Chunker, initErrs []string) {

	c := padfinderPreChunker{
		maxChunkSize: dgrCfg.GlobalMaxChunkSize,
	}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		// A panic as this should not be possible
		dgrCfg.InternalPanicf(
			"option set registration failed: %s",
			err,
		)
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		return nil, util.SubHelp(
			"PAD FIXME",
			optSet,
		)
	}

	// bail early if getopt fails
	if err := optSet.Getopt(args, nil); err != nil {
		return nil, []string{err.Error()}
	}

	if c.Re2Longest == "" {
		// FIXME - some day support more options but this one
		return nil, []string{"one of the available match options must be used"}
	} else {
		var err error
		if c.re2, err = regexp.Compile(c.Re2Longest); err != nil {
			initErrs = append(initErrs, fmt.Sprintf(
				"compilation of\n%s\n\tfailed: %s",
				c.Re2Longest,
				err,
			))
		} else {
			c.re2.Longest()
		}
	}

	return &c, initErrs
}

package padfinder

import (
	"github.com/ipfs-shipyard/DAGger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/constants"
)

type config struct {
	MaxRunSize       int    `getopt:"--max-pad-run"`
	StaticMaxLiteral int    `getopt:"--static-pad-literal-max"`
	StaticMinRepeats int    `getopt:"--static-pad-min-repeats"`
	StaticPadHex     string `getopt:"--pad-static-hex"`
	freeFormRE       string
}

type padfinderPreChunker struct {
	finder  finderInstance
	padMeta chunker.ChunkMeta
	config
}

func (c *padfinderPreChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb chunker.SplitResultCallback,
) (err error) {

	postBufIdx := len(buf)
	var curIdx, matchStart, matchEnd int
	var didOverflow bool

	for {
		// we will be running out of data, but still *could* run a round
		// abort early if we are allowed to
		if !useEntireBuffer &&
			curIdx+2*constants.MaxLeafPayloadSize > postBufIdx {
			return
		}

		if matchStart, matchEnd = c.finder.findNext(buf[curIdx:]); matchEnd > 0 {
			// We did match *somewhere* in the buffer - let's break this down

			// NOTE: the match{Start,End} offsets are relative to curIdx as it is NOW
			didOverflow = (curIdx+matchEnd == postBufIdx)

			// There is some leading non-pad data: pass it down the chain
			if matchStart != 0 {
				if err = cb(chunker.Chunk{Size: matchStart}); err != nil {
					return
				}
				curIdx += matchStart
			}

			sparseLen := matchEnd - matchStart

			// loop for identically sized runs
			for sparseLen >= c.MaxRunSize {
				if err = c.padSubSplit(c.MaxRunSize, cb); err != nil {
					return
				}
				sparseLen -= c.MaxRunSize
				curIdx += c.MaxRunSize
			}

			if didOverflow && !useEntireBuffer {
				return
			}

			// deal with leftover if any
			if sparseLen > 0 {
				if err = c.padSubSplit(sparseLen, cb); err != nil {
					return
				}
				curIdx += sparseLen
			}

		} else {
			// No match at all: no callback, just return right now so that the
			// underlying chunker has a chance to work the region on its own
			// Then we will come back here anew
			return
		}
	}
}

func (c *padfinderPreChunker) padSubSplit(
	length int,
	cb chunker.SplitResultCallback,
) error {

	// nothing to subsplit
	if c.StaticPadHex == "" {
		return cb(chunker.Chunk{
			Size: length,
			Meta: c.padMeta,
		})
	}

	// start with the shorter "leader", even-sized chunks follow after
	if (length % c.StaticMaxLiteral) > 0 {
		if err := cb(chunker.Chunk{
			Size: length % c.StaticMaxLiteral,
			Meta: c.padMeta,
		}); err != nil {
			return err
		}
	}

	// and now the even-sized
	for i := length / c.StaticMaxLiteral; i > 0; i-- {
		if err := cb(chunker.Chunk{
			Size: c.StaticMaxLiteral,
			Meta: c.padMeta,
		}); err != nil {
			return err
		}
	}

	return nil
}

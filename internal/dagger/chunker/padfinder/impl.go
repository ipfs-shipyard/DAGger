package padfinder

import (
	"github.com/ipfs-shipyard/DAGger/chunker"
)

type config struct {
	Re2Longest string `getopt:"--re2-longest Stuff"`
}

type padfinderPreChunker struct {
	maxChunkSize int
	config
}

var sparseMeta map[string]interface{} = map[string]interface{}{
	"sparse":        true,
	"nosubchunking": true,
}

func (c *padfinderPreChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb chunker.SplitResultCallback,
) (err error) {

	postBufIdx := len(buf)
	var matchOverflow bool
	var curIdx, padLen int

	for {
		// we will be running out of data, but still *could* run a round
		// abort early if we are allowed to
		if !useEntireBuffer &&
			curIdx+2*c.maxChunkSize > postBufIdx {
			return
		}

		var m [2]int
		{
			minSparseSize := 42
			firstImpossiblePos := postBufIdx - minSparseSize

			m[0] = curIdx
			m[1] = m[0]

			for m[1] < firstImpossiblePos &&
				m[1]-m[0] < minSparseSize {

				if buf[m[1]] != 0 {
					if buf[m[1]+minSparseSize] != 0 {
						m[1] += minSparseSize
					}
					m[0] = m[1] + 1
				}
				m[1]++
			}

			if m[1]-m[0] >= minSparseSize {
				// we found something - go forward as much as we can
				for m[1] < postBufIdx && buf[m[1]] == 0 {
					m[1]++
				}
				m[0] -= curIdx
				m[1] -= curIdx
			} else {
				m[1] = 0
			}
		}

		// if m := c.re2.FindIndex(buf[curIdx:]); m != nil && m[1] > 0 {
		if m[1] > 0 {
			// We did match *somewhere* in the buffer - let's break this down
			// NOTE: the m[{0,1}] offsets are relative to curIdx as it was at loop start
			matchOverflow = (curIdx+m[1] >= postBufIdx)

			// There is some leading non-pad data: pass it down the chain
			if m[0] != 0 {
				if err = cb(chunker.Chunk{
					Size: m[0],
				}); err != nil {
					return
				}
				curIdx += m[0]
			}

			padLen = m[1] - m[0]

			// The pad doesn't fit in a maxchunk: let's keep chopping off "full sparse chunks"
			for padLen >= c.maxChunkSize {
				if err = cb(chunker.Chunk{
					Size: c.maxChunkSize,
					Meta: sparseMeta,
				}); err != nil {
					return
				}
				padLen -= c.maxChunkSize
				curIdx += c.maxChunkSize
			}

			// Ok, there is more pad left and it is NOT "trailing off"
			// Send that too and then we loop and match again
			if !matchOverflow && padLen > 0 {
				if err = cb(chunker.Chunk{
					Size: padLen,
					Meta: sparseMeta,
				}); err != nil {
					return
				}
				curIdx += padLen
			}

		} else {
			// No match at all: no callback, just return right now so that the
			// underlying chunker has a chance to work the region on its own
			// Then we will come back here anew
			return
		}
	}
}

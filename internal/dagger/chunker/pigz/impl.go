package pigz

import (
	"github.com/ipfs-shipyard/DAGger/chunker"
)

type config struct {
	TargetValue int32 `getopt:"--state-target    State value denoting a chunk boundary"`
	MaskBits    int   `getopt:"--state-mask-bits Amount of bits of state to compare to target on every iteration. For random input average chunk size is about 2**m"`
	MaxSize     int   `getopt:"--max-size        Maximum data chunk size"`
	MinSize     int   `getopt:"--min-size        Minimum data chunk size"`
}

type pigzChunker struct {
	// derived from the tables at the end of the file, selectable via --hash-table
	mask           int32
	minSansPreheat int
	config
}

func (c *pigzChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb chunker.SplitResultCallback,
) (err error) {

	var state int32
	var curIdx, lastIdx, nextRoundMax int
	postBufIdx := len(buf)

	for {
		lastIdx = curIdx
		nextRoundMax = lastIdx + c.MaxSize

		// we will be running out of data, but still *could* run a round
		if nextRoundMax > postBufIdx {
			// abort early if we are allowed to
			if !useEntireBuffer {
				return
			}
			// otherwise signify where we stop hard
			nextRoundMax = postBufIdx
		}

		// in case we will *NOT* be able to run another round at all
		if curIdx+c.MinSize >= postBufIdx {
			if useEntireBuffer && postBufIdx != curIdx {
				err = cb(chunker.Chunk{Size: postBufIdx - curIdx})
			}
			return
		}

		// preheat
		curIdx += c.minSansPreheat
		for i := 0; i < c.MaskBits; i++ {
			state = (state << 1) ^ int32(buf[curIdx])
			curIdx++
		}

		// cycle
		for curIdx < nextRoundMax && ((state & c.mask) != c.TargetValue) {
			state = (state << 1) ^ int32(buf[curIdx])
			curIdx++
		}

		// always a find at this point, we bailed on short buffers earlier
		err = cb(chunker.Chunk{Size: curIdx - lastIdx})
		if err != nil {
			return
		}
	}
}

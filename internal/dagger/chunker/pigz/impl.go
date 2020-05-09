package pigz

import (
	"github.com/ipfs-shipyard/DAGger/chunker"
)

type config struct {
	TargetValue uint32 `getopt:"--state-target    State value denoting a chunk boundary"`
	MaskBits    int    `getopt:"--state-mask-bits Amount of bits of state to compare to target on every iteration. For random input average chunk size is about 2**m"`
	MinSize     int    `getopt:"--min-size        Minimum data chunk size"`
}

type pigzChunker struct {
	// derived from the tables at the end of the file, selectable via --hash-table
	mask           uint32
	maxGlobalSize  int
	minSansPreheat int
	config
}

func (c *pigzChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb chunker.SplitResultCallback,
) (err error) {

	var state uint32
	var curIdx, lastIdx, nextRoundMax int
	postBufIdx := len(buf)

	for {
		lastIdx = curIdx
		nextRoundMax = lastIdx + c.maxGlobalSize

		// we will be running out of data, but still *could* run a round
		if nextRoundMax >= postBufIdx {
			// abort early if we are allowed to
			if !useEntireBuffer {
				return
			}
			// otherwise signify where we stop hard
			nextRoundMax = postBufIdx - 1
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
			curIdx++
			state = ((state << 1) ^ uint32(buf[curIdx])) & c.mask
		}

		// cycle
		for curIdx < nextRoundMax && state != c.TargetValue {
			curIdx++
			state = ((state << 1) ^ uint32(buf[curIdx])) & c.mask
		}

		// awlays a find at this point, we bailed on short buffers earlier
		err = cb(chunker.Chunk{Size: curIdx - lastIdx})
		if err != nil {
			return
		}
	}
}

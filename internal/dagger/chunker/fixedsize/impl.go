package fixedsize

import (
	"github.com/ipfs-shipyard/DAGger/chunker"
)

type fixedSizeChunker struct {
	size int
}

func (c *fixedSizeChunker) Split(
	buf []byte,
	useEntireBuffer bool,
	cb chunker.SplitResultCallback,
) (err error) {

	curIdx := c.size

	for curIdx < len(buf) {
		err = cb(chunker.Chunk{Size: c.size})
		if err != nil {
			return
		}
		curIdx += c.size
	}

	if curIdx-c.size < len(buf) && useEntireBuffer {
		err = cb(chunker.Chunk{Size: len(buf) - (curIdx - c.size)})
	}
	return
}

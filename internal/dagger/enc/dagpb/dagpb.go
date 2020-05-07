package dagpb

import (
	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/enc"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	"github.com/ipfs-shipyard/DAGger/internal/zcpstring"
)

const (
	UnixFsTypeRaw  = byte(0)
	UnixFsTypeFile = byte(2)

	pbHdrF1VI = byte(0x08)
	pbHdrF2VI = byte(0x10)
	pbHdrF3VI = byte(0x18)
	pbHdrF4VI = byte(0x20)
	pbHdrF1LD = byte(0x0A)
	pbHdrF2LD = byte(0x12)
	pbHdrF3LD = byte(0x1A)
	pbHdrF4LD = byte(0x22)
)

func UnixFSv1Leaf(ls block.LeafSource, bm block.Maker, leafUnixFsType byte) *block.Header {

	if ls.Size == 0 {
		// short-circuit for convergence with go-ipfs
		// represents the following protobuf regardless of settings
		// 1 {
		// 	1: 2
		// 	3: 0
		// }
		return bm(
			zcpstring.NewFromSlice([]byte("\x0a\x04\x08\x02\x18\x00")),
			block.CodecPB,
			0,
			0,
			0,
		)
	}

	dataLen := enc.VarintSlice(uint64(ls.Size))

	blockData := zcpstring.NewWithSegmentCap(9)
	blockData.AddByte(pbHdrF1LD)
	blockData.AddSlice(enc.VarintSlice(uint64(3 + 2*len(dataLen) + ls.Size + 1)))
	blockData.AddByte(pbHdrF1VI)
	blockData.AddByte(leafUnixFsType)
	blockData.AddByte(pbHdrF2LD)
	blockData.AddSlice(dataLen)
	blockData.AddZcp(ls.Content)
	blockData.AddByte(pbHdrF3VI)
	blockData.AddSlice(dataLen)

	return bm(
		blockData,
		block.CodecPB,
		uint64(ls.Size),
		0,
		0,
	)
}

func UnixFSv1LinkNode(
	blocks []*block.Header,
	bm block.Maker,
	legacyCIDv0Links bool,
	omitTsizeAndOffsets bool,
) *block.Header {

	var totalPayload, subDagSize uint64
	var linkBlock, seekOffsets *zcpstring.ZcpString
	if omitTsizeAndOffsets {
		seekOffsets = &zcpstring.ZcpString{}
		linkBlock = zcpstring.NewWithSegmentCap(
			(5 * len(blocks)) + 6,
		)
	} else {
		seekOffsets = zcpstring.NewWithSegmentCap(2 * len(blocks))
		linkBlock = zcpstring.NewWithSegmentCap(
			(9 * len(blocks)) + 6 + 2*len(blocks),
		)
	}

	for i := range blocks {

		cid := blocks[i].Cid()
		if legacyCIDv0Links &&
			!blocks[i].IsInlined() &&
			blocks[i].SizeCumulativePayload() != blocks[i].SizeCumulativeDag() { // size inequality is a hack to quickly distinguish raw leaf blocks from everything else
			cid = cid[2:]
		}

		cidLenVI := enc.VarintSlice(uint64(len(cid)))
		var dagSizeVI []byte
		var frameLen uint64

		if omitTsizeAndOffsets {
			frameLen = uint64(1 + len(cidLenVI) + len(cid))
		} else {
			dagSizeVI = enc.VarintSlice(blocks[i].SizeCumulativeDag())
			frameLen = uint64(1 + len(cidLenVI) + len(cid) + 3 + len(dagSizeVI))
		}

		linkBlock.AddByte(pbHdrF2LD)
		linkBlock.AddSlice(enc.VarintSlice(frameLen))

		linkBlock.AddByte(pbHdrF1LD)
		linkBlock.AddSlice(cidLenVI)
		linkBlock.AddSlice(cid)

		if !omitTsizeAndOffsets {
			// yes, a zero-length piece needed here for convergence :(((
			linkBlock.AddByte(pbHdrF2LD)
			linkBlock.AddByte(0)

			linkBlock.AddByte(pbHdrF3VI)
			linkBlock.AddSlice(dagSizeVI)

			seekOffsets.AddByte(pbHdrF4VI)
			seekOffsets.AddSlice(enc.VarintSlice(blocks[i].SizeCumulativePayload()))
		}

		if linkBlock.Size() >= int(constants.HardMaxBlockSize) {
			util.InternalPanicf(
				"accumulated linked block size %s exceeded the hard maximum block size %s",
				util.Commify(linkBlock.Size()),
				util.Commify(int(constants.HardMaxBlockSize)),
			)
		}
		totalPayload += blocks[i].SizeCumulativePayload()
		subDagSize += blocks[i].SizeCumulativeDag()
	}

	// measure before we append the data part
	linkSectionSize := linkBlock.Size()

	payloadSizeVI := enc.VarintSlice(totalPayload)

	linkBlock.AddByte(pbHdrF1LD)
	linkBlock.AddSlice(enc.VarintSlice(uint64(3 + len(payloadSizeVI) + seekOffsets.Size())))

	linkBlock.AddByte(pbHdrF1VI)
	linkBlock.AddByte(2)
	linkBlock.AddByte(pbHdrF3VI)
	linkBlock.AddSlice(payloadSizeVI)
	linkBlock.AddZcp(seekOffsets)

	return bm(
		linkBlock,
		block.CodecPB,
		totalPayload,
		subDagSize,
		linkSectionSize,
	)
}

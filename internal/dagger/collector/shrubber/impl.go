package shrubber

import (
	"encoding/binary"
	"log"

	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

type config struct {
	MaxPayload    int `getopt:"--max-payload   FIXME Maximum payload size in each node. To skip payload-based balancing, set this to 0."`
	PadLayerNodes int `getopt:"--pad-layer-nodes  FIXME LS"`
	CidMaskBits   int `getopt:"--cid-tail-mask-bits FIXME Amount of bits from the end of a cryptographic Cid to compare of state to compare to target on every iteration. For random input average chunk size is about 2**m"`
	CidTailTarget int `getopt:"--cid-tail-target    FIXME State value denoting a chunk boundary"`
	MinSubgroup   int `getopt:"--min-subgroup  FIXME The minimum amount of nodes clustered together before employing CID-based subgrouping"`
}
type collector struct {
	cidMask int
	config
	sumPayload uint64
	padCluster struct {
		runSize   uint64
		base      string
		padBlocks []*padBlock
	}
	stack []*dgrblock.Header
	*dgrcollector.DaggerConfig
}
type padBlock struct {
	repeat int
	block  *dgrblock.Header
}

func (co *collector) FlushState() *dgrblock.Header {
	co.flushPadding()

	if len(co.stack) == 0 {
		return nil
	}

	var tailHdr *dgrblock.Header
	if len(co.stack) == 1 {
		tailHdr = co.stack[0]
	} else {
		tailHdr = co.NodeEncoder.NewLink(
			dgrencoder.NodeOrigin{OriginatingLayer: co.ChainPosition},
			co.stack,
		)
	}
	co.NextCollector.AppendBlock(tailHdr)

	// we flush often, do not realloc
	co.stack = co.stack[:0]
	co.sumPayload = 0

	return nil // we are never last: do not return the intermediate block
}

func (co *collector) AppendData(ds dgrblock.DataSource) *dgrblock.Header {

	curBase, curBaseFound := ds.Meta["padding-cluster-atom-hex"].(string)

	if !curBaseFound {
		hdr := co.NodeEncoder.NewLeaf(ds)
		co.AppendBlock(hdr)
		return hdr
	}

	if co.padCluster.base != curBase ||
		(co.MaxPayload > 0 &&
			co.padCluster.runSize+uint64(ds.Size) > uint64(co.MaxPayload)) {
		co.flushPadding()

		hdr := co.NodeEncoder.NewLeaf(ds)
		co.padCluster.base = curBase
		co.padCluster.runSize = hdr.SizeCumulativePayload()
		co.padCluster.padBlocks = []*padBlock{&padBlock{
			block:  hdr,
			repeat: 1,
		}}

		return hdr
	}

	co.padCluster.runSize += uint64(ds.Size)
	lastBlock := co.padCluster.padBlocks[len(co.padCluster.padBlocks)-1]

	if lastBlock.block.SizeCumulativePayload() == uint64(ds.Size) {
		lastBlock.repeat++
		return lastBlock.block
	}

	hdr := co.NodeEncoder.NewLeaf(ds)
	co.padCluster.padBlocks = append(co.padCluster.padBlocks, &padBlock{
		block:  hdr,
		repeat: 1,
	})
	return hdr
}

func (co *collector) AppendBlock(newHdr *dgrblock.Header) {
	// the *last* thing we do is append the block we just got,
	// after performing various flushes
	defer func() {
		co.stack = append(co.stack, newHdr)
		co.sumPayload += newHdr.SizeCumulativePayload()
	}()

	// assemble anything there is in the padding stack
	co.flushPadding()

	if len(co.stack) > 0 &&
		co.stack[len(co.stack)-1].IsCidInlined() != newHdr.IsCidInlined() {
		co.FlushState()
		return
	}

	if co.MaxPayload > 0 {
		if newHdr.SizeCumulativePayload() > uint64(co.MaxPayload) {
			log.Panicf(
				"block %s representing %s bytes of payload appended at sub-balancing layer with activated max-payload limit of %s",
				newHdr.String(),
				util.Commify64(int64(newHdr.SizeCumulativePayload())),
				util.Commify(co.MaxPayload),
			)
		}

		if co.sumPayload+newHdr.SizeCumulativePayload() > uint64(co.MaxPayload) {
			co.FlushState()
			return
		}
	}

	if len(co.stack) > co.MinSubgroup+1 {

		tgtIdx := len(co.stack) - 1
		tgtBlock := co.stack[tgtIdx]
		tgtCid := tgtBlock.Cid()

		if (int(binary.BigEndian.Uint16(tgtCid[len(tgtCid)-2:])) & co.cidMask) == co.CidTailTarget {

			// fmt.Printf("\t\tMatch %s\n", tgtBlock.CidBase32())
			linkHdr := co.NodeEncoder.NewLink(
				dgrencoder.NodeOrigin{OriginatingLayer: co.ChainPosition},
				co.stack[0:tgtIdx+1],
			)
			co.NextCollector.AppendBlock(linkHdr)

			// fmt.Printf("%d\ttail of %s\n", 0, co.stack[tgtIdx].CidBase32())
			co.sumPayload -= linkHdr.SizeCumulativePayload()

			// shift everything to the last cut, without realloc
			co.stack = co.stack[:copy(
				co.stack,
				co.stack[tgtIdx+1:],
			)]
			return
		}
	}
}

func (co *collector) flushPadding() {
	if len(co.padCluster.padBlocks) == 0 {
		return
	}

	// start with a state-reset: allows recursive calls to AppendBlock without inf-loop
	var pbs []*padBlock
	pbs, co.padCluster.padBlocks = co.padCluster.padBlocks, co.padCluster.padBlocks[:0]
	co.padCluster.base = ""

	if len(pbs) == 1 && pbs[0].repeat == 1 {
		co.AppendBlock(pbs[0].block)
		return
	}

	finBlocks := make([]*dgrblock.Header, 0, len(pbs)*co.PadLayerNodes)

	expBlocks := make([]*dgrblock.Header, 0, 7)
	expNext := make([]*dgrblock.Header, 0, co.PadLayerNodes)

	for pi := 0; pi < len(pbs); pi++ {
		if pbs[pi].repeat == 1 {
			finBlocks = append(finBlocks, pbs[pi].block)
			continue
		}

		count := pbs[pi].repeat

		// list of individual building blocks, growing from smallest to largest
		expBlocks = expBlocks[:0]
		expBlocks = append(expBlocks, pbs[pi].block)

		for {
			curLevelCount := 1
			// don't drag in float64/Pow() just for this
			for i := len(expBlocks); i > 1; i-- {
				curLevelCount *= co.PadLayerNodes
			}

			// keep proceeding to next-level expBlocks as long as we can use at least 2 of them
			// ( using a highest level exp-block only once does not gain anything )
			if count >= (2 * curLevelCount * co.PadLayerNodes) {

				// use current exp-block as many times as remains from next-level
				for i := (count % (curLevelCount * co.PadLayerNodes)) / curLevelCount; i > 0; i-- {
					finBlocks = append(finBlocks, expBlocks[len(expBlocks)-1])
				}

				// assemble next exp-block
				expNext = expNext[:0]
				for i := co.PadLayerNodes; i > 0; i-- {
					expNext = append(expNext, expBlocks[len(expBlocks)-1])
				}
				expBlocks = append(expBlocks, co.NodeEncoder.NewLink(
					dgrencoder.NodeOrigin{OriginatingLayer: -1, LocalSubLayer: 2},
					expNext,
				))
			} else {
				// we are done - use current superblock as many times as needed and stop
				for i := count / curLevelCount; i > 0; i-- {
					finBlocks = append(finBlocks, expBlocks[len(expBlocks)-1])
				}
				break
			}
		}
	}

	co.AppendBlock(co.NodeEncoder.NewLink(
		dgrencoder.NodeOrigin{OriginatingLayer: -1, LocalSubLayer: 2},
		finBlocks,
	))
}

/*

	for left, right := 0, len(finBlocks)-1; left < right; left, right = left+1, right-1 {
		finBlocks[left], finBlocks[right] = finBlocks[right], finBlocks[left]
	}

}
*/

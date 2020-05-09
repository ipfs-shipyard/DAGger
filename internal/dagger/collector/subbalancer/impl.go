package subbalancer

import (
	"encoding/binary"
	"log"

	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

type config struct {
	MaxPayload          int `getopt:"--max-payload   Maximum payload size in each node. To skip payload-based balancing, set this to 0."`
	MinSurroundSiblings int `getopt:"--min-surrounding-siblings The minimum amount of siblings before and after a node, for its CID to be matched "`
}
type state struct {
	sumPayload int
	stack      []*dgrblock.Header
}
type collector struct {
	config
	*dgrcollector.DaggerConfig
	state
}

func (co *collector) FlushState() *dgrblock.Header {
	if len(co.stack) == 0 {
		return nil
	}

	// it is critical to reset the collector state when we are done - we reuse the object!
	defer func() { co.state = state{} }()

	var tailHdr *dgrblock.Header
	if len(co.stack) == 1 {
		tailHdr = co.stack[0]
	} else {
		tailHdr = co.NodeEncoder.NewLink(
			dgrencoder.NodeOrigin{OriginatorIndex: co.IndexInChain},
			co.stack,
		)
	}
	co.NextCollector.AppendBlock(tailHdr)

	return nil // we are never last: do not return the intermediate block
}

func (co *collector) AppendLeaf(ls dgrblock.LeafSource) (hdr *dgrblock.Header) {
	hdr = co.NodeEncoder.NewLeaf(ls)
	co.AppendBlock(hdr)
	return
}

func (co *collector) AppendBlock(newHdr *dgrblock.Header) {
	// the *last* thing we do is append the block we just got
	defer func() {
		co.stack = append(co.stack, newHdr)
		co.sumPayload += int(newHdr.SizeCumulativePayload())
	}()

	if len(co.stack) > 0 &&
		co.stack[len(co.stack)-1].IsCidInlined() != newHdr.IsCidInlined() {
		co.FlushState()
		return
	}

	if co.MaxPayload > 0 && newHdr.SizeCumulativePayload() > uint64(co.MaxPayload) {
		log.Panicf(
			"block %s representing %s bytes of payload appended at sub-balancing layer with activated max-payload limit of %s",
			newHdr.String(),
			util.Commify64(int64(newHdr.SizeCumulativePayload())),
			util.Commify(co.MaxPayload),
		)
	}

	if co.sumPayload+int(newHdr.SizeCumulativePayload()) > co.MaxPayload {
		// co.FlushState()

		co.NextCollector.AppendBlock(co.NodeEncoder.NewLink(
			dgrencoder.NodeOrigin{OriginatorIndex: co.IndexInChain},
			co.stack[0:len(co.stack)],
		))

		// fmt.Printf("%d\ttail of %s\n", 0, co.stack[tgtIdx].CidBase32())
		co.sumPayload = 0
		// int(co.stack[len(co.stack)].SizeCumulativePayload())
		co.stack = co.stack[len(co.stack):]
		return
	}

	if len(co.stack) >= co.MinSurroundSiblings*2 {
		tgtIdx := len(co.stack) - co.MinSurroundSiblings + 1
		tgtCid := co.stack[tgtIdx].Cid()

		tailVal := binary.BigEndian.Uint16(tgtCid[len(tgtCid)-2:])
		if tailVal&((1<<5)-1) == 0 {

			linkHdr := co.NodeEncoder.NewLink(
				dgrencoder.NodeOrigin{OriginatorIndex: co.IndexInChain},
				co.stack[0:tgtIdx+1],
			)
			co.NextCollector.AppendBlock(linkHdr)

			// fmt.Printf("%d\ttail of %s\n", 0, co.stack[tgtIdx].CidBase32())
			co.sumPayload -= int(linkHdr.SizeCumulativePayload())
			co.stack = co.stack[tgtIdx+1:]
		}
	}
}

package dgrblock

import (
	"encoding/base32"
	"fmt"
	"hash"
	"log"
	"math"
	"sync/atomic"

	blake2b "github.com/minio/blake2b-simd"
	sha256 "github.com/minio/sha256-simd"
	"github.com/twmb/murmur3"
	"golang.org/x/crypto/sha3"

	"github.com/ipfs-shipyard/DAGger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	"github.com/ipfs-shipyard/DAGger/internal/zcpstring"
)

// multihash ids come from https://github.com/multiformats/multicodec/blob/master/table.csv
var AvailableHashers = map[string]hasher{
	"none": {
		hasherMaker: nil,
		unsafe:      true,
	},
	"sha2-256": {
		multihashID: 0x12,
		hasherMaker: sha256.New,
	},
	"sha3-512": {
		multihashID: 0x14,
		hasherMaker: sha3.New512,
	},
	"blake2b-256": {
		multihashID: 0xb220,
		hasherMaker: blake2b.New256,
	},
	"murmur3-128": {
		multihashID: 0x22,
		hasherMaker: func() hash.Hash { return murmur3.New128() },
		unsafe:      true,
	},
}

type hasher struct {
	hasherMaker func() hash.Hash
	multihashID uint
	unsafe      bool // do not allow use in car emitters
}

const (
	CodecRaw uint = 0x55
	CodecPB  uint = 0x70

	NulRootCarHeader = "\x19" + // 25 bytes of CBOR (encoded as varint :cryingbear: )
		// map with 2 keys
		"\xA2" +
		// text-key with length 5
		"\x65" +
		// "roots"
		"\x72\x6F\x6F\x74\x73" +
		// 1 element array
		"\x81" +
		// tag 42
		"\xD8\x2A" +
		// bytes with length 5
		"\x45" +
		// nul-identity-cid prefixed with \x00 as required in DAG-CBOR: https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md#links
		"\x00\x01\x55\x00\x00" +
		// text-key with length 7
		"\x67" +
		// "version"
		"\x76\x65\x72\x73\x69\x6F\x6E" +
		// 1, we call this v0 due to the nul-identity CID being an open question: https://github.com/ipld/go-car/issues/26#issuecomment-604299576
		"\x01"

	// encodes as CID of "baaaaaanohasha"...
	dummyPrefix = "\x00\x00\x00\x01\xAE\x38\x24\x70"
)

type Header struct {
	// Everything in this struct needs to be "cacheable"
	// That is no data that changes without a panic can be present
	// ( e.g. stuff like "how many times was block seen in dag" is out )
	dummyHashed  bool
	isCidInlined bool
	sizeBlock    int
	// sizeCidRefs      int
	totalSizePayload uint64
	totalSizeDag     uint64
	cid              []byte
	cidReady         chan struct{}
	contentGone      *int32
	content          *zcpstring.ZcpString
}

func (h *Header) Content() (c *zcpstring.ZcpString) {
	// read first, check second
	c = h.content
	if constants.PerformSanityChecks &&
		atomic.LoadInt32(h.contentGone) != 0 {
		log.Panic("block content no longer available")
	}
	return
}
func (h *Header) EvictContent() {
	if constants.PerformSanityChecks {
		atomic.AddInt32(h.contentGone, 1)
	}
	h.content = nil
}

func (h *Header) Cid() []byte {
	<-h.cidReady

	if constants.PerformSanityChecks && !h.dummyHashed &&
		(h.cid[0] != byte(1) ||
			(len(h.cid) < 4) ||
			(h.cid[2] != byte(0) && len(h.cid) < 4+(128/8))) {
		log.Panicf(
			"block header with a seemingly invalid CID '%x' encountered",
			h.cid,
		)
	}

	return h.cid
}
func (h *Header) SizeBlock() int { return h.sizeBlock }

// func (h *Header) SizeLinkSection() int          { return h.sizeLinkSection }
func (h *Header) IsCidInlined() bool            { return h.isCidInlined }
func (h *Header) DummyHashed() bool             { return h.dummyHashed }
func (h *Header) SizeCumulativeDag() uint64     { return h.totalSizeDag }
func (h *Header) SizeCumulativePayload() uint64 { return h.totalSizePayload }

type Maker func(
	blockContent *zcpstring.ZcpString,
	codecID uint,
	sizePayload uint64,
	sizeSubDag uint64,
) *Header

type DataSource struct {
	chunker.Chunk // critically *NOT* a reference, so that an empty DataSource{} is usable on its own
	Content       *zcpstring.ZcpString
}

var b32Encoder *base32.Encoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func (h *Header) CidBase32() string {
	if h == nil {
		return "N/A"
	}

	return "b" + b32Encoder.EncodeToString(h.Cid())
}

func (h *Header) String() string {
	if h.isCidInlined {
		return fmt.Sprintf(
			"Identity-embedded %d bytes: %s",
			h.sizeBlock,
			h.CidBase32(),
		)
	}

	return h.CidBase32()
}

type hashTask struct {
	hashBasedCidLen int
	hdr             *Header
}
type AsyncHashingBus chan<- hashTask

func MakerFromConfig(
	hashAlg string,
	cidHashSize int,
	inlineMaxSize int,
	maxAsyncHashers int,
) (maker Maker, asyncHashQueue chan hashTask, errString string) {

	hashopts, found := AvailableHashers[hashAlg]
	if !found {
		errString = fmt.Sprintf(
			"invalid hash function '%s'. Available hash names are %s",
			hashAlg,
			util.AvailableMapKeys(AvailableHashers),
		)
		return
	}

	var nativeHashSize int
	if hashopts.hasherMaker == nil {
		nativeHashSize = math.MaxInt32
	} else {
		nativeHashSize = hashopts.hasherMaker().Size()
	}

	if nativeHashSize < cidHashSize {
		errString = fmt.Sprintf(
			"selected hash function '%s' does not produce a digest satisfying the requested amount of --hash-bits '%d'",
			hashAlg,
			cidHashSize*8,
		)
		return
	}

	if maxAsyncHashers < 0 {
		errString = fmt.Sprintf(
			"invalid negative value '%d' for maxAsyncHashers",
			maxAsyncHashers,
		)
		return
	}

	type codecMeta struct {
		hashedCidLength   int
		hashedCidPrefix   []byte
		identityCidPrefix []byte
		dummyCid          []byte
	}
	// if we need to support codec ids over 127 - this will have to be switched to a map
	var codecs [128]codecMeta

	// Makes code easier to follow - in most conditionals below the CID
	// is "ready" instantly/synchronously. It is only at the very last
	// case that we spawn an actual goroutine: then we make a *new* channel
	cidPreMadeChan := make(chan struct{})
	close(cidPreMadeChan)

	var hasherSingleton hash.Hash
	if hashopts.hasherMaker != nil {

		if maxAsyncHashers == 0 {
			hasherSingleton = hashopts.hasherMaker()

		} else {
			asyncHashQueue = make(chan hashTask, 8*maxAsyncHashers) // SANCHECK queue up to 8 times the available workers

			for i := 0; i < maxAsyncHashers; i++ {
				go func() {
					hasher := hashopts.hasherMaker()
					for {
						task, chanOpen := <-asyncHashQueue
						if !chanOpen {
							return
						}
						hasher.Reset()
						task.hdr.Content().WriteTo(hasher)
						task.hdr.cid = (hasher.Sum(task.hdr.cid))[0:task.hashBasedCidLen:task.hashBasedCidLen]
						close(task.hdr.cidReady)
					}
				}()
			}
		}
	}

	maker = func(
		blockContent *zcpstring.ZcpString,
		codecID uint,
		sizeSubPayload uint64,
		sizeSubDag uint64,
		// sizeLinkSection int,
	) *Header {

		if blockContent == nil {
			blockContent = &zcpstring.ZcpString{}
		}

		if constants.PerformSanityChecks && blockContent.Size() > constants.MaxBlockWireSize {
			log.Panicf(
				"size of supplied block %s exceeds the hard maximum block size %s",
				util.Commify(blockContent.Size()),
				util.Commify(constants.MaxBlockWireSize),
			)
		}

		if constants.PerformSanityChecks && codecID > 127 {
			log.Panicf(
				"codec IDs larger than 127 are not supported, however %d was supplied",
				codecID,
			)
		} else if codecs[codecID].hashedCidLength == 0 {
			// we will do this only once per runtime per codec
			// inefficiency is a-ok

			codecs[codecID].identityCidPrefix = append(codecs[codecID].identityCidPrefix, byte(1))
			codecs[codecID].identityCidPrefix = util.AppendVarint(codecs[codecID].identityCidPrefix, uint64(codecID))
			codecs[codecID].identityCidPrefix = append(codecs[codecID].identityCidPrefix, byte(0))

			codecs[codecID].hashedCidPrefix = append(codecs[codecID].hashedCidPrefix, byte(1))
			codecs[codecID].hashedCidPrefix = util.AppendVarint(codecs[codecID].hashedCidPrefix, uint64(codecID))
			codecs[codecID].hashedCidPrefix = util.AppendVarint(codecs[codecID].hashedCidPrefix, uint64(hashopts.multihashID))
			codecs[codecID].hashedCidPrefix = util.AppendVarint(codecs[codecID].hashedCidPrefix, uint64(cidHashSize))

			codecs[codecID].hashedCidLength = len(codecs[codecID].hashedCidPrefix) + cidHashSize

			// this is what we assign in case the nul hasher is selected
			codecs[codecID].dummyCid = make(
				[]byte,
				codecs[codecID].hashedCidLength,
			)
			copy(codecs[codecID].dummyCid, dummyPrefix)
		}

		hdr := &Header{
			content:          blockContent,
			contentGone:      new(int32),
			cidReady:         cidPreMadeChan,
			sizeBlock:        blockContent.Size(),
			totalSizeDag:     sizeSubDag + uint64(blockContent.Size()),
			totalSizePayload: sizeSubPayload, // at present there is no payload in link-nodes
			// sizeLinkSection:  sizeLinkSection,
		}

		if inlineMaxSize > 0 &&
			inlineMaxSize >= hdr.sizeBlock {

			hdr.isCidInlined = true

			hdr.cid = append(
				make(
					[]byte,
					0,
					(len(codecs[codecID].identityCidPrefix)+
						util.VarintWireSize(uint64(hdr.sizeBlock))+
						blockContent.Size()),
				),
				codecs[codecID].identityCidPrefix...,
			)
			hdr.cid = util.AppendVarint(hdr.cid, uint64(hdr.sizeBlock))
			hdr.cid = blockContent.AppendTo(hdr.cid)

		} else if hashopts.hasherMaker == nil {
			hdr.dummyHashed = true
			hdr.cid = codecs[codecID].dummyCid

		} else {
			hdr.cid = append(
				make(
					[]byte,
					0,
					(len(codecs[codecID].hashedCidPrefix)+nativeHashSize),
				),
				codecs[codecID].hashedCidPrefix...,
			)

			finLen := codecs[codecID].hashedCidLength

			if asyncHashQueue == nil {
				hasherSingleton.Reset()
				blockContent.WriteTo(hasherSingleton)
				hdr.cid = (hasherSingleton.Sum(hdr.cid))[0:finLen:finLen]
			} else {
				hdr.cidReady = make(chan struct{})
				asyncHashQueue <- hashTask{
					hashBasedCidLen: finLen,
					hdr:             hdr,
				}
			}
		}

		return hdr
	}

	return
}

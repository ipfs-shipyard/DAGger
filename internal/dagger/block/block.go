package block

import (
	"encoding/base32"
	"fmt"
	"hash"
	"sync/atomic"

	blake2b "github.com/minio/blake2b-simd"
	sha256 "github.com/minio/sha256-simd"
	"github.com/twmb/murmur3"
	"golang.org/x/crypto/sha3"

	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/enc"
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

	// encodes as CID of "baaaaaanohasha"...
	dummyPrefix string = "\x00\x00\x00\x01\xAE\x38\x24\x70"
)

type Header struct {
	// Everything in this struct needs to be "cacheable"
	// That is no data that changes without a panic can be present
	// ( e.g. stuff like "how many times was block seen in dag" is out )
	sizeBlock        int
	sizeLinkSection  int
	isInlined        bool
	dummyHashed      bool
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
		util.InternalPanicf("block content no longer available")
	}
	return
}
func (h *Header) EvictContent() {
	if constants.PerformSanityChecks &&
		atomic.AddInt32(h.contentGone, 1) != 1 {
		util.InternalPanicf("block content no longer available")
	}
	h.content = nil
}

func (h *Header) Cid() []byte {
	<-h.cidReady

	if constants.PerformSanityChecks && !h.dummyHashed &&
		(h.cid[0] != byte(1) ||
			(len(h.cid) < 4) ||
			(h.cid[2] != byte(0) && len(h.cid) < 4+(128/8))) {
		util.InternalPanicf(
			"block header with a seemingly invalid CID '%x' encountered",
			h.cid,
		)
	}

	return h.cid
}
func (h *Header) SizeBlock() int                { return h.sizeBlock }
func (h *Header) SizeLinkSection() int          { return h.sizeLinkSection }
func (h *Header) IsInlined() bool               { return h.isInlined }
func (h *Header) DummyHashed() bool             { return h.dummyHashed }
func (h *Header) SizeCumulativeDag() uint64     { return h.totalSizeDag }
func (h *Header) SizeCumulativePayload() uint64 { return h.totalSizePayload }
func (h *Header) IsSparse() bool                { return false } // FIXME

type Maker func(
	blockContent *zcpstring.ZcpString,
	codecID uint,
	sizePayload uint64,
	sizeSubDag uint64,
	sizeLinkSection int,
) *Header

type DataSource struct {
	chunker.Chunk
	Content *zcpstring.ZcpString
}

func RawDataLeaf(ds DataSource, bm Maker) *Header {
	return bm(
		ds.Content,
		CodecRaw,
		uint64(ds.Size),
		0,
		0,
	)
}

var b32Encoder *base32.Encoding = base32.NewEncoding("abcdefghijklmnopqrstuvwxyz234567").WithPadding(base32.NoPadding)

func (h *Header) CidBase32() string {
	if h == nil {
		return "N/A"
	}

	return "b" + b32Encoder.EncodeToString(h.Cid())
}

func (h *Header) String() string {
	if h.isInlined {
		return fmt.Sprintf(
			"Identity-embedded %d bytes: %s",
			h.sizeBlock,
			h.CidBase32(),
		)
	}

	return h.CidBase32()
}

func MakerFromConfig(
	hashAlg string,
	cidHashSize int,
	inlineMaxSize int,
	maxAsyncHashers int,
) (maker Maker, errString string) {
	var nativeHashSize int

	hashopts, found := AvailableHashers[hashAlg]
	if !found {
		return nil, fmt.Sprintf(
			"invalid hashing algorithm '%s'. Available hash algorithms are %s",
			hashAlg,
			util.AvailableMapKeys(AvailableHashers),
		)
	}

	if hashopts.hasherMaker == nil {
		nativeHashSize = 1<<31 - 1
	} else {
		nativeHashSize = hashopts.hasherMaker().Size()
	}

	if nativeHashSize < cidHashSize {
		return nil, fmt.Sprintf(
			"selected hash algorithm '%s' does not produce a digest satisfying the requested hash bits '%d'",
			hashAlg,
			cidHashSize*8,
		)
	}

	if maxAsyncHashers < 0 {
		return nil, fmt.Sprintf(
			"invalid negative value '%d' for maxAsyncHashers",
			maxAsyncHashers,
		)
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

	var asyncRateLimiter chan struct{}
	if maxAsyncHashers > 0 {
		asyncRateLimiter = make(chan struct{}, maxAsyncHashers)
	}

	maker = func(
		blockContent *zcpstring.ZcpString,
		codecID uint,
		sizeSubPayload uint64,
		sizeSubDag uint64,
		sizeLinkSection int,
	) *Header {

		if blockContent == nil {
			blockContent = &zcpstring.ZcpString{}
		}

		if constants.PerformSanityChecks && blockContent.Size() > int(constants.HardMaxBlockSize) {
			util.InternalPanicf(
				"size of supplied block (%s) exceeds the hard maximum block size (%s)",
				util.Commify(blockContent.Size()),
				util.Commify64(int64(constants.HardMaxBlockSize)),
			)
		}

		if constants.PerformSanityChecks && codecID > 127 {
			util.InternalPanicf(
				"codec IDs larger than 127 are not supported, however %d was supplied",
				codecID,
			)
		} else if codecs[codecID].hashedCidLength == 0 {
			// we will do this only once per runtime per codec
			// inefficiency is a-ok

			codecs[codecID].identityCidPrefix = append(codecs[codecID].identityCidPrefix, byte(1))
			codecs[codecID].identityCidPrefix = enc.AppendVarint(codecs[codecID].identityCidPrefix, uint64(codecID))
			codecs[codecID].identityCidPrefix = append(codecs[codecID].identityCidPrefix, byte(0))

			codecs[codecID].hashedCidPrefix = append(codecs[codecID].hashedCidPrefix, byte(1))
			codecs[codecID].hashedCidPrefix = enc.AppendVarint(codecs[codecID].hashedCidPrefix, uint64(codecID))
			codecs[codecID].hashedCidPrefix = enc.AppendVarint(codecs[codecID].hashedCidPrefix, uint64(hashopts.multihashID))
			codecs[codecID].hashedCidPrefix = enc.AppendVarint(codecs[codecID].hashedCidPrefix, uint64(cidHashSize))

			codecs[codecID].hashedCidLength = len(codecs[codecID].hashedCidPrefix) + cidHashSize

			// this is what we assign in case the null hasher is selected
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
			totalSizePayload: sizeSubPayload, // at present there is no payload in links
			sizeLinkSection:  sizeLinkSection,
		}

		if inlineMaxSize > 0 &&
			inlineMaxSize >= hdr.sizeBlock {

			hdr.isInlined = true

			hdr.cid = append(
				make(
					[]byte,
					0,
					(len(codecs[codecID].identityCidPrefix)+enc.VarintMaxWireBytes+blockContent.Size()),
				),
				codecs[codecID].identityCidPrefix...,
			)
			hdr.cid = enc.AppendVarint(hdr.cid, uint64(hdr.sizeBlock))
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

			hasher := hashopts.hasherMaker()

			if maxAsyncHashers == 0 {
				blockContent.WriteTo(hasher)
				hdr.cid = (hasher.Sum(hdr.cid))[:codecs[codecID].hashedCidLength]
			} else {
				hdr.cidReady = make(chan struct{})
				asyncRateLimiter <- struct{}{}
				go func() {
					hdr.Content().WriteTo(hasher)
					hdr.cid = (hasher.Sum(hdr.cid))[:codecs[codecID].hashedCidLength]
					<-asyncRateLimiter
					close(hdr.cidReady)
				}()
			}
		}

		return hdr
	}

	return maker, ""
}

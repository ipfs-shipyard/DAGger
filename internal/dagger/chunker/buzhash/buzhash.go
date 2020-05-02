package buzhash

import (
	"fmt"
	"math/bits"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

type config struct {
	TargetValue uint32 `getopt:"--state-target State value denoting a chunk boundary (IPFS default: 0)"`
	MaskBits    int    `getopt:"--state-mask-bits Amount of bits of state to compare to target on every iteration. For random input average chunk size is about 2**m (IPFS default: 17)"`
	MaxSize     int    `getopt:"--max-size Maximum data chunk size (IPFS default: 524288)"`
	MinSize     int    `getopt:"--min-size Minimum data chunk size (IPFS default: 131072)"`
	xvName      string // getopt attached dynamically below
}

type buzhashChunker struct {
	// derived from the tables at the end of the file, selectable via --hash-table
	mask           uint32
	minSansPreheat int
	xv             xorVector
	config
}

func (c *buzhashChunker) MinChunkSize() int { return c.MinSize }

func (c *buzhashChunker) Split(buf []byte, moreDataNextInvocation bool, cb func(res chunker.Chunk)) {
	var state uint32
	var curIdx, lastIdx, nextRoundMax int
	postBufIdx := len(buf)

	for {
		lastIdx = curIdx
		nextRoundMax = lastIdx + c.MaxSize

		// we will be running out of data, but still *could* run a round
		if nextRoundMax > postBufIdx {
			// go back for more data if we can
			if moreDataNextInvocation {
				return
			}
			// otherwise signify where we stop hard
			nextRoundMax = postBufIdx
		}

		// in case we will *NOT* be able to run another round at all
		if curIdx+c.MinSize >= postBufIdx {
			if !moreDataNextInvocation && postBufIdx != curIdx {
				cb(chunker.Chunk{Size: postBufIdx - curIdx})
			}
			return
		}

		// reset
		state = 0

		// preheat
		curIdx += c.minSansPreheat
		for i := 0; i < 32; i++ {
			state = bits.RotateLeft32(state, 1) ^ c.xv[buf[curIdx]]
			curIdx++
		}

		// cycle
		for curIdx < nextRoundMax && state&c.mask != c.TargetValue {
			// it seems we are skipping one rotation compared to what asuran does
			// https://gitlab.com/asuran-rs/asuran/-/blob/06206d116259821aded5ab1ee2897655b1724c69/asuran-chunker/src/buzhash.rs#L93
			state = bits.RotateLeft32(state, 1) ^ c.xv[buf[curIdx]] ^ c.xv[buf[curIdx-32]]
			curIdx++
		}

		// awlays a find at this point, we bailed on short buffers earlier
		cb(chunker.Chunk{Size: curIdx - lastIdx})
	}
}

func NewChunker(args []string, cfg *chunker.CommonConfig) (_ chunker.Chunker, initErrs []string) {

	c := buzhashChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		// A panic as this should not be possible
		cfg.InternalPanicf(
			"option set registration failed: %s",
			err,
		)
	}
	optSet.FlagLong(&c.xvName, "hash-table", 0, "The hash table to use, one of: "+util.AvailableMapKeys(hashTables))

	if args == nil {
		return nil, util.SubHelp(
			"Chunker based on hashing by cyclic polynomial, similar to the one used\n"+
				"in 'attic-backup'. As source of \"hashing\" uses a predefined table of\n"+
				"values selectable via the hash-table option.",
			optSet,
		)
	}

	// bail early if getopt fails
	if err := optSet.Getopt(args, nil); err != nil {
		return nil, []string{err.Error()}
	}

	args = optSet.Args()
	if len(args) != 0 {
		initErrs = append(initErrs, fmt.Sprintf(
			"unexpected parameter(s): %s...",
			args[0],
		))
	}

	if c.MinSize < 1 || c.MinSize > cfg.GlobalMaxChunkSize-1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value for 'min-size' in the range [1:%d] must be specified",
			cfg.GlobalMaxChunkSize-1,
		),
		)
	}
	if c.MaxSize < 1 || c.MaxSize > cfg.GlobalMaxChunkSize {
		initErrs = append(initErrs, fmt.Sprintf(
			"value for 'max-size' in the range [1:%d] must be specified",
			cfg.GlobalMaxChunkSize,
		),
		)
	}
	if c.MinSize >= c.MaxSize {
		initErrs = append(initErrs,
			"value for 'max-size' must be larger than 'min-size'",
		)
	}

	if !optSet.IsSet("state-target") {
		initErrs = append(initErrs,
			"value for the uint32 'state-target' must be specified",
		)
	}

	if c.MaskBits < 8 || c.MaskBits > 22 {
		initErrs = append(initErrs,
			"value for 'state-mask-bits' in the range [8:22] must be specified",
		)
	}
	c.mask = 1<<uint(c.MaskBits) - 1

	var exists bool
	if c.xv, exists = hashTables[c.xvName]; !exists {
		initErrs = append(initErrs, fmt.Sprintf(
			"unknown hash-table '%s' requested, available names are: %s",
			c.xvName,
			util.AvailableMapKeys(hashTables),
		))
	}

	c.minSansPreheat = c.MinSize - 32

	return &c, initErrs
}

type xorVector [256]uint32

var hashTables = map[string]xorVector{
	"GoIPFSv0": {
		0x6236e7d5, 0x10279b0b, 0x72818182, 0xdc526514, 0x2fd41e3d, 0x777ef8c8, 0x83ee5285, 0x2c8f3637,
		0x2f049c1a, 0x57df9791, 0x9207151f, 0x9b544818, 0x74eef658, 0x2028ca60, 0x0271d91a, 0x27ae587e,
		0xecf9fa5f, 0x236e71cd, 0xf43a8a2e, 0xbb13380, 0x9e57912c, 0x89a26cdb, 0x9fcf3d71, 0xa86da6f1,
		0x9c49f376, 0x346aecc7, 0xf094a9ee, 0xea99e9cb, 0xb01713c6, 0x88acffb, 0x2960a0fb, 0x344a626c,
		0x7ff22a46, 0x6d7a1aa5, 0x6a714916, 0x41d454ca, 0x8325b830, 0xb65f563, 0x447fecca, 0xf9d0ea5e,
		0xc1d9d3d4, 0xcb5ec574, 0x55aae902, 0x86edc0e7, 0xd3a9e33, 0xe70dc1e1, 0xe3c5f639, 0x9b43140a,
		0xc6490ac5, 0x5e4030fb, 0x8e976dd5, 0xa87468ea, 0xf830ef6f, 0xcc1ed5a5, 0x611f4e78, 0xddd11905,
		0xf2613904, 0x566c67b9, 0x905a5ccc, 0x7b37b3a4, 0x4b53898a, 0x6b8fd29d, 0xaad81575, 0x511be414,
		0x3cfac1e7, 0x8029a179, 0xd40efeda, 0x7380e02, 0xdc9beffd, 0x2d049082, 0x99bc7831, 0xff5002a8,
		0x21ce7646, 0x1cd049b, 0xf43994f, 0xc3c6c5a5, 0xbbda5f50, 0xec15ec7, 0x9adb19b6, 0xc1e80b9,
		0xb9b52968, 0xae162419, 0x2542b405, 0x91a42e9d, 0x6be0f668, 0x6ed7a6b9, 0xbc2777b4, 0xe162ce56,
		0x4266aad5, 0x60fdb704, 0x66f832a5, 0x9595f6ca, 0xfee83ced, 0x55228d99, 0x12bf0e28, 0x66896459,
		0x789afda, 0x282baa8, 0x2367a343, 0x591491b0, 0x2ff1a4b1, 0x410739b6, 0x9b7055a0, 0x2e0eb229,
		0x24fc8252, 0x3327d3df, 0xb0782669, 0x1c62e069, 0x7f503101, 0xf50593ae, 0xd9eb275d, 0xe00eb678,
		0x5917ccde, 0x97b9660a, 0xdd06202d, 0xed229e22, 0xa9c735bf, 0xd6316fe6, 0x6fc72e4c, 0x206dfa2,
		0xd6b15c5a, 0x69d87b49, 0x9c97745, 0x13445d61, 0x35a975aa, 0x859aa9b9, 0x65380013, 0xd1fb6391,
		0xc29255fd, 0x784a3b91, 0xb9e74c26, 0x63ce4d40, 0xc07cbe9e, 0xe6e4529e, 0xfb3632f, 0x9438d9c9,
		0x682f94a8, 0xf8fd4611, 0x257ec1ed, 0x475ce3d6, 0x60ee2db1, 0x2afab002, 0x2b9e4878, 0x86b340de,
		0x1482fdca, 0xfe41b3bf, 0xd4a412b0, 0xe09db98c, 0xc1af5d53, 0x7e55e25f, 0xd3346b38, 0xb7a12cbd,
		0x9c6827ba, 0x71f78bee, 0x8c3a0f52, 0x150491b0, 0xf26de912, 0x233e3a4e, 0xd309ebba, 0xa0a9e0ff,
		0xca2b5921, 0xeeb9893c, 0x33829e88, 0x9870cc2a, 0x23c4b9d0, 0xeba32ea3, 0xbdac4d22, 0x3bc8c44c,
		0x1e8d0397, 0xf9327735, 0x783b009f, 0xeb83742, 0x2621dc71, 0xed017d03, 0x5c760aa1, 0x5a69814b,
		0x96e3047f, 0xa93c9cde, 0x615c86f5, 0xb4322aa5, 0x4225534d, 0xd2e2de3, 0xccfccc4b, 0xbac2a57,
		0xf0a06d04, 0xbc78d737, 0xf2d1f766, 0xf5a7953c, 0xbcdfda85, 0x5213b7d5, 0xbce8a328, 0xd38f5f18,
		0xdb094244, 0xfe571253, 0x317fa7ee, 0x4a324f43, 0x3ffc39d9, 0x51b3fa8e, 0x7a4bee9f, 0x78bbc682,
		0x9f5c0350, 0x2fe286c, 0x245ab686, 0xed6bf7d7, 0xac4988a, 0x3fe010fa, 0xc65fe369, 0xa45749cb,
		0x2b84e537, 0xde9ff363, 0x20540f9a, 0xaa8c9b34, 0x5bc476b3, 0x1d574bd7, 0x929100ad, 0x4721de4d,
		0x27df1b05, 0x58b18546, 0xb7e76764, 0xdf904e58, 0x97af57a1, 0xbd4dc433, 0xa6256dfd, 0xf63998f3,
		0xf1e05833, 0xe20acf26, 0xf57fd9d6, 0x90300b4d, 0x89df4290, 0x68d01cbc, 0xcf893ee3, 0xcc42a046,
		0x778e181b, 0x67265c76, 0xe981a4c4, 0x82991da1, 0x708f7294, 0xe6e2ae62, 0xfc441870, 0x95e1b0b6,
		0x445f825, 0x5a93b47f, 0x5e9cf4be, 0x84da71e7, 0x9d9582b0, 0x9bf835ef, 0x591f61e2, 0x43325985,
		0x5d2de32e, 0x8d8fbf0f, 0x95b30f38, 0x7ad5b6e, 0x4e934edf, 0x3cd4990e, 0x9053e259, 0x5c41857d,
	},
}

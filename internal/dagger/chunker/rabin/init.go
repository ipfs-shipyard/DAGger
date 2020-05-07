package rabin

import (
	"fmt"

	"github.com/ipfs-shipyard/DAGger/chunker"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewChunker(
	args []string,
	dgrCfg *dgrchunker.DaggerConfig,
) (
	_ chunker.Chunker,
	_ dgrchunker.InstanceConstants,
	initErrs []string,
) {

	c := rabinChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		// A panic as this should not be possible
		dgrCfg.InternalPanicf(
			"option set registration failed: %s",
			err,
		)
	}
	optSet.FlagLong(&c.presetName, "rabin-preset", 0, "The precomputed rabin preset to use, one of: "+util.AvailableMapKeys(rabinPresets))

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = util.SubHelp(
			"Chunker based on the venerable 'Rabin Fingerprint', similar to the one\n"+
				"used by `restic`, the LBFS, and others. Uses precomputed lookup tables for\n"+
				"a polynomial of degree 53, selectable via the rabin-preset option. This is\n"+
				"a slimmed-down implementation, adapted from multiple \"classic\" versions.\n",
			optSet,
		)
		return
	}

	// bail early if getopt fails
	if err := optSet.Getopt(args, nil); err != nil {
		initErrs = []string{err.Error()}
		return
	}

	args = optSet.Args()
	if len(args) != 0 {
		initErrs = append(initErrs, fmt.Sprintf(
			"unexpected parameter(s): %s...",
			args[0],
		))
	}

	if c.MinSize < 1 || c.MinSize > dgrCfg.GlobalMaxChunkSize-1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value for 'min-size' in the range [1:%d] must be specified",
			dgrCfg.GlobalMaxChunkSize-1,
		),
		)
	}
	if c.MaxSize < 1 || c.MaxSize > dgrCfg.GlobalMaxChunkSize {
		initErrs = append(initErrs, fmt.Sprintf(
			"value for 'max-size' in the range [1:%d] must be specified",
			dgrCfg.GlobalMaxChunkSize,
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
			"value for the uint64 'state-target' must be specified",
		)
	}

	if c.MaskBits < 8 || c.MaskBits > 22 {
		initErrs = append(initErrs,
			"value for 'state-mask-bits' in the range [8:22] must be specified",
		)
	}
	c.mask = 1<<uint(c.MaskBits) - 1

	var exists bool
	if c.preset, exists = rabinPresets[c.presetName]; !exists {
		initErrs = append(initErrs, fmt.Sprintf(
			"unknown rabin-preset '%s' requested, available names are: %s",
			c.presetName,
			util.AvailableMapKeys(rabinPresets),
		))
	}

	// Due to outTable[0] always being 0, this is simply the value 1
	// but derive it longform nevertheless
	c.initState = ((c.outTable[0] << 8) | 1) ^ (c.modTable[c.outTable[0]>>45])
	c.minSansPreheat = c.MinSize - c.windowSize

	return &c, dgrchunker.InstanceConstants{MinChunkSize: c.MinSize}, initErrs
}

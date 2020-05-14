package buzhash

import (
	"fmt"

	"github.com/ipfs-shipyard/DAGger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/constants"
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

	c := buzhashChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		initErrs = []string{fmt.Sprintf("option set registration failed: %s", err)}
		return
	}
	optSet.FlagLong(&c.xvName, "hash-table", 0, "The hash table to use, one of: "+util.AvailableMapKeys(hashTables))

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = util.SubHelp(
			"Chunker based on hashing by cyclic polynomial, similar to the one used\n"+
				"in 'attic-backup'. As source of \"hashing\" uses a predefined table of\n"+
				"values selectable via the hash-table option.",
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

	if c.MinSize < 1 || c.MinSize > constants.MaxLeafPayloadSize-1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value for 'min-size' in the range [1:%d] must be specified",
			constants.MaxLeafPayloadSize-1,
		),
		)
	}
	if c.MaxSize < 1 || c.MaxSize > constants.MaxLeafPayloadSize {
		initErrs = append(initErrs, fmt.Sprintf(
			"value for 'max-size' in the range [1:%d] must be specified",
			constants.MaxLeafPayloadSize,
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

	return &c, dgrchunker.InstanceConstants{MinChunkSize: c.MinSize}, initErrs
}

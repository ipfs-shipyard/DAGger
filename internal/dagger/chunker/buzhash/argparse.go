package buzhash

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

	c := buzhashChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		initErrs = []string{fmt.Sprintf("option set registration failed: %s", err)}
		return
	}
	optSet.FlagLong(&c.xvName, "hash-table", 0, "The hash table to use, one of: "+util.AvailableMapKeys(hashTables), "name")

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
	if initErrs = util.ArgParse(args, optSet); len(initErrs) > 0 {
		return
	}

	if c.MinSize >= c.MaxSize {
		initErrs = append(initErrs,
			"value for 'max-size' must be larger than 'min-size'",
		)
	}

	c.mask = 1<<uint(c.MaskBits) - 1
	c.target = uint32(c.TargetValue)

	var exists bool
	if c.xv, exists = hashTables[c.xvName]; !exists {
		initErrs = append(initErrs, fmt.Sprintf(
			"unknown hash-table '%s' requested, available names are: %s",
			c.xvName,
			util.AvailableMapKeys(hashTables),
		))
	}

	c.minSansPreheat = c.MinSize - 32

	return &c, dgrchunker.InstanceConstants{
		MinChunkSize: c.MinSize,
		MaxChunkSize: c.MaxSize,
	}, initErrs
}

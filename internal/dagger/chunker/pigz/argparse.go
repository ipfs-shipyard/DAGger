package pigz

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

	c := pigzChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		initErrs = []string{fmt.Sprintf("option set registration failed: %s", err)}
		return
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = util.SubHelp(
			"FIXME",
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
	c.minSansPreheat = c.MinSize - c.MaskBits

	return &c, dgrchunker.InstanceConstants{
		MinChunkSize: c.MinSize,
		MaxChunkSize: c.MaxSize,
	}, initErrs
}

package shrubber

import (
	"fmt"

	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewCollector(args []string, dgrCfg *dgrcollector.DaggerConfig) (_ dgrcollector.Collector, initErrs []string) {

	co := &collector{
		DaggerConfig: dgrCfg,
	}

	optSet := getopt.New()
	if err := options.RegisterSet("", &co.config, optSet); err != nil {
		initErrs = []string{fmt.Sprintf("option set registration failed: %s", err)}
		return
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = util.SubHelp(
			"Acts as a pre-processor for the next collector in the chain, grouping nodes\n"+
				"based on various criteria. Can not be the last collector in a chain.",
			optSet,
		)
		return
	}

	// bail early if getopt fails
	if initErrs = util.ArgParse(args, optSet); len(initErrs) > 0 {
		return
	}

	if co.NextCollector == nil {
		initErrs = append(
			initErrs,
			"collector can not be last in chain",
		)
	}

	co.cidMask = (1 << uint(co.SubgroupCidMaskBits)) - 1
	co.cidTailTarget = uint16(co.SubgroupCidTarget)

	return co, initErrs
}

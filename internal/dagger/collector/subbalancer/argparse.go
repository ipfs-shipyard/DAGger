package subbalancer

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

	// if co.MaxOutdegree < 2 {
	// 	initErrs = append(initErrs, fmt.Sprintf(
	// 		"value of 'max-outdegree' %d is out of range [2:...]",
	// 		co.MaxOutdegree,
	// 	))
	// }

	if co.NextCollector == nil {
		initErrs = append(
			initErrs,
			"collector can not be last in chain",
		)
	}

	co.cidMask = (1 << co.CidMaskBits) - 1

	return co, initErrs
}

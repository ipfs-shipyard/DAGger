package fixedoutdegree

import (
	"fmt"

	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewCollector(args []string, dgrCfg *dgrcollector.DaggerConfig) (_ dgrcollector.Collector, initErrs []string) {

	co := &collector{
		DaggerConfig: dgrCfg,
		state:        state{stack: [][]*dgrblock.Header{{}}},
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
			"Produces a DAG with every node having a fixed outdegree (amount of children)",
			optSet,
		)
		return
	}

	// bail early if getopt fails
	if initErrs = util.ArgParse(args, optSet); len(initErrs) > 0 {
		return
	}

	if co.NextCollector != nil {
		initErrs = append(
			initErrs,
			"collector must appear last in chain",
		)
	}

	return co, initErrs
}

package nul

import (
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

func NewCollector(args []string, dgrCfg *dgrcollector.DaggerConfig) (_ dgrcollector.Collector, initErrs []string) {

	if args == nil {
		initErrs = util.SubHelp(
			"Does not form a DAG, nor emits a root CID. Simply redirects chunked data\n"+
				"to /dev/null. Takes no arguments.\n",
			nil,
		)
		return
	}

	if len(args) > 1 {
		initErrs = append(initErrs, "collector takes no arguments")
	}
	if dgrCfg.NextCollector != nil {
		initErrs = append(initErrs, "collector must appear last in chain")
	}

	return &nulCollector{dgrCfg}, initErrs
}

package main

import (
	"fmt"
	"log"
	"os"

	// import early for version check
	_ "github.com/ipfs-shipyard/DAGger/constants"

	"github.com/ipfs-shipyard/DAGger/internal/dagger"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

func main() {

	// Parse CLI and initialize everything
	// On error it will log.Fatal() on its own
	cfg, panicfWrapper := dagger.ParseOpts(os.Args)

	util.InternalPanicf = panicfWrapper

	inStat, statErr := os.Stdin.Stat()
	if statErr != nil {
		panicfWrapper("unexpected error stat()ing stdIN: %s", statErr)
	}

	if 0 != (inStat.Mode() & os.ModeCharDevice) {
		// do not try to optimize a TTY
		fmt.Fprint(
			os.Stderr,
			"------\nYou seem to be feeding data straight from a terminal, an odd choice...\nNevertheless will proceed to read until EOF ( Ctrl+D )\n------\n",
		)
	} else {
		for _, opt := range util.ReadOptimizations {
			if err := opt.Action(os.Stdin, inStat); err != nil && err != os.ErrInvalid {
				log.Printf("Failed to apply read optimization hint '%s' to stdIN: %s\n", opt.Name, err)
			}
		}
	}

	processErr := func() error {
		if util.ProfileStartStop != nil {
			defer util.ProfileStartStop()()
		}

		return dagger.ProcessReader(
			cfg,
			os.Stdin,
			nil,
		)
	}()

	if processErr != nil {
		log.Fatalf("Unexpected error processing STDIN: %s", processErr)
	}

	dagger.OutputSummary(cfg)
}

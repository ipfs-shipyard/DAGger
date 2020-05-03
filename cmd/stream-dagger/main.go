package main

import (
	"fmt"
	"log"
	"os"
	"runtime"

	"golang.org/x/sys/unix"

	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

func main() {

	inStat, statErr := os.Stdin.Stat()
	if statErr != nil {
		log.Fatalf("unexpected error stat()ing stdIN: %s", statErr)
	}

	// Parse CLI and initialize everything
	// On error it will log.Fatal() on its own
	dgr, panicfWrapper := dagger.NewFromArgv(os.Args)

	util.InternalPanicf = panicfWrapper

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

	// func() wrapper for multiple defer triggers
	// Written this way to make profiling more straightforward
	processErr := func() error {

		if constants.PerformSanityChecks {
			defer func() {
				// when we get here we should have shut down every goroutine there is
				expectRunning := 1
				if runtime.NumGoroutine() > expectRunning {
					log.Printf("\n\nUnexpected amount of goroutines: expected %d but %d goroutines still running\n\n",
						expectRunning,
						runtime.NumGoroutine(),
					)
					p, _ := os.FindProcess(os.Getpid())
					p.Signal(unix.SIGQUIT)
				}

				// needed to trigger the zcpstring overallocation guards
				// unless we profile in which case we do it there
				if util.ProfileStartStop == nil {
					defer runtime.GC() // recommended by https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
					defer runtime.GC() // recommended harder by @warpfork and @kubuxu :cryingbear:
				}
			}()
		}

		// starts profiler, and schedules stop, as first order of business
		if util.ProfileStartStop != nil {
			defer util.ProfileStartStop()()
		}

		return dgr.ProcessReader(
			os.Stdin,
			nil,
		)
	}()

	if processErr != nil {
		log.Fatalf("Unexpected error processing STDIN: %s", processErr)
	}

	dgr.OutputSummary()
}

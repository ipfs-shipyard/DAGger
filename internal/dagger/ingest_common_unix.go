// +build linux darwin

package dagger

import (
	"runtime"

	"golang.org/x/sys/unix"
)

func init() {

	preProcessTasks = func(dgr *Dagger) {
		var ru unix.Rusage
		unix.Getrusage(unix.RUSAGE_SELF, &ru) // ignore errors
		sys := &dgr.statSummary.SysStats

		// set everything to negative values: we will simply += in postprocessing
		sys.CpuUserNsecs = -unix.TimevalToNsec(ru.Utime)
		sys.CpuSysNsecs = -unix.TimevalToNsec(ru.Stime)
		sys.MinFlt = -ru.Minflt
		sys.MajFlt = -ru.Majflt
		sys.BioRead = -ru.Inblock
		sys.BioWrite = -ru.Oublock
		sys.Sigs = -ru.Nsignals
		sys.CtxSwYield = -ru.Nvcsw
		sys.CtxSwForced = -ru.Nivcsw
	}

	postProcessTasks = func(dgr *Dagger) {
		var ru unix.Rusage
		unix.Getrusage(unix.RUSAGE_SELF, &ru) // ignore errors

		if runtime.GOOS != "darwin" {
			// anywhere but mac, maxrss is actually KiB, because fuck standards
			// https://lists.apple.com/archives/darwin-kernel/2009/Mar/msg00005.html
			ru.Maxrss *= 1024
		}

		sys := &dgr.statSummary.SysStats

		sys.MaxRssBytes = ru.Maxrss
		sys.CpuUserNsecs += unix.TimevalToNsec(ru.Utime)
		sys.CpuSysNsecs += unix.TimevalToNsec(ru.Stime)
		sys.MinFlt += ru.Minflt
		sys.MajFlt += ru.Majflt
		sys.BioRead += ru.Inblock
		sys.BioWrite += ru.Oublock
		sys.Sigs += ru.Nsignals
		sys.CtxSwYield += ru.Nvcsw
		sys.CtxSwForced += ru.Nivcsw
	}
}

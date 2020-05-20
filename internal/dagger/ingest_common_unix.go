// +build linux darwin

package dagger

import (
	"io/ioutil"
	"log"
	"os"
	"runtime"
	"time"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	"golang.org/x/sys/unix"
)

func (dgr *Dagger) initOptimizedCarFifos() (err error) {

	defer func() {
		// try to cleanup if possible
		if err != nil && dgr.carFifoDirectory != "" {
			os.RemoveAll(dgr.carFifoDirectory)
		}
	}()

	if dgr.carFifoDirectory, err = ioutil.TempDir("", "DagStream"+time.Now().Format("20060102_")); err != nil {
		return
	}

	// FIXME/SANCHECK: if I open with O_WRONLY - everything hangs on mac :(

	if err = unix.Mkfifo(dgr.carFifoDirectory+"/blocks.fifo", 0600); err != nil {
		return
	}
	if dgr.carFifoData, err = os.OpenFile(dgr.carFifoDirectory+"/blocks.fifo", os.O_RDWR, 0); err != nil {
		return
	}

	if err = unix.Mkfifo(dgr.carFifoDirectory+"/pins.fifo", 0600); err != nil {
		return
	}
	if dgr.carFifoPins, err = os.OpenFile(dgr.carFifoDirectory+"/pins.fifo", os.O_RDWR, 0); err != nil {
		return
	}

	dgr.carDataWriter = dgr.carFifoData

	for _, pipe := range []*os.File{
		dgr.carFifoData,
		dgr.carFifoPins,
	} {
		if pipeStat, statErr := pipe.Stat(); statErr != nil {
			return statErr
		} else {
			for _, opt := range util.WriteOptimizations {
				if err := opt.Action(pipe, pipeStat); err != nil && err != os.ErrInvalid {
					log.Printf("Failed to apply write optimization hint '%s' to car stream output: %s\n", opt.Name, err)
				}
			}
		}
	}

	return nil
}

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

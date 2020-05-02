// +build profiling

package util

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"time"
)

func init() {
	ProfileStartStop = setupProfiling
}

var profileOutDir string

func setupProfiling() (profilingStopper func()) {

	if profileOutDir == "" {
		log.Fatalf(`The destination 'profileOutDir' is set to an empty string: you need to build with -ldflags="-X '....profileOutDir=...'"`)
	}

	pathPrefix := profileOutDir + "/" + time.Now().Format("2006-01-02_15-04-05.000")
	var openHandles []*os.File

	cpuProfFh := openProfHandle("cpu", pathPrefix, &openHandles)
	heapProfFh := openProfHandle("heap", pathPrefix, &openHandles)

	runtime.GC() // recommended by https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
	runtime.GC() // recommended harder by @warpfork and @kubuxu :cryingbear:

	if err := pprof.StartCPUProfile(cpuProfFh); err != nil {
		log.Fatalf(
			"Unable to start CPU profiling: %s",
			err,
		)
	}

	// this function is a closer, no aborts on errors
	profilingStopper = func() {

		pprof.StopCPUProfile()
		runtime.GC() // recommended by https://golang.org/pkg/runtime/pprof/#hdr-Profiling_a_Go_program
		runtime.GC() // recommended harder by @warpfork and @kubuxu :cryingbear:

		if err := pprof.Lookup("heap").WriteTo(heapProfFh, 0); err != nil {
			log.Printf(
				"Error writing out 'heap' profile: %s",
				err,
			)
		}

		for _, fh := range openHandles {
			if err := fh.Close(); err != nil {
				log.Printf(
					"Closing %s failed: %s",
					fh.Name(),
					err,
				)
			}
		}
	}

	return profilingStopper
}

func openProfHandle(profName string, pathPrefix string, openHandles *[]*os.File) (fh *os.File) {
	filename := fmt.Sprintf("%s_%s.prof", pathPrefix, profName)

	var err error

	if fh, err = os.OpenFile(
		filename,
		os.O_RDWR|os.O_CREATE|os.O_EXCL,
		0640,
	); err != nil {
		log.Fatalf(
			"Unable to open '%s' profile output: %s",
			profName,
			err,
		)
	}

	// if we managed to open it - we also link it right away
	os.Symlink(
		filepath.Base(fh.Name()),
		fh.Name()+".templnk",
	)
	os.Rename(
		fh.Name()+".templnk",
		fmt.Sprintf(
			"%s/latest_%s.prof",
			filepath.Dir(fh.Name()),
			profName,
		),
	)

	*openHandles = append(*openHandles, fh)

	return fh
}

package util

import (
	"os"

	"golang.org/x/sys/unix"
)

func init() {

	// Raise the size of the passed-in pipe. Do so blindly without checks,
	// trying smaller and smaller powers of 2 ( starting from 32MiB ),
	// as the entire process is opportunistic and depends on system tuning.
	// This unfortunately only works on Linux
	// Background: https://github.com/afborchert/pipebuf
	ReadOptimizations = append(ReadOptimizations, FileHandleOptimization{
		"F_SETPIPE_SZ",
		func(file *os.File, stat os.FileInfo) (err error) {
			if 0 == (stat.Mode() & os.ModeNamedPipe) {
				return os.ErrInvalid
			}

			for pipeSize := 32 * 1024 * 1024; pipeSize > 512; pipeSize /= 2 {
				if _, err = unix.FcntlInt(file.Fd(), unix.F_SETPIPE_SZ, pipeSize); err == nil {
					return
				}
			}

			return
		},
	})

}

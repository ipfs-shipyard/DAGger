package util

import (
	"os"

	"golang.org/x/sys/unix"
)

func init() {

	// http://adityaramesh.com/io_benchmark/#read_optimizations
	// https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
	ReadOptimizations = append(ReadOptimizations, FileHandleOptimization{
		"F_RDAHEAD",
		func(file *os.File, stat os.FileInfo) error {
			if !stat.Mode().IsRegular() {
				return os.ErrInvalid
			}

			_, err := unix.FcntlInt(file.Fd(), unix.F_RDAHEAD, 1)
			return err
		},
	})

	// http://adityaramesh.com/io_benchmark/#read_optimizations
	// https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
	ReadOptimizations = append(ReadOptimizations, FileHandleOptimization{
		"F_RDADVISE (like POSIX_FADV_WILLNEED)",
		func(file *os.File, stat os.FileInfo) error {
			if !stat.Mode().IsRegular() {
				return os.ErrInvalid
			}

			s := stat.Size()
			if s == 0 {
				return nil
			}

			if s >= 1<<31 {
				s = 1<<31 - 1
			}
			_, err := unix.FcntlInt(
				file.Fd(),
				unix.F_RDADVISE,
				int(_addressofref(&unix.Radvisory_t{
					Offset: 0,
					Count:  int32(s),
				})),
			)
			return err
		},
	})

}

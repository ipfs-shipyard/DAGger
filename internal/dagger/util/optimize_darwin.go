package util

import (
	"math"
	"os"

	"golang.org/x/sys/unix"
)

func init() {

	// http://adityaramesh.com/io_benchmark/#read_optimizations
	// https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
	ReadOptimizations = append(ReadOptimizations, FileHandleOptimization{
		"F_RDAHEAD",
		func(fh *os.File, stat os.FileInfo) error {
			if !stat.Mode().IsRegular() {
				return os.ErrInvalid
			}

			_, err := unix.FcntlInt(fh.Fd(), unix.F_RDAHEAD, 1)
			return err
		},
	})

	// http://adityaramesh.com/io_benchmark/#read_optimizations
	// https://developer.apple.com/library/archive/documentation/System/Conceptual/ManPages_iPhoneOS/man2/fcntl.2.html
	ReadOptimizations = append(ReadOptimizations, FileHandleOptimization{
		"F_RDADVISE (like POSIX_FADV_WILLNEED)",
		func(fh *os.File, stat os.FileInfo) error {
			if !stat.Mode().IsRegular() {
				return os.ErrInvalid
			}

			s := stat.Size()
			if s == 0 {
				return nil
			}

			if s > math.MaxInt32 {
				s = math.MaxInt32
			}
			_, err := unix.FcntlInt(
				fh.Fd(),
				unix.F_RDADVISE,
				// Yes, we are casting an address to an int, because... golang
				// This shoud be safe, however, as it is immediately recast back to uintptr
				// https://github.com/golang/sys/blob/417ce2331b/unix/zsyscall_darwin_amd64.go#L714-L715
				int(_addressofref(&unix.Radvisory_t{
					Offset: 0,
					Count:  int32(s),
				})),
			)
			return err
		},
	})

}

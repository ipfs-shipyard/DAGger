package main

import (
	"bytes"
	"encoding/binary"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"regexp"
	"strconv"

	"github.com/klauspost/compress/zstd"
)

var sizeFinder = regexp.MustCompile(`(?m)^Decompressed Size:.+\(([0-9]+) B\)`)
var nativeZstdWorks bool

func main() {

	defer func() {
		if err := os.Stdout.Close(); err != nil {
			log.Fatalf("Failed stdout flush: %s", err)
		}
	}()

	// each file's decompressed size is SInt64-prefixed to the output
	for _, fn := range os.Args[1:] {

		var size int64

		// FIXME: entire scope is a crappy replacement for https://github.com/klauspost/compress/issues/237
		{
			var out bytes.Buffer
			nativeZstd := exec.Command("zstd", "-lv", fn)
			nativeZstd.Stdout = &out
			if err := nativeZstd.Run(); err == nil {
				if sizeStr := sizeFinder.FindSubmatch(out.Bytes()); len(sizeStr) == 2 && len(sizeStr[1]) > 0 {
					size, err = strconv.ParseInt(
						string(sizeStr[1]),
						10,
						64,
					)

					if err == nil && size >= 0 {
						nativeZstdWorks = true
					}
				}
			}
		}

		// if all above fails :cryingbear:
		if !nativeZstdWorks {
			size = decompressFile(fn, ioutil.Discard)
		}

		if err := binary.Write(os.Stdout, binary.BigEndian, size); err != nil {
			log.Fatalf("Failed writing streamsize to stdout: %s", err)
		}

		decompressFile(fn, os.Stdout)
	}
}

// fatal()s-out in case of error
func decompressFile(fn string, sink io.Writer) int64 {

	// Opportunstically try to invoke the native ( 30% faster ) zstd
	// Since we already validated it works earlier, we do not need to
	// determine/return the length at all
	if nativeZstdWorks {
		nativeZstd := exec.Command("zstd", "-qdck", fn)
		nativeZstd.Stdout = sink
		if err := nativeZstd.Run(); err != nil {
			log.Fatalf("Native decompressor failed: %s", err)
		}

		return -1
	}

	in, openErr := os.Open(fn)
	if openErr != nil {
		log.Fatalf("Open of '%s' failed: %s", fn, openErr)
	}

	decompressor, initErr := zstd.NewReader(in)
	if initErr != nil {
		log.Fatalf("Failed decompressor construction: %s", initErr)
	}

	defer func() {
		if err := in.Close(); err != nil {
			log.Fatalf("Failed input close: %s", err)
		}

		decompressor.Close()
	}()

	written, copyErr := io.Copy(sink, decompressor)
	if copyErr != nil {
		log.Fatalf("Decompression failed after %d bytes: %s", written, copyErr)
	}

	return written
}

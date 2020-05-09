package zcpstring

import (
	"io"
	"log"
	"runtime"

	"github.com/ipfs-shipyard/DAGger/internal/constants"
)

var byteDict [256]byte

func init() {
	for i := 1; i < 256; i++ {
		byteDict[i] = byte(i)
	}
}

type ZcpString struct {
	growGuard bool
	size      int
	slices    [][]byte
}

func (z *ZcpString) Size() int { return z.size }

func NewFromSlice(s []byte) *ZcpString {
	return &ZcpString{
		slices:    [][]byte{s},
		size:      len(s),
		growGuard: constants.PerformSanityChecks,
	}
}
func NewWithSegmentCap(capOfSegmentContainer int) *ZcpString {
	zcp := &ZcpString{
		slices: make([][]byte, 0, capOfSegmentContainer),
	}
	if constants.PerformSanityChecks {
		zcp._attachOverallocationGuard()
		zcp.growGuard = true
	}
	return zcp
}

func (z *ZcpString) AddSlice(in []byte) {
	if constants.PerformSanityChecks && z.growGuard {
		z._assertSpaceFor(1)
	}
	z.size += len(in)
	z.slices = append(z.slices, in)
}
func (z *ZcpString) AddByte(in byte) {
	if constants.PerformSanityChecks && z.growGuard {
		z._assertSpaceFor(1)
	}
	z.size++
	z.slices = append(z.slices, byteDict[in:int(in)+1])
}
func (z *ZcpString) AddZcp(in *ZcpString) {
	if constants.PerformSanityChecks && z.growGuard {
		z._assertSpaceFor(len(in.slices))
	}
	z.size += in.size
	z.slices = append(z.slices, in.slices...)
}

func (z *ZcpString) AppendTo(target []byte) []byte {
	for i := range z.slices {
		target = append(target, z.slices[i]...)
	}
	return target
}
func (z *ZcpString) WriteTo(w io.Writer) (written int64, err error) {
	var n int
	for i := range z.slices {
		n, err = w.Write(z.slices[i])
		written += int64(n)
		if err != nil {
			return
		}
	}
	return
}

func (z *ZcpString) _assertSpaceFor(n int) {
	if len(z.slices)+n > cap(z.slices) {
		log.Panicf(
			"Unexpected segmentlist grow: have %d, need %d",
			cap(z.slices),
			len(z.slices)+n,
		)
	}
}
func (z *ZcpString) _attachOverallocationGuard() {
	runtime.SetFinalizer(z, func(z *ZcpString) {
		if cap(z.slices) != len(z.slices) {
			lim := z.size
			suffix := ""
			if lim > 20 {
				lim = 20
				suffix = "..."
			}
			// Fatal instead of Panic, as the stack won't help us much at this point
			log.Fatalf(
				"\n!!!!!!!!!!!!!!!!!!!!!\nOverallocated zcpstring %#v%s: at GC destroyed object has only %d segments used of %d capacity\n!!!!!!!!!!!!!!!!!!!!!\n\n",
				(z.AppendTo(make([]byte, 0)))[:lim],
				suffix,
				len(z.slices),
				cap(z.slices),
			)
		}
	})
}

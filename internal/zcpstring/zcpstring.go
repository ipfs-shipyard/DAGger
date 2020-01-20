package zcpstring

import (
	"io"
	"log"
	"runtime"

	"github.com/ipfs-shipyard/DAGger/constants"
)

var byteDict [256]byte

func init() {
	for i := 1; i < 256; i++ {
		byteDict[i] = byte(i)
	}
}

type ZcpString struct {
	trackAlloc bool
	size       int
	slices     [][]byte
}

func NewFromSlice(s []byte) *ZcpString {
	return &ZcpString{
		slices: [][]byte{s},
		size:   len(s),
	}
}

func NewWithSegmentCap(capOfSegmentContainer int) *ZcpString {
	z := &ZcpString{
		slices:     make([][]byte, 0, capOfSegmentContainer),
		trackAlloc: constants.PerformSanityChecks,
	}

	if constants.PerformSanityChecks {
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
					"\n!!!!!!!!!!!!!!!!!!!!!\nOverallocated zstring %#v%s: %d segments used of %d capacity\n!!!!!!!!!!!!!!!!!!!!!\n\n",
					(z.AppendTo(make([]byte, 0)))[:lim],
					suffix,
					len(z.slices),
					cap(z.slices),
				)
			}
		})
	}

	return z
}
func (z *ZcpString) SegmentLen() int { return len(z.slices) }
func (z *ZcpString) Size() int       { return z.size }

func (z *ZcpString) AddSlice(in []byte) {
	if constants.PerformSanityChecks && z.trackAlloc &&
		len(z.slices)+1 > cap(z.slices) {
		log.Panicf(
			"Unexpected segmentlist grow: have %d, need %d",
			cap(z.slices),
			len(z.slices)+1,
		)
	}

	z.size += len(in)
	z.slices = append(z.slices, in)
}
func (z *ZcpString) AddByte(in byte) {
	if constants.PerformSanityChecks && z.trackAlloc &&
		len(z.slices)+1 > cap(z.slices) {
		log.Panicf(
			"Unexpected segmentlist grow: have %d, need %d",
			cap(z.slices),
			len(z.slices)+1,
		)
	}

	z.size++
	z.slices = append(z.slices, byteDict[in:int(in)+1])
}
func (z *ZcpString) AddZcp(in *ZcpString) {
	if constants.PerformSanityChecks && z.trackAlloc &&
		len(z.slices)+len(in.slices) > cap(z.slices) {
		log.Panicf(
			"Unexpected segmentlist grow: have %d, need %d",
			cap(z.slices),
			len(z.slices)+len(in.slices),
		)
	}

	z.size += in.size
	z.slices = append(z.slices, in.slices...)
}

func (z *ZcpString) SliceList() [][]byte {
	return z.slices
}
func (z *ZcpString) AppendTo(target []byte) []byte {
	for _, s := range z.slices {
		target = append(target, s...)
	}
	return target
}
func (z *ZcpString) WriteTo(w io.Writer) (written int64, err error) {
	var len int
	for _, s := range z.slices {
		len, err = w.Write(s)
		written += int64(len)
		if err != nil {
			return
		}
	}
	return
}

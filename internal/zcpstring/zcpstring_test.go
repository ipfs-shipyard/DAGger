package zcpstring

import (
	"crypto/sha1"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/ipfs-shipyard/DAGger/internal/constants"
	"github.com/ipfs-shipyard/DAGger/maint/src/testhelpers"
)

func TestString(t *testing.T) {

	var corpus ZcpString
	var reference, inlayS []byte

	rand.Seed(time.Now().UnixNano())
	inlayZ := NewWithSegmentCap(1)
	inlayS = append(inlayS, time.Now().Local().String()...)
	inlayZ.AddSlice(inlayS)

	var expectedFinalCap int
	for i := 0; i < 520; i++ {
		b := rand.Intn(256)

		reference = append(reference, inlayS...)
		reference = append(reference, byte(b))
		reference = append(reference, byte(i))

		expectedFinalCap += 3

		if i%2 == 0 {
			corpus.AddZcp(inlayZ)
		} else {
			corpus.AddSlice(inlayS)
		}
		corpus.AddByte(byte(b))
		corpus.AddByte(byte(i))
	}

	if expectedFinalCap != len(corpus.slices) {
		t.Errorf(
			"Final test corpus segment count %d does not match expected value of %d",
			len(corpus.slices),
			expectedFinalCap,
		)
	}

	refHash := sha1.Sum(reference)

	h := sha1.New()
	corpus.WriteTo(h)
	var writetoHash [20]byte
	copy(writetoHash[:], h.Sum([]byte{}))

	appendHash := sha1.Sum(corpus.AppendTo([]byte{}))

	if refHash != writetoHash || refHash != appendHash {

		t.Errorf(
			"Hashes derived from zcpstring do not match the hash of the %d byte reference\n%s\nRefHash:\t%x\nWriteTo:\t%x\nAppendTo:\t%x",
			len(reference),
			testhelpers.EncodeTestVector(reference),
			refHash,
			writetoHash,
			appendHash,
		)
	}

	// if we are doing over/underallocation checks we need to GC to trigger the finalizers
	if constants.PerformSanityChecks {
		runtime.GC()
		runtime.GC() // recommended harder by @warpfork and @kubuxu :cryingbear:
	}
}

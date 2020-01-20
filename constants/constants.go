package constants

import (
	"os"
	"strconv"
)

const (
	// hard limit for any block on-wire size in bytes
	// the 32 is explicit - many assumptions we won't cross over
	HardMaxBlockSize int32 = 2 * 1024 * 1024

	// SANCHECK - will need to possibly be updated for CBOR ( whichever ends up providing less available space )
	// Based on DAG-PB/UnixFs-v1, with max len of "2MiB shave a few bytes" encoded as 3 varint bytes
	// (2b(type2/file)+4b(data-field:3-byte-len-delimited)+4b(size-field:3-byte-varint))+(4b(DAG-type-1:3-byte-len-delimited))
	HardMaxPayloadSize int32 = HardMaxBlockSize - (2 + 4 + 4 + 4)

	minGoVersion = __SOFTWARE_REQUIRES_GO_VERSION_1_13__
)

var LongTests bool
var VeryLongTests bool

func init() {
	VeryLongTests = isTruthy("TEST_DAGGER_VERY_LONG")
	LongTests = VeryLongTests || isTruthy("TEST_DAGGER_LONG")
}

func isTruthy(varname string) bool {
	envStr := os.Getenv(varname)
	if envStr != "" {
		if num, err := strconv.ParseUint(envStr, 10, 64); err != nil || num != 0 {
			return true
		}
	}
	return false
}

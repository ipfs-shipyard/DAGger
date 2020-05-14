package constants

import (
	"os"
	"strconv"
)

const (
	// https://github.com/ipfs/go-ipfs-chunker/pull/21#discussion_r369197120
	MaxLeafPayloadSize = 1024 * 1024
	MaxBlockWireSize   = (2 * 1024 * 1024) - 1

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

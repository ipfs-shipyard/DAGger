// +build !race

package util

func init() {
	CheckGoroutineShutdown = true
}

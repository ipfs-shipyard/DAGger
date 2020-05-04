// +build !race

package util

func init() {
	CheckGoroutineCount = true
}

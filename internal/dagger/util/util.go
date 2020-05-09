package util

import (
	"bytes"
	"fmt"
	"log"
	"math/bits"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ipfs-shipyard/DAGger/internal/constants"
	getopt "github.com/pborman/getopt/v2"
)

func VarintWireSize(v uint64) int {
	if constants.PerformSanityChecks && v >= 1<<63 {
		log.Panicf("Value %#v too large for a varint: https://github.com/multiformats/unsigned-varint#practical-maximum-of-9-bytes-for-security", v)
	}

	if v == 0 {
		return 1
	}

	return (bits.Len64(v) + 6) / 7
}
func VarintSlice(v uint64) []byte {
	return AppendVarint(
		make([]byte, 0, VarintWireSize(v)),
		v,
	)
}
func AppendVarint(tgt []byte, v uint64) []byte {
	for v > 127 {
		tgt = append(tgt, byte(v|128))
		v >>= 7
	}
	return append(tgt, byte(v))
}

var CheckGoroutineShutdown bool

var ProfileStartStop func() func()

// FileHandleOptimizations is populated by individual OS-specific init()s
var ReadOptimizations []FileHandleOptimization

type FileHandleOptimization struct {
	Name   string
	Action func(
		file *os.File,
		stat os.FileInfo,
	) error
}

// This is a surprisingly cheap and reliable way to emulate a part of unsafe.*
// Use this for various syscalls, not to pull in unsafe and make folks go 😱🙀🤮
func _addressofref(val interface{}) uintptr {
	a, _ := strconv.ParseInt(fmt.Sprintf("%p", val), 0, 64)
	return uintptr(a)
}

func Commify(inVal int) []byte {
	return Commify64(int64(inVal))
}

func Commify64(inVal int64) []byte {
	inStr := strconv.FormatInt(inVal, 10)

	outStr := make([]byte, 0, 20)
	i := 1

	if inVal < 0 {
		outStr = append(outStr, '-')
		i++
	}

	for i <= len(inStr) {
		outStr = append(outStr, inStr[i-1])

		if i < len(inStr) &&
			((len(inStr)-i)%3) == 0 {
			outStr = append(outStr, ',')
		}

		i++
	}

	return outStr
}

func AvailableMapKeys(m interface{}) string {
	v := reflect.ValueOf(m)
	if v.Kind() != reflect.Map {
		log.Panicf("input type not a map: %v", v)
	}
	avail := make([]string, 0, v.Len())
	for _, k := range v.MapKeys() {
		avail = append(avail, "'"+k.String()+"'")
	}
	sort.Strings(avail)
	return strings.Join(avail, ", ")
}

// ugly as sin due to lack of lookaheads :/
var indenter = regexp.MustCompile(`(?m)^([^\n])`)
var nonOptIndenter = regexp.MustCompile(`(?m)^\s{0,12}([^\s\n\-])`)
var dashStripper = regexp.MustCompile(`(?m)^(\s*)\-\-`)

func SubHelp(description string, optSet *getopt.Set) (sh []string) {

	sh = append(
		sh,
		string(indenter.ReplaceAll(
			[]byte(description),
			[]byte(`  $1`),
		)),
	)

	if optSet == nil {
		return sh
	}

	b := bytes.NewBuffer(make([]byte, 0, 1024))
	optSet.PrintOptions(b)

	sh = append(sh, "  ------------\n   SubOptions")
	sh = append(sh,
		string(dashStripper.ReplaceAll(
			nonOptIndenter.ReplaceAll(
				b.Bytes(),
				[]byte(`              $1`),
			),
			[]byte(`$1  `),
		)),
	)

	return sh
}

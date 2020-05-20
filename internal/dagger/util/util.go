package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math"
	"math/bits"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/ipfs-shipyard/DAGger/internal/constants"
	"github.com/mattn/go-isatty"
	getopt "github.com/pborman/getopt/v2"
)

func VarintWireSize(v uint64) int {
	if constants.PerformSanityChecks && v > math.MaxInt64 {
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

func CborHeaderWiresize(l uint64) int {
	switch {
	case l <= 23:
		return 1
	case l <= math.MaxUint8:
		return 2
	case l <= math.MaxUint16:
		return 3
	case l <= math.MaxUint32:
		return 5
	default:
		return 9
	}
}

func CborHeaderWrite(w io.Writer, t byte, l uint64) (err error) {
	switch {

	case l <= 23:
		_, err = w.Write([]byte{(t << 5) | byte(l)})

	case l <= math.MaxUint8:
		_, err = w.Write([]byte{(t << 5) | 24, byte(l)})

	case l <= math.MaxUint16:
		var b [3]byte
		b[0] = (t << 5) | 25
		binary.BigEndian.PutUint16(b[1:3], uint16(l))
		_, err = w.Write(b[:])

	case l <= math.MaxUint32:
		var b [5]byte
		b[0] = (t << 5) | 26
		binary.BigEndian.PutUint32(b[1:5], uint32(l))
		_, err = w.Write(b[:])

	default:
		var b [9]byte
		b[0] = (t << 5) | 27
		binary.BigEndian.PutUint64(b[1:], uint64(l))
		_, err = w.Write(b[:])

	}

	return
}

var CheckGoroutineShutdown bool

var ProfileStartStop func() func()

func IsTTY(maybeFile interface{}) bool {
	// not a file => not a TTY
	if f, isFile := maybeFile.(*os.File); isFile {
		return isatty.IsTerminal(f.Fd()) || isatty.IsCygwinTerminal(f.Fd())
	}
	return false
}

// FileHandleOptimizations is populated by individual OS-specific init()s
var ReadOptimizations, WriteOptimizations []FileHandleOptimization

type FileHandleOptimization struct {
	Name   string
	Action func(
		handle *os.File,
		stat os.FileInfo,
	) error
}

// This is a surprisingly cheap and reliable way to emulate a part of unsafe.*
// Use this for various optimization syscalls (in platform-specific util's)
// Saves us from pulling unsafe proper, which is known to make folks go 😱🙀🤮
func _addressofref(val interface{}) uintptr {
	a, _ := strconv.ParseUint(fmt.Sprintf("%p", val), 0, 64)
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

var maxPlaceholder = regexp.MustCompile(`\bMaxPayload\b`)

func ArgParse(args []string, optSet *getopt.Set) (argErrs []string) {

	if err := optSet.Getopt(args, nil); err != nil {
		argErrs = append(argErrs, err.Error())
	}

	unexpectedArgs := optSet.Args()
	if len(unexpectedArgs) != 0 {
		argErrs = append(argErrs, fmt.Sprintf(
			"unexpected free-form parameter(s): %s...",
			unexpectedArgs[0],
		))
	}

	// going through the limits when we are already in error is too confusing
	if len(argErrs) > 0 {
		return
	}

	optSet.VisitAll(func(o getopt.Option) {
		if spec := []byte(reflect.ValueOf(o).Elem().FieldByName("name").String()); len(spec) > 0 {

			max := int((^uint(0)) >> 1)
			min := -max - 1

			if spec[0] == '[' && spec[len(spec)-1] == ']' {
				spec = maxPlaceholder.ReplaceAll(spec, []byte(fmt.Sprintf("%d", constants.MaxLeafPayloadSize)))

				if _, err := fmt.Sscanf(string(spec), "[%d:]", &min); err != nil {
					if _, err := fmt.Sscanf(string(spec), "[%d:%d]", &min, &max); err != nil {
						argErrs = append(argErrs, fmt.Sprintf("Failed parsing '%s' as '[%%d:%%d]' - %s", spec, err))
						return
					}
				}
			} else if bytes.Equal(spec, []byte("uint32")) {
				min = 0
				max = math.MaxUint32
			} else {
				// not a spec we recognize
				return
			}

			if !o.Seen() {
				argErrs = append(argErrs, fmt.Sprintf("a value for %s must be specified", o.LongName()))
				return
			}

			actual, err := strconv.ParseInt(o.Value().String(), 10, 64)
			if err != nil {
				argErrs = append(argErrs, err.Error())
				return
			}

			if actual < int64(min) || actual > int64(max) {
				argErrs = append(argErrs, fmt.Sprintf(
					"value '%d' supplied for %s out of range [%d:%d]",
					actual,
					o.LongName(),
					min, max,
				))
			}
		}
	})

	return
}

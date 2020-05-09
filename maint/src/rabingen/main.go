package main

import (
	"fmt"
	"log"
	"math/bits"
	"os"
	"regexp"
	"strconv"
)

// this is the struct we output
type variant struct {
	polynomial uint64
	windowSize int
	degShift   int
	outTable   [256]uint64
	modTable   [256]uint64
}

// Pol is a polynomial from F_2[X].
type Pol uint64

func main() {

	var v variant
	var wSize int64

	if len(os.Args) != 3 {
		log.Fatal("Requires 2 arguments: the uint64 polynomial, and a window size")
	}

	var err error
	if v.polynomial, err = strconv.ParseUint(os.Args[1], 10, 64); err != nil {
		log.Fatalf("Unable to parse uint64 polynomial '%s': %s", os.Args[1], err)
	}
	if wSize, err = strconv.ParseInt(os.Args[2], 10, 31); err != nil {
		log.Fatalf("Unable to parse int windowSize '%s': %s", os.Args[2], err)
	}

	v.windowSize = int(wSize)

	pol := Pol(v.polynomial)

	// calculate table for sliding out bytes. The byte to slide out is used as
	// the index for the table, the value contains the following:
	// out_table[b] = Hash(b || 0 ||        ...        || 0)
	//                          \ windowsize-1 zero bytes /
	// To slide out byte b_0 for window size w with known hash
	// H := H(b_0 || ... || b_w), it is sufficient to add out_table[b_0]:
	//    H(b_0 || ... || b_w) + H(b_0 || 0 || ... || 0)
	//  = H(b_0 + b_0 || b_1 + 0 || ... || b_w + 0)
	//  = H(    0     || b_1 || ...     || b_w)
	//
	// Afterwards a new byte can be shifted in.
	for b := 0; b < 256; b++ {
		var h Pol

		h <<= 8
		h |= Pol(b)
		h = h.Mod(pol)

		for i := 0; i < v.windowSize-1; i++ {
			h <<= 8
			h = h.Mod(pol)
		}
		v.outTable[b] = uint64(h)
	}

	// calculate table for reduction mod Polynomial
	k := pol.Deg()
	if k != 53 {
		log.Fatalf("Polynomial of degree %d provided, but degree 53 expected\n", k)
	}
	v.degShift = k - 8
	for b := uint64(0); b < 256; b++ {
		// mod_table[b] = A | B, where A = (b(x) * x^k mod pol) and  B = b(x) * x^k
		//
		// The 8 bits above deg(Polynomial) determine what happens next and so
		// these bits are used as a lookup to this table. The value is split in
		// two parts: Part A contains the result of the modulus operation, part
		// B is used to cancel out the 8 top bits so that one XOR operation is
		// enough to reduce modulo Polynomial
		v.modTable[b] = uint64(Pol(b<<uint(k)).Mod(pol)) | b<<uint(k)
	}

	fmt.Printf("%s\n",
		regexp.MustCompile(`((?:0x[0-9a-f]+(?:\}, |, )){4})`).ReplaceAll(
			[]byte(fmt.Sprintf("%#v\n", v)),
			[]byte("$1\n"),
		),
	)
}

// Add returns x+y.
func (x Pol) Add(y Pol) Pol {
	return Pol(uint64(x) ^ uint64(y))
}

// Deg returns the degree of the polynomial x. If x is zero, -1 is returned.
func (x Pol) Deg() int {
	return bits.Len64(uint64(x)) - 1
}

// DivMod returns x / d = q, and remainder r,
// see https://en.wikipedia.org/wiki/Division_algorithm
func (x Pol) DivMod(d Pol) (Pol, Pol) {
	if x == 0 {
		return 0, 0
	}

	if d == 0 {
		panic("division by zero")
	}

	D := d.Deg()
	diff := x.Deg() - D
	if diff < 0 {
		return 0, x
	}

	var q Pol
	for diff >= 0 {
		m := d << uint(diff)
		q |= 1 << uint(diff)
		x = x.Add(m)

		diff = x.Deg() - D
	}

	return q, x
}

// Div returns the integer division result x / d.
func (x Pol) Div(d Pol) Pol {
	q, _ := x.DivMod(d)
	return q
}

// Mod returns the remainder of x / d
func (x Pol) Mod(d Pol) Pol {
	_, r := x.DivMod(d)
	return r
}

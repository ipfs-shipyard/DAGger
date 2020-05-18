// +build padfinder_rure

package padfinder

import (
	rure "github.com/BurntSushi/rure-go"
)

type finderInstance struct{ *rure.Regex }

const (
	freeformOptName = "pad-freeform-rure"
	freeformOptDesc = "FIXME"
)

func compileRegex(re string) (finderInstance, error) {
	r, err := rure.CompileOptions(
		re,
		0,
		rure.NewOptions(),
	)
	return finderInstance{r}, err
}

func (rure finderInstance) findNext(haystack []byte) (int, int) {
	if start, end, didMatch := rure.FindBytes(haystack); didMatch {
		return start, end
	}
	return 0, 0
}

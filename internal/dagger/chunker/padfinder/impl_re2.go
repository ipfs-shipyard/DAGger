// +build !padfinder_rure

package padfinder

import (
	"regexp"
)

type finderInstance struct{ *regexp.Regexp }

const (
	freeformOptName = "pad-freeform-re2"
	freeformOptDesc = "FIXME"
)

func compileRegex(re string) (finderInstance, error) {
	r, err := regexp.Compile(re)
	return finderInstance{r}, err
}

func (re2 finderInstance) findNext(haystack []byte) (int, int) {
	if m := re2.FindIndex(haystack); m != nil {
		return m[0], m[1]
	}
	return 0, 0
}

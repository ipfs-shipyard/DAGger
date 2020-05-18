// +build !padfinder_re2,!padfinder_rure

package padfinder

type finderInstance struct{}

const (
	freeformOptName = __MUST_SELECT_ONE_PADFINDER_BUILD_FLAVOR_TAG___padfinder_re2___OR___padfinder_rure
	freeformOptDesc = ""
)

func compileRegex(re string) (f finderInstance, e error)       { return }
func (re2 finderInstance) findNext(haystack []byte) (int, int) { return 0, 0 }

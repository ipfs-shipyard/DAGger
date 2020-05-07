package trickle

import (
	"fmt"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/enc/dagpb"
	dgrlinker "github.com/ipfs-shipyard/DAGger/internal/dagger/linker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

type linker struct {
	config
	*dgrlinker.DaggerConfig
	state
}

func NewLinker(args []string, dgrCfg *dgrlinker.DaggerConfig) (_ dgrlinker.Linker, initErrs []string) {

	l := &linker{
		DaggerConfig: dgrCfg,
	}

	optSet := getopt.New()
	if err := options.RegisterSet("", &l.config, optSet); err != nil {
		// A panic as this should not be possible
		dgrCfg.InternalPanicf(
			"option set registration failed: %s",
			err,
		)
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		return nil, util.SubHelp(
			"Produces a \"side-balanced\" DAG optimized for streaming. Data blocks further\n"+
				"away from the stream start are arranged in nodes at increasing depth away\n"+
				"from the root. The rough \"placement group\" for a particular node LeafIndex\n"+
				"away from the stream start can be derived numerically via:\n"+
				"int( log( LeafIndex / MaxDirectLeaves ) / log( 1 + MaxSiblingSubgroups ) )\n"+
				"See the example program in trickle/init.go for more info\n"+
				"First argument must be a version specifier 'vN'. Currently only supports\n"+
				"'v0', generating go-ipfs-standard, inefficient, 'Tsize'-full linknodes.",
			optSet,
		)
	}

	// bail early if version unset
	if len(args) < 2 || args[1] != "--v0" {
		return nil, []string{"first linker option must be a version - currently only 'v0' is supported"}
	}

	if l.NextLinker != nil {
		initErrs = append(
			initErrs,
			"v0 linker must appear last in chain",
		)
	}

	args = append(
		[]string{args[0]},
		// drop the v0
		args[2:]...,
	)

	// bail early if getopt fails
	if err := optSet.Getopt(args, nil); err != nil {
		return nil, []string{err.Error()}
	}

	args = optSet.Args()
	if len(args) != 0 {
		initErrs = append(initErrs, fmt.Sprintf(
			"unexpected parameter(s): %s...",
			args[0],
		))
	}

	if l.LegacyCIDv0Links &&
		(l.HasherName != "sha2-256" ||
			l.HasherBits != 256) {
		initErrs = append(
			initErrs,
			"legacy CIDv0 linking requires --hash=sha2-256 and --hash-bits=256",
		)
	}

	if l.MaxDirectLeaves < 1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value of 'max-direct-leaves' %d is out of range [1:...]",
			l.MaxDirectLeaves,
		))
	}

	if l.MaxSiblingSubgroups < 1 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value of 'max-sibling-subgroups' %d is out of range [1:...]",
			l.MaxSiblingSubgroups,
		))
	}

	if l.LegacyDecoratedLeaves {
		l.newLeaf = func(ls block.LeafSource) *block.Header {
			return dagpb.UnixFSv1Leaf(ls, l.BlockMaker, dagpb.UnixFsTypeRaw)
		}
	} else {
		l.newLeaf = func(ls block.LeafSource) *block.Header {
			return block.RawDataLeaf(ls, l.BlockMaker)
		}
	}

	return l, initErrs
}

// Complete CLI program demonstrating node placement
//
/*

package main

import (
	"fmt"
	"math"
)

const (
	totalLeaves         = 200
	maxDirectLeaves     = 4 // ipfs default is 174
	maxSiblingSubgroups = 2 // ipfs default is 4
)

func main() {
	for existingLeafCount := 0; existingLeafCount <= totalLeaves; existingLeafCount++ {

		// displaying direct leaf population is not interesting
		if (existingLeafCount % maxDirectLeaves) != 0 {
			continue
		}

		// all calculations below rely on the pre-existing leaf count ( 0-based index if you will )
		remainingLeaves := existingLeafCount
		fmt.Printf("%5s", fmt.Sprintf("#%d", remainingLeaves))

		for remainingLeaves >= maxDirectLeaves {

			descentLevel := int(
				(math.Log(float64((remainingLeaves) / maxDirectLeaves))) /
					math.Log(float64((1 + maxSiblingSubgroups))),
			)

			descentLevelMembers := maxDirectLeaves * int(math.Pow(
				float64(maxSiblingSubgroups+1),
				float64(descentLevel),
			))

			if remainingLeaves >= descentLevelMembers {
				subGroupNodeIndexAtLevel := (remainingLeaves / descentLevelMembers) + (descentLevel * maxSiblingSubgroups)
				fmt.Printf("\t%d", subGroupNodeIndexAtLevel)
			}

			remainingLeaves %= descentLevelMembers
		}
		fmt.Print("\n")
	}
}

*/

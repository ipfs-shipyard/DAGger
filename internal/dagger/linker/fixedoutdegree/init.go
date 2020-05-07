package fixedoutdegree

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
		state:        state{stack: [][]*block.Header{{}}},
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
			"Produces a DAG with every node having a fixed outdegree (amount of children)\n"+
				"First argument must be a version specifier 'vN'. Currently only supports\n"+
				"'v0' generating go-ipfs-standard, inefficient, 'Tsize'-full linknodes.",
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

	if l.MaxOutdegree < 2 {
		initErrs = append(initErrs, fmt.Sprintf(
			"value of 'max-outdegree' %d is out of range [2:...]",
			l.MaxOutdegree,
		))
	}

	if l.LegacyDecoratedLeaves {
		l.newLeaf = func(ls block.LeafSource) *block.Header {
			return dagpb.UnixFSv1Leaf(ls, l.BlockMaker, dagpb.UnixFsTypeFile)
		}
	} else {
		l.newLeaf = func(ls block.LeafSource) *block.Header {
			return block.RawDataLeaf(ls, l.BlockMaker)
		}
	}

	return l, initErrs
}

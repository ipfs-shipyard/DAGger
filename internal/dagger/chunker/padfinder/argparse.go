package padfinder

import (
	"fmt"

	"github.com/ipfs-shipyard/DAGger/chunker"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"

	"github.com/ipfs-shipyard/DAGger/internal/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

func NewChunker(
	args []string,
	dgrCfg *dgrchunker.DaggerConfig,
) (
	_ chunker.Chunker,
	_ dgrchunker.InstanceConstants,
	initErrs []string,
) {

	c := padfinderPreChunker{}

	optSet := getopt.New()
	if err := options.RegisterSet("", &c.config, optSet); err != nil {
		initErrs = []string{fmt.Sprintf("option set registration failed: %s", err)}
		return
	}

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = util.SubHelp(
			"PAD FIXME",
			optSet,
		)
		return
	}

	// bail early if getopt fails
	if err := optSet.Getopt(args, nil); err != nil {
		initErrs = []string{err.Error()}
		return
	}

	// if c.Re2Longest == "" {
	// 	// FIXME - some day support more options but this one
	// 	initErrs = append(initErrs, "one of the available match options must be used")
	// } else {
	// 	var err error
	// 	if c.re2, err = regexp.Compile(c.Re2Longest); err != nil {
	// 		initErrs = append(initErrs, fmt.Sprintf(
	// 			"compilation of\n%s\n\tfailed: %s",
	// 			c.Re2Longest,
	// 			err,
	// 		))
	// 	} else {
	// 		c.re2.Longest()
	// 	}
	// }

	c.maxChunkSize = constants.MaxLeafPayloadSize

	// this is a pre-chunker, any length goes
	return &c, dgrchunker.InstanceConstants{MinChunkSize: 0}, initErrs
}

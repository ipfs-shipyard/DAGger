package padfinder

import (
	"fmt"

	"github.com/ipfs-shipyard/DAGger/chunker"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"

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

	optSet.FlagLong(&c.freeFormRE, freeformOptName, 0, freeformOptDesc, "regex")

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
	if initErrs = util.ArgParse(args, optSet); len(initErrs) > 0 {
		return
	}

	c.padMeta = chunker.ChunkMeta{
		"no-subchunking": true,
		"is-padding":     true,
	}
	var regexes []string

	if c.freeFormRE != "" {
		regexes = append(regexes, c.freeFormRE)
	}
	if c.StaticPadHex != "" {

		if len(c.StaticPadHex)%2 == 1 {
			initErrs = append(initErrs, "odd length hex string")

		} else {
			c.padMeta["padding-cluster-atom-hex"] = c.StaticPadHex

			esc := []byte("\\x")
			hex := []byte(c.StaticPadHex)
			inner := make([]byte, 0, len(hex)*2)
			for i := 0; i < len(hex); i += 2 {
				inner = append(inner, esc...)
				inner = append(inner, hex[i:i+2]...)
			}
			regexes = append(regexes, fmt.Sprintf(
				"(?:%s){%d,}",
				inner,
				c.StaticMinRepeats,
			))
		}
	}

	if len(regexes) != 1 {
		initErrs = append(initErrs, "one and only one valid pad- option must be supplied")
		return
	}

	var err error
	if c.finder, err = compileRegex(regexes[0]); err != nil {
		initErrs = append(initErrs, fmt.Sprintf(
			"compilation of\n%s\n\tfailed: %s",
			regexes[0],
			err,
		))
		return
	}

	return &c, dgrchunker.InstanceConstants{
		MaxChunkSize: c.MaxRunSize,
		MinChunkSize: 0, // this is a pre-chunker: any length goes
	}, initErrs
}

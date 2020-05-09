package fixedsize

import (
	"fmt"
	"strconv"

	"github.com/ipfs-shipyard/DAGger/chunker"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
)

func NewChunker(
	args []string,
	dgrCfg *dgrchunker.DaggerConfig,
) (
	_ chunker.Chunker,
	_ dgrchunker.InstanceConstants,
	initErrs []string,
) {

	// on nil-args the "error" is the help text to be incorporated into
	// the larger help display
	if args == nil {
		initErrs = util.SubHelp(
			"Splits buffer into equally sized chunks. Requires a single parameter: the\n"+
				"size of each chunk in bytes (IPFS default: 262144)\n",
			nil,
		)
		return
	}

	c := fixedSizeChunker{}

	if len(args) != 2 {
		initErrs = append(initErrs, "chunker requires an integer argument, the size of each chunk in bytes")
	} else {
		sizearg, err := strconv.ParseUint(
			args[1][2:], // stripping off '--'
			10,
			25, // 25bits == 32 * 1024 * 1024 == 32MiB
		)
		if err != nil {
			initErrs = append(initErrs, fmt.Sprintf("argument parse failed: %s", err))
		} else {
			c.size = int(sizearg)
		}
	}

	if c.size > dgrCfg.GlobalMaxChunkSize {
		initErrs = append(initErrs, fmt.Sprintf(
			"provided chunk size '%s' exceeds specified maximum payload size '%s",
			util.Commify(c.size),
			util.Commify(dgrCfg.GlobalMaxChunkSize),
		))
	}

	return &c, dgrchunker.InstanceConstants{MinChunkSize: c.size}, initErrs
}

package dagger

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/ipfs/go-qringbuf"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"

	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker/buzhash"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker/fixedsize"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/chunker/rabin"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/linker"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/linker/ipfs/balanced"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/linker/ipfs/trickle"
)

var availableChunkers = map[string]chunker.Initializer{
	"fixed-size": fixedsize.NewChunker,
	"buzhash":    buzhash.NewChunker,
	"rabin":      rabin.NewChunker,
}
var availableLinkers = map[string]linker.Initializer{
	"ipfs-balanced": balanced.NewLinker,
	"ipfs-trickle":  trickle.NewLinker,
	"none":          linker.NewNulLinker,
}

const (
	emNone                = "none"
	emStatsText           = "stats-text"
	emStatsJsonl          = "stats-jsonl"
	emRootsJsonl          = "roots-jsonl"
	emChunksJsonl         = "chunks-jsonl"
	emCarV0Fifos          = "carV0-fifos"
	emCarV0RootlessStream = "carV0-rootless-stream"
)

// where the CLI initial error messages go
var argParseErrOut = os.Stderr

type blockPostProcessResult struct {
}

// both options and "context" in one go
type config struct {
	// unexported state members are not visible to the getopt auto-generator
	chainedChunkers     []chunker.Chunker
	chainedLinkers      []linker.Linker
	qrb                 *qringbuf.QuantizedRingBuffer
	uniqueBlockCallback func(hdr *block.Header) blockPostProcessResult
	asyncWG             sync.WaitGroup
	stats               runStats

	// where to output
	emitters map[string]io.WriteCloser

	// speederization shortcut flags for internal logic
	generateRoots bool
	emitChunks    bool

	//
	// Bulk of CLI options definition starts here, the rest further down in setupArgvParser()
	//

	Help             bool `getopt:"-h --help             Display basic help"`
	HelpAll          bool `getopt:"--help-all            Display help including options of individual plugins"`
	MultipartStream  bool `getopt:"--multipart           Expect multiple SInt64BE-size-prefixed streams on stdIN"`
	ProcessNulInputs bool `getopt:"--process-nul-inputs  Instead of skipping zero-length streams, emit a proper zero-length CID based on other settings"`
	AsyncHashing     bool `getopt:"--async-hashing       Spawn a short-lived goroutine for each hash operation. Disable for predictable benchmarking. Default:"`

	emittersStdErr []string // Emitter spec: option/helptext in setupArgvParser
	emittersStdOut []string // Emitter spec: option/helptext in setupArgvParser

	// 34 bytes is the largest identity CID that fits in 63 chars (dns limit) of b-prefixed base32 encoding
	InlineMaxSize      int `getopt:"--inline-max-size          Use identity-CID to refer to blocks having on-wire size at or below the specified value (34 is recommended), 0 disables"`
	GlobalMaxChunkSize int `getopt:"--max-chunk-size           Maximum data chunk size, same as maximum amount of payload in a leaf datablock. Default:"`
	RingBufferSize     int `getopt:"--ring-buffer-size         The size of the quantized ring buffer used for ingestion. Default:"`
	RingBufferSectSize int `getopt:"--ring-buffer-sync-size    (EXPERT SETTING) The size of each buffer synchronization sector. Default:"` // option vaguely named 'sync' to not confuse users
	RingBufferMinRead  int `getopt:"--ring-buffer-min-sysread  (EXPERT SETTING) Perform next read(2) only when the specified amount of free space is available in the buffer. Default:"`

	HashBits int    `getopt:"--hash-bits Amount of bits taken from *start* of the hash output"`
	hashAlg  string // hash algorithm to use: option/helptext in setupArgvParser

	requestedChunkers string // Chunker chain: option/helptext in setupArgvParser
	requestedLinkers  string // Linker chain: option/helptext in setupArgvParser

	IpfsCompatCmd string `getopt:"--ipfs-add-compatible-command A complete go-ipfs/js-ipfs add command serving as a basis config (any conflicting option will take precedence)"`
}

func ParseOpts(argv []string) (*config, util.PanicfWrapper) {

	// Some minimal non-controversial defaults, all overridable
	// *NEVER* have defaults that influence the resulting CIDs
	cfg := &config{
		GlobalMaxChunkSize: 1024 * 1024,
		HashBits:           256,
		AsyncHashing:       true,

		// RingBufferSize: int(2*constants.HardMaxBlockSize) + 128*1024, // bare-minimum with defaults
		RingBufferSize: 32 * 1024 * 1024,

		//SANCHECK: these numbers have not been validated
		RingBufferMinRead:  128 * 1024,
		RingBufferSectSize: 512 * 1024,

		emittersStdOut:   []string{emRootsJsonl},
		emittersStdErr:   []string{emStatsText},
		requestedLinkers: "none",

		// not defaults but rather the list of known/configured emitters
		emitters: map[string]io.WriteCloser{
			emNone:        nil,
			emStatsText:   nil,
			emStatsJsonl:  nil,
			emRootsJsonl:  nil,
			emChunksJsonl: nil,
			// emCarV0Fifos:          nil,
			// emCarV0RootlessStream: nil,
		},
	}

	// constant used in JSON output
	cfg.stats.summary.Event = "summary"

	cfg.stats.summary.SysStats.ArgvInitial = make([]string, len(argv)-1)
	copy(cfg.stats.summary.SysStats.ArgvInitial, argv[1:])

	optSet := makeArgvParser(cfg)
	optSet.Parse(argv)
	if cfg.Help || cfg.HelpAll {
		printUsage(optSet, cfg)
		os.Exit(0)
	}

	// accumulator for multiple erros, to present to the user all at once
	var argErrs []string

	unexpectedArgs := optSet.Args()
	if len(unexpectedArgs) != 0 {
		argErrs = append(argErrs, fmt.Sprintf(
			"Program does not take free-form arguments: '%s ...'",
			unexpectedArgs[0],
		))
	}

	// pre-populate from a compat `ipfs add` command if one was supplied
	if optSet.IsSet("ipfs-add-compatible-command") {
		if errStrings := presetFromIPFS(cfg, optSet); len(errStrings) > 0 {
			argErrs = append(argErrs, errStrings...)
		}
	}

	// has a default
	if cfg.GlobalMaxChunkSize < 1 || cfg.GlobalMaxChunkSize > int(constants.HardMaxPayloadSize) {
		argErrs = append(argErrs, fmt.Sprintf(
			"--max-chunk-size '%s' out of bounds [1:%d]",
			util.Commify(cfg.GlobalMaxChunkSize),
			constants.HardMaxPayloadSize,
		))
	}

	// has a default
	if cfg.HashBits < 128 || (cfg.HashBits%8) != 0 {
		argErrs = append(argErrs, "The value of --hash-bits must be a minimum of 128 and be divisible by 8")
	}

	if !optSet.IsSet("inline-max-size") &&
		!optSet.IsSet("ipfs-add-compatible-command") &&
		cfg.requestedLinkers != "none" {
		argErrs = append(argErrs, "You must specify a valid value for --inline-max-size")
	} else if cfg.InlineMaxSize < 0 ||
		(cfg.InlineMaxSize > 0 && cfg.InlineMaxSize < 4) ||
		cfg.InlineMaxSize > cfg.GlobalMaxChunkSize {
		argErrs = append(argErrs, fmt.Sprintf(
			"--inline-max-size '%s' out of bounds 0 or [4:%d]",
			util.Commify(cfg.InlineMaxSize),
			cfg.GlobalMaxChunkSize,
		))
	}

	var blockMaker block.Maker

	if _, exists := block.AvailableHashers[cfg.hashAlg]; !exists {
		argErrs = append(argErrs, fmt.Sprintf(
			"You must specify a valid hashing algorithm via '--hash=algname'. Available hash algorithms are %s",
			util.AvailableMapKeys(block.AvailableHashers),
		))
	} else {

		var errStr string
		blockMaker, errStr = block.MakerFromConfig(
			cfg.hashAlg,
			cfg.HashBits/8,
			cfg.InlineMaxSize,
			cfg.AsyncHashing,
		)
		if errStr != "" {
			argErrs = append(argErrs, errStr)
		}
	}

	if errorStrings := parseEmitterSpecs(cfg); len(errorStrings) > 0 {
		argErrs = append(argErrs, errorStrings...)
	} else {
		// setup block-aggregation callback if needed
		for _, needsAggregation := range []string{
			emStatsText,
			emStatsJsonl,
			emCarV0Fifos,
			emCarV0RootlessStream,
		} {
			if cfg.emitters[needsAggregation] != nil {
				cfg.stats.seenBlocks = make(seenBlocks, 1024) // SANCHECK: somewhat arbitrary, but eh...
				cfg.stats.seenRoots = make(seenRoots, 32)
				break
			}
		}

		if cfg.emitters[emCarV0Fifos] != nil || cfg.emitters[emCarV0RootlessStream] != nil {
			cfg.uniqueBlockCallback = func(hdr *block.Header) blockPostProcessResult {
				util.InternalPanicf("car writing not yet implemented") // FIXME
				return blockPostProcessResult{}
			}
		}
	}

	if cfg.requestedChunkers == "" {
		argErrs = append(argErrs,
			"You must specify at least one stream chunker via '--chunkers=algname1:opt1:opt2::algname2:...'. Available chunker names are: "+
				util.AvailableMapKeys(availableChunkers),
		)
	} else if errorStrings := setupChunkerChain(cfg); len(errorStrings) > 0 {
		argErrs = append(argErrs, errorStrings...)
	}

	if optSet.IsSet("linkers") && cfg.requestedLinkers == "" {
		argErrs = append(argErrs,
			"When specified linker chain must be in the form '--linkers=algname1:opt1:opt2::algname2:...'. Available linker names are: "+
				util.AvailableMapKeys(availableLinkers),
		)
	} else if errorStrings := setupLinkerChain(cfg, blockMaker); len(errorStrings) > 0 {
		argErrs = append(argErrs, errorStrings...)
	}

	if len(argErrs) != 0 {

		fmt.Fprint(argParseErrOut, "\nFatal error parsing arguments:\n\n")
		printUsage(optSet, cfg)

		sort.Strings(argErrs)
		fmt.Fprintf(
			argParseErrOut,
			"Fatal error parsing arguments:\n\t%s\n",
			strings.Join(argErrs, "\n\t"),
		)
		os.Exit(1)
	}

	// Opts are good - populate what we ended up with
	optSet.VisitAll(func(o getopt.Option) {
		switch o.LongName() {
		case "help", "help-all", "ipfs-add-compatible-command":
			// do nothing for these
		default:
			cfg.stats.summary.SysStats.ArgvExpanded = append(
				cfg.stats.summary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
					o.LongName(),
					o.Value().String(),
				),
			)
		}
	})
	sort.Strings(cfg.stats.summary.SysStats.ArgvExpanded)

	// panic with better stream-position context ( where possible )
	return cfg, func(f string, a ...interface{}) {
		prefix := fmt.Sprintf("INTERNAL ERROR\tstream:%d\tpos:%d\n======================\n",
			cfg.stats.totalStreams, cfg.stats.curStreamOffset,
		)
		panic(fmt.Sprintf(prefix+f, a...))
	}
}

func printUsage(optSet *getopt.Set, cfg *config) {
	optSet.PrintUsage(argParseErrOut)
	if cfg.HelpAll {
		printPluginUsage(argParseErrOut)
	} else {
		fmt.Fprint(argParseErrOut, "\nTry --help-all for more info\n\n")
	}
}

func printPluginUsage(out io.Writer) {

	chunkerNames := make([]string, 0, len(availableChunkers))
	for name, initializer := range availableChunkers {
		if initializer != nil {
			chunkerNames = append(chunkerNames, name)
		}
	}
	if len(chunkerNames) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(chunkerNames)
		for _, name := range chunkerNames {
			fmt.Fprintf(
				out,
				"[C]hunker '%s'\n",
				name,
			)
			_, h := availableChunkers[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	linkerNames := make([]string, 0, len(availableLinkers))
	for name, initializer := range availableLinkers {
		if initializer != nil {
			linkerNames = append(linkerNames, name)
		}
	}
	if len(linkerNames) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(linkerNames)
		for _, name := range linkerNames {
			fmt.Fprintf(
				out,
				"[L]inker '%s'\n",
				name,
			)
			_, h := availableLinkers[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}

		}
	}

	fmt.Fprint(out, "\n")
}

func makeArgvParser(cfg *config) (optSet *getopt.Set) {
	// The default documented way of using pborman/options is to muck with globals
	// Operate over objects instead, allowing us to re-parse argv multiple times
	optSet = getopt.New()
	if err := options.RegisterSet("", cfg, optSet); err != nil {
		log.Fatalf("Option set registration failed: %s", err)
	}

	// program does not take freeform args
	// need to override this for sensible help render
	optSet.SetParameters("")

	// Several options have the help assembled programmatically
	optSet.FlagLong(&cfg.hashAlg, "hash", 0, "Hash algorithm to use, one of: "+util.AvailableMapKeys(block.AvailableHashers))
	optSet.FlagLong(&cfg.requestedChunkers, "chunkers", 0,
		"Stream chunking algorithm chain. Each chunker is one of: "+util.AvailableMapKeys(availableChunkers),
		"'ch1:o1.1:o1.2:...:o1.N::ch2:o2.1:o2.2:...:o2.N::ch3...'",
	)
	optSet.FlagLong(&cfg.requestedLinkers, "linkers", 0,
		"Block linking algorithm chain. Each linker is one of: "+util.AvailableMapKeys(availableLinkers),
		"'ln1:o1.1:o1.2:...:o1.N::ln.2...'",
	)
	optSet.FlagLong(&cfg.emittersStdErr, "emit-stderr", 0, fmt.Sprintf(
		"One or more emitters to activate on stdERR. Available emitters are %s. Default: ",
		util.AvailableMapKeys(cfg.emitters),
	), "commaSepEmitters")
	optSet.FlagLong(&cfg.emittersStdOut, "emit-stdout", 0,
		"One or more emitters to activate on stdOUT. Available emitters same as above. Default: ",
		"commaSepEmitters",
	)

	return optSet
}

func parseEmitterSpecs(cfg *config) (argErrs []string) {
	activeStderr := make(map[string]bool, len(cfg.emittersStdErr))
	for _, s := range cfg.emittersStdErr {
		activeStderr[s] = true
		if val, exists := cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("Invalid emitter '%s' specified with --emit-stderr", s))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			cfg.emitters[s] = os.Stderr
		}
	}
	activeStdout := make(map[string]bool, len(cfg.emittersStdOut))
	for _, s := range cfg.emittersStdOut {
		activeStdout[s] = true
		if val, exists := cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("Invalid emitter '%s' specified for --emit-stdout", s))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			cfg.emitters[s] = os.Stdout
		}
	}

	for _, exclusiveEmitter := range []string{
		emNone,
		emStatsText,
		emCarV0Fifos,
		emCarV0RootlessStream,
	} {
		if activeStderr[exclusiveEmitter] && len(activeStderr) > 1 {
			argErrs = append(argErrs, fmt.Sprintf(
				"When specified, emitter '%s' must be the sole argument to --emit-stderr",
				exclusiveEmitter,
			))
		}
		if activeStdout[exclusiveEmitter] && len(activeStdout) > 1 {
			argErrs = append(argErrs, fmt.Sprintf(
				"When specified, emitter '%s' must be the sole argument to --emit-stdout",
				exclusiveEmitter,
			))
		}
	}

	// set shortcut defaults based on emitters
	cfg.emitChunks = (cfg.emitters[emChunksJsonl] != nil)
	cfg.generateRoots = (cfg.emitters[emRootsJsonl] != nil || cfg.emitters[emStatsJsonl] != nil)

	return argErrs
}

func setupChunkerChain(cfg *config) (argErrs []string) {
	commonCfg := chunker.CommonConfig{
		GlobalMaxChunkSize: cfg.GlobalMaxChunkSize,
		InternalPanicf:     util.InternalPanicf,
	}

	individualChunkers := strings.Split(cfg.requestedChunkers, "::")

	for _, chunkerCmd := range individualChunkers {
		chunkerArgs := strings.Split(chunkerCmd, ":")
		init, exists := availableChunkers[chunkerArgs[0]]
		if !exists {
			argErrs = append(argErrs, fmt.Sprintf(
				"Chunker '%s' not found. Available chunker names are: %s",
				chunkerArgs[0],
				util.AvailableMapKeys(availableChunkers),
			))
			continue
		}

		for n := range chunkerArgs {
			if n > 0 {
				chunkerArgs[n] = "--" + chunkerArgs[n]
			}
		}

		if chunkerInstance, initErrors := init(
			chunkerArgs,
			&commonCfg,
		); len(initErrors) > 0 {
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of chunker '%s' failed: %s",
					chunkerArgs[0],
					e,
				))
			}
		} else {
			cfg.chainedChunkers = append(cfg.chainedChunkers, chunkerInstance)
		}
	}

	return argErrs
}

func setupLinkerChain(cfg *config, bm block.Maker) (argErrs []string) {
	commonCfg := linker.CommonConfig{
		InternalPanicf:     util.InternalPanicf,
		GlobalMaxBlockSize: int(constants.HardMaxBlockSize),
		BlockMaker:         bm,
		HasherName:         cfg.hashAlg,
		HasherBits:         cfg.HashBits,
	}

	individualLinkers := strings.Split(cfg.requestedLinkers, "::")

	cfg.chainedLinkers = make([]linker.Linker, len(individualLinkers))
	// we need to process the linkers in reverse, in order to populate NextLinker
	for linkerNum := len(individualLinkers) - 1; linkerNum >= 0; linkerNum-- {

		linkerCmd := individualLinkers[linkerNum]

		linkerArgs := strings.Split(linkerCmd, ":")
		init, exists := availableLinkers[linkerArgs[0]]
		if !exists {
			argErrs = append(argErrs, fmt.Sprintf(
				"Linker '%s' not found. Available linker names are: %s",
				linkerArgs[0],
				util.AvailableMapKeys(availableLinkers),
			))
			continue
		}

		for n := range linkerArgs {
			if n > 0 {
				linkerArgs[n] = "--" + linkerArgs[n]
			}
		}

		generatorIdx := len(individualLinkers) - linkerNum
		linkerCfg := commonCfg // SHALLOW COPY!!!
		// every linker gets a callback with their own index closed over
		linkerCfg.NewLinkBlockCallback = func(hdr *block.Header, linkerLayer int, links []*block.Header) {
			cfg.asyncWG.Add(1)
			go registerNewBlock(
				cfg,
				hdr,
				generatedBy{generatorIdx, linkerLayer},
				links,
				nil, // a linkblock has no data, for now at least
			)
		}

		if linkerNum != len(individualLinkers)-1 {
			linkerCfg.NextLinker = cfg.chainedLinkers[linkerNum+1]
		}

		if linkerInstance, initErrors := init(
			linkerArgs,
			&linkerCfg,
		); len(initErrors) > 0 {

			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of linker '%s' failed: %s",
					linkerArgs[0],
					e,
				))
			}
		} else {
			cfg.chainedLinkers[linkerNum] = linkerInstance
		}
	}

	return argErrs
}

type compatIpfsArgs struct {
	CidVersion    int    `getopt:"--cid-version"`
	InlineActive  bool   `getopt:"--inline"`
	InlineLimit   int    `getopt:"--inline-limit"`
	UseRawLeaves  bool   `getopt:"--raw-leaves"`
	UpgradeV0CID  bool   `getopt:"--upgrade-cidv0-in-output"`
	TrickleLinker bool   `getopt:"--trickle"`
	Chunker       string `getopt:"--chunker"`
}

func presetFromIPFS(cfg *config, mainOptSet *getopt.Set) (parseErrors []string) {

	lVals, optSet := options.RegisterNew("", &compatIpfsArgs{
		Chunker:    "size",
		CidVersion: 0,
	})
	ipfsOpts := lVals.(*compatIpfsArgs)

	args := append([]string{"ipfs-compat"}, strings.Split(cfg.IpfsCompatCmd, " ")...)
	for {
		if err := optSet.Getopt(args, nil); err != nil {
			parseErrors = append(parseErrors, err.Error())
		}
		args = optSet.Args()
		if len(args) == 0 {
			break
		} else if args[0] != "" && args[0] != "add" { // next iteration will eat the chaff as a "progname"
			parseErrors = append(parseErrors, fmt.Sprintf(
				"unexpected ipfs-compatible parameter(s): %s...",
				args[0],
			))
		}
	}

	// bail early if errors present already
	if len(parseErrors) > 0 {
		return parseErrors
	}

	if !mainOptSet.IsSet("hash") {
		cfg.hashAlg = "sha2-256"
	}

	if !mainOptSet.IsSet("inline-max-size") {
		if ipfsOpts.InlineActive {
			if optSet.IsSet("inline-limit") {
				cfg.InlineMaxSize = ipfsOpts.InlineLimit
			} else {
				cfg.InlineMaxSize = 32
			}
		} else {
			cfg.InlineMaxSize = 0
		}
	}

	// ignore everything compat if a linker is already given
	if !mainOptSet.IsSet("linkers") {

		var linkerOpts []string

		// either trickle or balanced, go-ipfs doesn't understand much else
		if ipfsOpts.TrickleLinker {
			linkerOpts = append(linkerOpts, "ipfs-trickle")
			// mandatory defaults
			linkerOpts = append(linkerOpts, "v0")
			linkerOpts = append(linkerOpts, "max-direct-leaves=174")
			linkerOpts = append(linkerOpts, "max-sibling-subgroups=4")
		} else {
			linkerOpts = append(linkerOpts, "ipfs-balanced")
			// mandatory defaults
			linkerOpts = append(linkerOpts, "v0")
			linkerOpts = append(linkerOpts, "max-children=174")
		}

		if ipfsOpts.CidVersion != 1 {

			if ipfsOpts.UpgradeV0CID && ipfsOpts.CidVersion == 0 {

				linkerOpts = append(linkerOpts, "cidv0")

				if cfg.hashAlg != "sha2-256" || cfg.HashBits != 256 {
					parseErrors = append(
						parseErrors,
						"Legacy --upgrade-cidv0-in-output requires --hash=sha2-256 and --hash-bits=256",
					)
				}
			} else {
				parseErrors = append(
					parseErrors,
					fmt.Sprintf("--cid-version=%d is unsupported ( try --cid-version=1 or --upgrade-cidv0-in-output )", ipfsOpts.CidVersion),
				)
			}
		} else if !optSet.IsSet("raw-leaves") {
			ipfsOpts.UseRawLeaves = true
		}

		if !ipfsOpts.UseRawLeaves {
			linkerOpts = append(linkerOpts, "unixfs-leaves")
		}

		cfg.requestedLinkers = strings.Join(linkerOpts, ":")
	}

	// ignore everything compat if a chunker is already given
	if !mainOptSet.IsSet("chunkers") {

		if strings.HasPrefix(ipfsOpts.Chunker, "size") {
			sizeopts := strings.Split(ipfsOpts.Chunker, "-")
			if sizeopts[0] == "size" {
				if len(sizeopts) == 1 {
					cfg.requestedChunkers = "fixed-size:262144"
				} else if len(sizeopts) == 2 {
					cfg.requestedChunkers = "fixed-size:" + sizeopts[1]
				}
			}
		} else if strings.HasPrefix(ipfsOpts.Chunker, "rabin") {
			rabinopts := strings.Split(ipfsOpts.Chunker, "-")
			if rabinopts[0] == "rabin" {

				var bits, min, max string

				if len(rabinopts) == 1 {
					bits = "18"
					min = "87381"  // (2**18)/3
					max = "393216" // (2**18)+(2**18)/2

				} else if len(rabinopts) == 2 {
					if avg, err := strconv.ParseUint(rabinopts[1], 10, 64); err == nil {
						bits = fmt.Sprintf("%d", int(math.Log2(float64(avg))))
						min = fmt.Sprintf("%d", avg/3)
						max = fmt.Sprintf("%d", avg+avg/2)
					}

				} else if len(rabinopts) == 4 {
					if avg, err := strconv.ParseUint(rabinopts[2], 10, 64); err == nil {
						bits = fmt.Sprintf("%d", int(math.Log2(float64(avg))))
						min = rabinopts[1]
						max = rabinopts[3]
					}
				}

				if bits != "" {
					cfg.requestedChunkers = fmt.Sprintf("rabin:rabin-preset=GoIPFSv0:state-target=0:state-mask-bits=%s:min-size=%s:max-size=%s",
						bits,
						min,
						max,
					)
				}
			}
		} else if strings.HasPrefix(ipfsOpts.Chunker, "buzhash") {
			buzopts := strings.Split(ipfsOpts.Chunker, "-")
			if buzopts[0] == "buzhash" {
				if len(buzopts) == 1 {
					cfg.requestedChunkers = "buzhash:hash-table=GoIPFSv0:state-target=0:state-mask-bits=17:min-size=131072:max-size=524288"
				}
			}
		}

		if cfg.requestedChunkers == "" {
			parseErrors = append(
				parseErrors,
				fmt.Sprintf("Invalid ipfs-compatible spec --chunker=%s", ipfsOpts.Chunker),
			)
		}
	}

	return parseErrors
}

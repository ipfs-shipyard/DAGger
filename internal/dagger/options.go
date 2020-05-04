package dagger

import (
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"

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
	"github.com/ipfs-shipyard/DAGger/internal/dagger/linker/ipfs/fixedoutdegree"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/linker/ipfs/trickle"
)

var availableChunkers = map[string]chunker.Initializer{
	"fixed-size": fixedsize.NewChunker,
	"buzhash":    buzhash.NewChunker,
	"rabin":      rabin.NewChunker,
}
var availableLinkers = map[string]linker.Initializer{
	"ipfs-fixed-outdegree": fixedoutdegree.NewLinker,
	"ipfs-trickle":         trickle.NewLinker,
	"none":                 linker.NewNulLinker,
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

type config struct {
	optSet *getopt.Set

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

	emittersStdErr []string // Emitter spec: option/helptext in setupArgvParser
	emittersStdOut []string // Emitter spec: option/helptext in setupArgvParser

	// no-option-attached, parsing error accumulators
	erroredChunkers []string
	erroredLinkers  []string

	// 34 bytes is the largest identity CID that fits in 63 chars (dns limit) of b-prefixed base32 encoding
	InlineMaxSize      int `getopt:"--inline-max-size          Use identity-CID to refer to blocks having on-wire size at or below the specified value (34 is recommended), 0 disables"`
	GlobalMaxChunkSize int `getopt:"--max-chunk-size           Maximum data chunk size, same as maximum amount of payload in a leaf datablock. Default:"`
	AsyncHashers       int `getopt:"--async-hashers            Number of concurrent short-lived goroutines performing hashing. Set to 0 (disable) for predictable benchmarking. Default:"`
	RingBufferSize     int `getopt:"--ring-buffer-size         The size of the quantized ring buffer used for ingestion. Default:"`
	RingBufferSectSize int `getopt:"--ring-buffer-sync-size    (EXPERT SETTING) The size of each buffer synchronization sector. Default:"` // option vaguely named 'sync' to not confuse users
	RingBufferMinRead  int `getopt:"--ring-buffer-min-sysread  (EXPERT SETTING) Perform next read(2) only when the specified amount of free space is available in the buffer. Default:"`

	StatsEnabled int `getopt:"--stats-enabled  An integer representing enabled stat aggregations: bit0:Blocks, bit1:RingbufferTiming. Default:"`

	HashBits int    `getopt:"--hash-bits Amount of bits taken from *start* of the hash output"`
	hashAlg  string // hash algorithm to use: option/helptext in setupArgvParser

	requestedChunkers string // Chunker chain: option/helptext in setupArgvParser
	requestedLinkers  string // Linker chain: option/helptext in setupArgvParser

	IpfsCompatCmd string `getopt:"--ipfs-add-compatible-command A complete go-ipfs/js-ipfs add command serving as a basis config (any conflicting option will take precedence)"`
}

const (
	statsBlocks = 1 << iota
	statsRingbuf
)

func NewFromArgv(argv []string) (*Dagger, util.PanicfWrapper) {

	// Some minimal non-controversial defaults, all overridable
	// Try really hard to *NOT* have defaults that influence resulting CIDs
	dgr := &Dagger{
		cfg: config{
			GlobalMaxChunkSize: 1024 * 1024,
			HashBits:           256,
			AsyncHashers:       runtime.NumCPU() * 4, // SANCHECK yes, this is high: seems the simd version knows what to do...

			StatsEnabled: statsBlocks,

			// RingBufferSize: 2*int(constants.HardMaxBlockSize) + 128*1024, // bare-minimum with defaults
			RingBufferSize: 24 * 1024 * 1024, // SANCHECK low seems good somehow... fits in L3 maybe?

			//SANCHECK: these numbers have not been validated
			RingBufferMinRead:  256 * 1024,
			RingBufferSectSize: 64 * 1024,

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
		},
	}

	// init some constants
	{
		s := &dgr.statSummary
		s.EventType = "summary"

		s.SysStats.ArgvInitial = make([]string, len(argv)-1)
		copy(s.SysStats.ArgvInitial, argv[1:])

		s.SysStats.NumCPU = runtime.NumCPU()
		s.SysStats.PageSize = os.Getpagesize()
		s.SysStats.GoVersion = runtime.Version()
	}

	cfg := &dgr.cfg
	cfg.initArgvParser()
	cfg.optSet.Parse(argv)
	if cfg.Help || cfg.HelpAll {
		cfg.printUsage()
		os.Exit(0)
	}

	// accumulator for multiple erros, to present to the user all at once
	var argErrs []string

	unexpectedArgs := cfg.optSet.Args()
	if len(unexpectedArgs) != 0 {
		argErrs = append(argErrs, fmt.Sprintf(
			"Program does not take free-form arguments: '%s ...'",
			unexpectedArgs[0],
		))
	}

	// pre-populate from a compat `ipfs add` command if one was supplied
	if cfg.optSet.IsSet("ipfs-add-compatible-command") {
		if errStrings := cfg.presetFromIPFS(); len(errStrings) > 0 {
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

	if !cfg.optSet.IsSet("inline-max-size") &&
		!cfg.optSet.IsSet("ipfs-add-compatible-command") &&
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

		if cfg.hashAlg == "none" && !cfg.optSet.IsSet("async-hashers") {
			cfg.AsyncHashers = 0
		}

		var errStr string
		blockMaker, dgr.asyncHasherBus, errStr = block.MakerFromConfig(
			cfg.hashAlg,
			cfg.HashBits/8,
			cfg.InlineMaxSize,
			cfg.AsyncHashers,
		)
		if errStr != "" {
			argErrs = append(argErrs, errStr)
		}
	}

	if errorStrings := cfg.parseEmitterSpecs(); len(errorStrings) > 0 {
		argErrs = append(argErrs, errorStrings...)
	} else {
		// setup fifo callback if needed
		for _, carType := range []string{
			emCarV0Fifos,
			emCarV0RootlessStream,
		} {
			if cfg.emitters[carType] != nil {

				if (dgr.cfg.StatsEnabled & statsBlocks) != statsBlocks {
					argErrs = append(argErrs, fmt.Sprintf(
						"disabled blockstat collection required by emitter '%s'",
						carType,
					))
					break
				}

				dgr.uniqueBlockCallback = func(hdr *block.Header) blockPostProcessResult {
					util.InternalPanicf("car writing not yet implemented") // FIXME
					return blockPostProcessResult{}
				}
			}
		}
	}

	var errorMessages []string

	if cfg.requestedChunkers == "" {
		argErrs = append(argErrs,
			"You must specify at least one stream chunker via '--chunkers=algname1:opt1:opt2::algname2:...'. Available chunker names are: "+
				util.AvailableMapKeys(availableChunkers),
		)
	} else if errorMessages, cfg.erroredChunkers = dgr.setupChunkerChain(); len(errorMessages) > 0 {
		argErrs = append(argErrs, errorMessages...)
	}

	if cfg.optSet.IsSet("linkers") && cfg.requestedLinkers == "" {
		argErrs = append(argErrs,
			"When specified linker chain must be in the form '--linkers=algname1:opt1:opt2::algname2:...'. Available linker names are: "+
				util.AvailableMapKeys(availableLinkers),
		)
	} else if errorMessages, cfg.erroredLinkers = dgr.setupLinkerChain(blockMaker); len(errorMessages) > 0 {
		argErrs = append(argErrs, errorMessages...)
	}

	if len(argErrs) != 0 {
		fmt.Fprint(argParseErrOut, "\nFatal error parsing arguments:\n\n")
		cfg.printUsage()

		sort.Strings(argErrs)
		fmt.Fprintf(
			argParseErrOut,
			"Fatal error parsing arguments:\n\t%s\n",
			strings.Join(argErrs, "\n\t"),
		)
		os.Exit(1)
	}

	// Opts are good - populate what we ended up with
	cfg.optSet.VisitAll(func(o getopt.Option) {
		switch o.LongName() {
		case "help", "help-all", "ipfs-add-compatible-command":
			// do nothing for these
		default:
			dgr.statSummary.SysStats.ArgvExpanded = append(
				dgr.statSummary.SysStats.ArgvExpanded, fmt.Sprintf(`--%s=%s`,
					o.LongName(),
					o.Value().String(),
				),
			)
		}
	})
	sort.Strings(dgr.statSummary.SysStats.ArgvExpanded)

	// panic with better stream-position context ( where possible )
	return dgr, func(f string, a ...interface{}) {
		prefix := fmt.Sprintf("INTERNAL ERROR\tstream:%d\tpos:%d\n======================\n",
			dgr.statSummary.Streams, dgr.curStreamOffset,
		)
		panic(fmt.Sprintf(prefix+f, a...))
	}
}

func (cfg *config) printUsage() {
	cfg.optSet.PrintUsage(argParseErrOut)
	if cfg.HelpAll || len(cfg.erroredChunkers) > 0 || len(cfg.erroredLinkers) > 0 {
		printPluginUsage(
			argParseErrOut,
			cfg.erroredChunkers,
			cfg.erroredLinkers,
		)
	} else {
		fmt.Fprint(argParseErrOut, "\nTry --help-all for more info\n\n")
	}
}

func printPluginUsage(
	out io.Writer,
	listChunkers []string,
	listLinkers []string,
) {

	// if nothing was requested explicitly - list everything
	if len(listChunkers) == 0 && len(listLinkers) == 0 {
		for name, initializer := range availableChunkers {
			if initializer != nil {
				listChunkers = append(listChunkers, name)
			}
		}
		for name, initializer := range availableLinkers {
			if initializer != nil {
				listLinkers = append(listLinkers, name)
			}
		}
	}

	if len(listChunkers) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listChunkers)
		for _, name := range listChunkers {
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

	if len(listLinkers) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listLinkers)
		for _, name := range listLinkers {
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

func (cfg *config) initArgvParser() {
	// The default documented way of using pborman/options is to muck with globals
	// Operate over objects instead, allowing us to re-parse argv multiple times
	o := getopt.New()
	if err := options.RegisterSet("", cfg, o); err != nil {
		log.Fatalf("Option set registration failed: %s", err)
	}
	cfg.optSet = o

	// program does not take freeform args
	// need to override this for sensible help render
	o.SetParameters("")

	// Several options have the help assembled programmatically
	o.FlagLong(&cfg.hashAlg, "hash", 0, "Hash algorithm to use, one of: "+util.AvailableMapKeys(block.AvailableHashers))
	o.FlagLong(&cfg.requestedChunkers, "chunkers", 0,
		"Stream chunking algorithm chain. Each chunker is one of: "+util.AvailableMapKeys(availableChunkers),
		"'ch1:o1.1:o1.2:...:o1.N::ch2:o2.1:o2.2:...:o2.N::ch3...'",
	)
	o.FlagLong(&cfg.requestedLinkers, "linkers", 0,
		"Block linking algorithm chain. Each linker is one of: "+util.AvailableMapKeys(availableLinkers),
		"'ln1:o1.1:o1.2:...:o1.N::ln.2...'",
	)
	o.FlagLong(&cfg.emittersStdErr, "emit-stderr", 0, fmt.Sprintf(
		"One or more emitters to activate on stdERR. Available emitters are %s. Default: ",
		util.AvailableMapKeys(cfg.emitters),
	), "commaSepEmitters")
	o.FlagLong(&cfg.emittersStdOut, "emit-stdout", 0,
		"One or more emitters to activate on stdOUT. Available emitters same as above. Default: ",
		"commaSepEmitters",
	)
}

func (cfg *config) parseEmitterSpecs() (argErrs []string) {
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

func (dgr *Dagger) setupChunkerChain() (argErrs []string, initFailFor []string) {
	commonCfg := chunker.CommonConfig{
		GlobalMaxChunkSize: dgr.cfg.GlobalMaxChunkSize,
		InternalPanicf:     util.InternalPanicf,
	}

	individualChunkers := strings.Split(dgr.cfg.requestedChunkers, "::")

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

			initFailFor = append(initFailFor, chunkerArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of chunker '%s' failed: %s",
					chunkerArgs[0],
					e,
				))
			}
		} else {
			dgr.chainedChunkers = append(dgr.chainedChunkers, chunkerInstance)
		}
	}

	return argErrs, initFailFor
}

func (dgr *Dagger) setupLinkerChain(bm block.Maker) (argErrs []string, initFailFor []string) {
	commonCfg := linker.CommonConfig{
		InternalPanicf:     util.InternalPanicf,
		GlobalMaxBlockSize: int(constants.HardMaxBlockSize),
		BlockMaker:         bm,
		HasherName:         dgr.cfg.hashAlg,
		HasherBits:         dgr.cfg.HashBits,
	}

	individualLinkers := strings.Split(dgr.cfg.requestedLinkers, "::")

	dgr.chainedLinkers = make([]linker.Linker, len(individualLinkers))
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
			dgr.asyncWG.Add(1)
			go dgr.registerNewBlock(
				hdr,
				generatedBy{generatorIdx, linkerLayer},
				links,
				nil, // a linkblock has no data, for now at least
			)
		}

		if linkerNum != len(individualLinkers)-1 {
			linkerCfg.NextLinker = dgr.chainedLinkers[linkerNum+1]
		}

		if linkerInstance, initErrors := init(
			linkerArgs,
			&linkerCfg,
		); len(initErrors) > 0 {

			initFailFor = append(initFailFor, linkerArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of linker '%s' failed: %s",
					linkerArgs[0],
					e,
				))
			}
		} else {
			dgr.chainedLinkers[linkerNum] = linkerInstance
		}
	}

	return argErrs, initFailFor
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

func (cfg *config) presetFromIPFS() (parseErrors []string) {

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

	if !cfg.optSet.IsSet("hash") {
		cfg.hashAlg = "sha2-256"
	}

	if !cfg.optSet.IsSet("inline-max-size") {
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
	if !cfg.optSet.IsSet("linkers") {

		var linkerOpts []string

		// either trickle or fixed-outdegree, go-ipfs doesn't understand much else
		if ipfsOpts.TrickleLinker {
			linkerOpts = append(linkerOpts, "ipfs-trickle")
			// mandatory defaults
			linkerOpts = append(linkerOpts, "v0")
			linkerOpts = append(linkerOpts, "max-direct-leaves=174")
			linkerOpts = append(linkerOpts, "max-sibling-subgroups=4")
		} else {
			linkerOpts = append(linkerOpts, "ipfs-fixed-outdegree")
			// mandatory defaults
			linkerOpts = append(linkerOpts, "v0")
			linkerOpts = append(linkerOpts, "max-outdegree=174")
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
	if !cfg.optSet.IsSet("chunkers") {

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

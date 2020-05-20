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

	"github.com/ipfs-shipyard/DAGger/internal/constants"
	dgrblock "github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	dgrchunker "github.com/ipfs-shipyard/DAGger/internal/dagger/chunker"
	dgrcollector "github.com/ipfs-shipyard/DAGger/internal/dagger/collector"
	dgrencoder "github.com/ipfs-shipyard/DAGger/internal/dagger/encoder"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"
	getopt "github.com/pborman/getopt/v2"
	"github.com/pborman/options"
)

type config struct {
	optSet *getopt.Set

	// where to output
	emitters emissionTargets

	//
	// Bulk of CLI options definition starts here, the rest further down in initArgvParser()
	//

	Help             bool `getopt:"-h --help             Display basic help"`
	HelpAll          bool `getopt:"--help-all            Display full help including options for every currently supported chunker/collector/encoder"`
	MultipartStream  bool `getopt:"--multipart           Expect multiple SInt64BE-size-prefixed streams on stdIN"`
	ProcessNulInputs bool `getopt:"--process-nul-inputs  Instead of skipping zero-length streams, emit an IPFS-compatible zero-length CID based on current settings"`

	emittersStdErr []string // Emitter spec: option/helptext in initArgvParser()
	emittersStdOut []string // Emitter spec: option/helptext in initArgvParser()

	// no-option-attached, parsing error accumulators
	erroredChunkers     []string
	erroredCollectors   []string
	erroredNodeEncoders []string

	// 34 bytes is the largest identity CID that fits in 63 chars (dns limit) of b-prefixed base32 encoding
	InlineMaxSize      int `getopt:"--inline-max-size=bytes         Use identity-CID to refer to blocks having on-wire size at or below the specified value (34 is recommended), 0 disables"`
	AsyncHashers       int `getopt:"--async-hashers=integer         Number of concurrent short-lived goroutines performing hashing. Set to 0 (disable) for predictable benchmarking. Default:"`
	RingBufferSize     int `getopt:"--ring-buffer-size=bytes        The size of the quantized ring buffer used for ingestion. Default:"`
	RingBufferSectSize int `getopt:"--ring-buffer-sync-size=bytes   (EXPERT SETTING) The size of each buffer synchronization sector. Default:"` // option vaguely named 'sync' to not confuse users
	RingBufferMinRead  int `getopt:"--ring-buffer-min-sysread=bytes (EXPERT SETTING) Perform next read(2) only when the specified amount of free space is available in the buffer. Default:"`

	StatsActive uint `getopt:"--stats-active=uint   A bitfield representing activated stat aggregations: bit0:BlockSizing, bit1:RingbufferTiming. Default:"`

	HashBits int    `getopt:"--hash-bits=integer Amount of bits taken from *start* of the hash output. Default:"`
	hashFunc string // hash function to use: option/helptext in initArgvParser()

	requestedChunkers    string // Chunker chain: option/helptext in initArgvParser()
	requestedCollectors  string // Collector chain: option/helptext in initArgvParser()
	requestedNodeEncoder string // The global (for now) node=>block encoder: option/helptext in initArgvParser

	IpfsCompatCmd string `getopt:"--ipfs-add-compatible-command=cmdstring A complete go-ipfs/js-ipfs add command serving as a basis config (any conflicting option will take precedence)"`
}

const (
	statsBlocks = 1 << iota
	statsRingbuf
)

type emissionTargets map[string]io.Writer

const (
	emNone               = "none"
	emStatsText          = "stats-text"
	emStatsJsonl         = "stats-jsonl"
	emRootsJsonl         = "roots-jsonl"
	emChunksJsonl        = "chunks-jsonl"
	emCarV0Fifos         = "car-v0-fifos-xargs"
	emCarV0PinlessStream = "car-v0-pinless-stream"
)

// where the CLI initial error messages go
var argParseErrOut = os.Stderr

func NewFromArgv(argv []string) (dgr *Dagger) {

	dgr = &Dagger{
		// Some minimal non-controversial defaults, all overridable
		// Try really hard to *NOT* have defaults that influence resulting CIDs
		cfg: config{
			HashBits:     256,
			AsyncHashers: runtime.NumCPU() * 4, // SANCHECK yes, this is high: seems the simd version knows what to do...

			StatsActive: statsBlocks,

			// RingBufferSize: 2*constants.HardMaxPayloadSize + 256*1024, // bare-minimum with defaults
			RingBufferSize: 24 * 1024 * 1024, // SANCHECK low seems good somehow... fits in L3 maybe?

			//SANCHECK: these numbers have not been validated
			RingBufferMinRead:  256 * 1024,
			RingBufferSectSize: 64 * 1024,

			emittersStdOut: []string{emRootsJsonl},
			emittersStdErr: []string{emStatsText},

			// not defaults but rather the list of known/configured emitters
			emitters: emissionTargets{
				emNone:               nil,
				emStatsText:          nil,
				emStatsJsonl:         nil,
				emRootsJsonl:         nil,
				emChunksJsonl:        nil,
				emCarV0Fifos:         nil,
				emCarV0PinlessStream: nil,
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

	// accumulator for multiple errors, to present to the user all at once
	argParseErrs := util.ArgParse(argv, cfg.optSet)

	if cfg.Help || cfg.HelpAll {
		cfg.printUsage()
		os.Exit(0)
	}

	// pre-populate from a compat `ipfs add` command if one was supplied
	if cfg.optSet.IsSet("ipfs-add-compatible-command") {
		if errStrings := cfg.presetFromIPFS(); len(errStrings) > 0 {
			argParseErrs = append(argParseErrs, errStrings...)
		}
	}

	// "invisible" set of defaults (not printed during --help)
	if cfg.requestedCollectors == "" && !cfg.optSet.IsSet("collectors") {
		cfg.requestedCollectors = "none"
		if cfg.requestedNodeEncoder == "" && !cfg.optSet.IsSet("node-encoder") {
			cfg.requestedNodeEncoder = "unixfsv1"
		}
	}

	// has a default
	if cfg.HashBits < 128 || (cfg.HashBits%8) != 0 {
		argParseErrs = append(argParseErrs, "The value of --hash-bits must be a minimum of 128 and be divisible by 8")
	}

	if !cfg.optSet.IsSet("inline-max-size") &&
		!cfg.optSet.IsSet("ipfs-add-compatible-command") &&
		cfg.requestedCollectors != "none" {
		argParseErrs = append(argParseErrs, "You must specify a valid value for --inline-max-size")
	} else if cfg.InlineMaxSize < 0 ||
		(cfg.InlineMaxSize > 0 && cfg.InlineMaxSize < 4) ||
		cfg.InlineMaxSize > constants.MaxLeafPayloadSize {
		// https://github.com/multiformats/cid/issues/21
		argParseErrs = append(argParseErrs, fmt.Sprintf(
			"--inline-max-size '%s' out of bounds 0 or [4:%d]",
			util.Commify(cfg.InlineMaxSize),
			constants.MaxLeafPayloadSize,
		))
	}

	// Parses/creates the blockmaker/nodeencoder, to pass in turn to the collector chain
	// Not stored in the dgr object itself, to cut down on logic leaks
	nodeEnc, errorMessages := dgr.setupEncoding()
	argParseErrs = append(argParseErrs, errorMessages...)
	argParseErrs = append(argParseErrs, dgr.setupChunkerChain()...)
	argParseErrs = append(argParseErrs, dgr.setupCollectorChain(nodeEnc)...)
	argParseErrs = append(argParseErrs, dgr.setupEmitters()...)

	// Opts check out - set up the car emitter
	if len(argParseErrs) == 0 {
		argParseErrs = append(argParseErrs, dgr.setupCarWriting()...)
	}

	if len(argParseErrs) != 0 {
		fmt.Fprint(argParseErrOut, "\nFatal error parsing arguments:\n\n")
		cfg.printUsage()

		sort.Strings(argParseErrs)
		fmt.Fprintf(
			argParseErrOut,
			"Fatal error parsing arguments:\n\t%s\n",
			strings.Join(argParseErrs, "\n\t"),
		)
		os.Exit(1)
	}

	// Opts *still* check out - take a snapshot of what we ended up with
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

	return
}

func (cfg *config) printUsage() {
	cfg.optSet.PrintUsage(argParseErrOut)
	if cfg.HelpAll || len(cfg.erroredChunkers) > 0 || len(cfg.erroredCollectors) > 0 {
		printPluginUsage(
			argParseErrOut,
			cfg.erroredCollectors,
			cfg.erroredNodeEncoders,
			cfg.erroredChunkers,
		)
	} else {
		fmt.Fprint(argParseErrOut, "\nTry --help-all for more info\n\n")
	}
}

func printPluginUsage(
	out io.Writer,
	listCollectors []string,
	listNodeEncoders []string,
	listChunkers []string,
) {

	// if nothing was requested explicitly - list everything
	if len(listCollectors) == 0 && len(listNodeEncoders) == 0 && len(listChunkers) == 0 {
		for name, initializer := range availableCollectors {
			if initializer != nil {
				listCollectors = append(listCollectors, name)
			}
		}
		for name, initializer := range availableNodeEncoders {
			if initializer != nil {
				listNodeEncoders = append(listNodeEncoders, name)
			}
		}
		for name, initializer := range availableChunkers {
			if initializer != nil {
				listChunkers = append(listChunkers, name)
			}
		}
	}

	if len(listCollectors) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listCollectors)
		for _, name := range listCollectors {
			fmt.Fprintf(
				out,
				"[C]ollector '%s'\n",
				name,
			)
			_, h := availableCollectors[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
			}
		}
	}

	if len(listNodeEncoders) != 0 {
		fmt.Fprint(out, "\n")
		sort.Strings(listNodeEncoders)
		for _, name := range listNodeEncoders {
			fmt.Fprintf(
				out,
				"[N]odeEncoder '%s'\n",
				name,
			)
			_, h := availableNodeEncoders[name](nil, nil)
			if len(h) == 0 {
				fmt.Fprint(out, "  -- no helptext available --\n\n")
			} else {
				fmt.Fprintln(out, strings.Join(h, "\n"))
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
			_, _, h := availableChunkers[name](nil, nil)
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
		log.Fatalf("option set registration failed: %s", err)
	}
	cfg.optSet = o

	// program does not take freeform args
	// need to override this for sensible help render
	o.SetParameters("")

	// Several options have the help-text assembled programmatically
	o.FlagLong(&cfg.hashFunc, "hash", 0, "Hash function to use, one of: "+util.AvailableMapKeys(dgrblock.AvailableHashers),
		"string",
	)
	o.FlagLong(&cfg.requestedNodeEncoder, "node-encoder", 0, "The IPLD-ish node encoder to use, one of: "+util.AvailableMapKeys(availableNodeEncoders),
		"'encname_opt1_opt2_..._optN",
	)
	o.FlagLong(&cfg.requestedChunkers, "chunkers", 0,
		"Stream chunking algorithm chain. Each chunker is one of: "+util.AvailableMapKeys(availableChunkers),
		"'ch1_o1.1_o1.2_..._o1.N__ch2_o2.1_o2.2_..._o2.N__ch3_...'",
	)
	o.FlagLong(&cfg.requestedCollectors, "collectors", 0,
		"Node-forming algorithm chain. Each collector is one of: "+util.AvailableMapKeys(availableCollectors),
		"'co1_o1.1_o1.2_..._o1.N__co2_...'",
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

func (dgr *Dagger) setupEmitters() (argErrs []string) {

	activeStderr := make(map[string]bool, len(dgr.cfg.emittersStdErr))
	for _, s := range dgr.cfg.emittersStdErr {
		activeStderr[s] = true
		if val, exists := dgr.cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("invalid emitter '%s' specified for --emit-stderr. Available emitters are: %s",
				s,
				util.AvailableMapKeys(dgr.cfg.emitters),
			))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			dgr.cfg.emitters[s] = os.Stderr
		}
	}
	activeStdout := make(map[string]bool, len(dgr.cfg.emittersStdOut))
	for _, s := range dgr.cfg.emittersStdOut {
		activeStdout[s] = true
		if val, exists := dgr.cfg.emitters[s]; !exists {
			argErrs = append(argErrs, fmt.Sprintf("invalid emitter '%s' specified for --emit-stdout. Available emitters are: %s",
				s,
				util.AvailableMapKeys(dgr.cfg.emitters),
			))
		} else if s == emNone {
			continue
		} else if val != nil {
			argErrs = append(argErrs, fmt.Sprintf("Emitter '%s' specified more than once", s))
		} else {
			dgr.cfg.emitters[s] = os.Stdout
		}
	}

	for _, exclusiveEmitter := range []string{
		emNone,
		emStatsText,
		emCarV0Fifos,
		emCarV0PinlessStream,
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

	// set couple shortcuts based on emitter config
	dgr.emitChunks = (dgr.cfg.emitters[emChunksJsonl] != nil)
	dgr.generateRoots = (dgr.cfg.emitters[emRootsJsonl] != nil || dgr.cfg.emitters[emStatsJsonl] != nil)

	return
}

func (dgr *Dagger) setupCarWriting() (argErrs []string) {

	var carSelectedOut io.Writer

	// we already checked that only one is set
	if dgr.cfg.emitters[emCarV0Fifos] != nil {
		carSelectedOut = dgr.cfg.emitters[emCarV0Fifos]
	} else if dgr.cfg.emitters[emCarV0PinlessStream] != nil {
		carSelectedOut = dgr.cfg.emitters[emCarV0PinlessStream]
	} else {
		return
	}

	if (dgr.cfg.StatsActive & statsBlocks) != statsBlocks {
		argErrs = append(argErrs, "disabling blockstat collection conflicts with streaming .car data")
	}

	if util.IsTTY(carSelectedOut) {
		argErrs = append(argErrs, "output of .car streams to a TTY is prohibited")
	}

	if len(argErrs) > 0 {
		return
	}

	if dgr.cfg.emitters[emCarV0PinlessStream] != nil {
		dgr.carDataWriter = carSelectedOut

		if f, isFh := carSelectedOut.(*os.File); isFh {
			if s, err := f.Stat(); err != nil {
				log.Printf("Failed to stat() the car stream output: %s", err)
			} else {
				for _, opt := range util.WriteOptimizations {
					if err := opt.Action(f, s); err != nil && err != os.ErrInvalid {
						log.Printf("Failed to apply write optimization hint '%s' to car stream output: %s\n", opt.Name, err)
					}
				}
			}
		}

		return
	}

	// we are in fifo-land
	if err := dgr.initOptimizedCarFifos(); err != nil {
		argErrs = append(argErrs, err.Error())
	}

	return
}

// Parses/creates the blockmaker/nodeencoder, to pass in turn to the collector chain
// Not stored in the dgr object itself, to cut down on logic leaks
func (dgr *Dagger) setupEncoding() (nodeEnc dgrencoder.NodeEncoder, argErrs []string) {

	cfg := dgr.cfg

	var blockMaker dgrblock.Maker
	if _, exists := dgrblock.AvailableHashers[cfg.hashFunc]; !exists {

		argErrs = append(argErrs, fmt.Sprintf(
			"You must specify a valid hash function via '--hash=algname'. Available hash names are %s",
			util.AvailableMapKeys(dgrblock.AvailableHashers),
		))
	} else {

		if cfg.hashFunc == "none" && !cfg.optSet.IsSet("async-hashers") {
			cfg.AsyncHashers = 0
		}

		var errStr string
		blockMaker, dgr.asyncHashingBus, errStr = dgrblock.MakerFromConfig(
			cfg.hashFunc,
			cfg.HashBits/8,
			cfg.InlineMaxSize,
			cfg.AsyncHashers,
		)
		if errStr != "" {
			argErrs = append(argErrs, errStr)
		}
	}

	nodeEncArgs := strings.Split(cfg.requestedNodeEncoder, "_")
	if init, exists := availableNodeEncoders[nodeEncArgs[0]]; !exists {
		argErrs = append(argErrs, fmt.Sprintf(
			"Encoder '%s' not found. Available encoder names are: %s",
			nodeEncArgs[0],
			util.AvailableMapKeys(availableNodeEncoders),
		))
	} else {
		for n := range nodeEncArgs {
			if n > 0 {
				nodeEncArgs[n] = "--" + nodeEncArgs[n]
			}
		}

		var initErrors []string
		if nodeEnc, initErrors = init(
			nodeEncArgs,
			&dgrencoder.DaggerConfig{
				BlockMaker: blockMaker,
				HasherName: cfg.hashFunc,
				HasherBits: cfg.HashBits,
				NewLinkBlockCallback: func(origin dgrencoder.NodeOrigin, newLinkHdr *dgrblock.Header, linkedBlocks []*dgrblock.Header) {
					dgr.asyncWG.Add(1)
					go dgr.postProcessBlock(
						origin,
						newLinkHdr,
						nil, // a link-node has no data, for now at least
					)
				},
			},
		); len(initErrors) > 0 {
			cfg.erroredNodeEncoders = append(cfg.erroredNodeEncoders, nodeEncArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of node encoder '%s' failed: %s",
					nodeEncArgs[0],
					e,
				))
			}
		}
	}

	return
}

func (dgr *Dagger) setupChunkerChain() (argErrs []string) {

	// bail early
	if dgr.cfg.requestedChunkers == "" {
		return []string{
			"You must specify at least one stream chunker via '--chunkers=algname1_opt1_opt2__algname2_...'. Available chunker names are: " +
				util.AvailableMapKeys(availableChunkers),
		}
	}

	individualChunkers := strings.Split(dgr.cfg.requestedChunkers, "__")

	for chunkerNum, chunkerCmd := range individualChunkers {
		chunkerArgs := strings.Split(chunkerCmd, "_")
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

		chunkerInstance, chunkerConstants, initErrors := init(
			chunkerArgs,
			&dgrchunker.DaggerConfig{
				IsLastInChain: (chunkerNum == len(individualChunkers)-1),
			},
		)

		if len(initErrors) == 0 {
			if chunkerConstants.MaxChunkSize < 1 || chunkerConstants.MaxChunkSize > constants.MaxLeafPayloadSize {
				initErrors = append(initErrors, fmt.Sprintf(
					"returned MaxChunkSize constant '%d' out of range [1:%d]",
					chunkerConstants.MaxChunkSize,
					constants.MaxLeafPayloadSize,
				))
			} else if chunkerConstants.MinChunkSize < 0 || chunkerConstants.MinChunkSize > chunkerConstants.MaxChunkSize {
				initErrors = append(initErrors, fmt.Sprintf(
					"returned MinChunkSize constant '%d' out of range [0:%d]",
					chunkerConstants.MinChunkSize,
					chunkerConstants.MaxChunkSize,
				))
			}
		}

		if len(initErrors) > 0 {
			dgr.cfg.erroredChunkers = append(dgr.cfg.erroredChunkers, chunkerArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of chunker '%s' failed: %s",
					chunkerArgs[0],
					e,
				))
			}
		} else {
			dgr.chainedChunkers = append(dgr.chainedChunkers, dgrChunkerUnit{
				instance:  chunkerInstance,
				constants: chunkerConstants,
			})
		}
	}

	return
}

func (dgr *Dagger) setupCollectorChain(nodeEnc dgrencoder.NodeEncoder) (argErrs []string) {

	// bail early
	if dgr.cfg.optSet.IsSet("collectors") && dgr.cfg.requestedCollectors == "" {
		return []string{
			"When specified, collector chain must be in the form '--collectors=algname1_opt1_opt2__algname2_...'. Available collector names are: " +
				util.AvailableMapKeys(availableCollectors),
		}
	}

	commonCfg := dgrcollector.DaggerConfig{
		NodeEncoder: nodeEnc,
	}

	for _, c := range dgr.chainedChunkers {
		if c.constants.MaxChunkSize > commonCfg.ChunkerChainMaxResult {
			commonCfg.ChunkerChainMaxResult = c.constants.MaxChunkSize
		}
	}

	individualCollectors := strings.Split(dgr.cfg.requestedCollectors, "__")

	// we need to process the collectors in reverse, in order to populate NextCollector
	dgr.chainedCollectors = make([]dgrcollector.Collector, len(individualCollectors))
	for collectorNum := len(individualCollectors); collectorNum > 0; collectorNum-- {

		collectorCmd := individualCollectors[collectorNum-1]

		collectorArgs := strings.Split(collectorCmd, "_")
		init, exists := availableCollectors[collectorArgs[0]]
		if !exists {
			argErrs = append(argErrs, fmt.Sprintf(
				"Collector '%s' not found. Available collector names are: %s",
				collectorArgs[0],
				util.AvailableMapKeys(availableCollectors),
			))
			continue
		}

		for n := range collectorArgs {
			if n > 0 {
				collectorArgs[n] = "--" + collectorArgs[n]
			}
		}

		collectorCfg := commonCfg // SHALLOW COPY!!!
		collectorCfg.ChainPosition = collectorNum
		if collectorNum != len(individualCollectors) {
			collectorCfg.NextCollector = dgr.chainedCollectors[collectorNum]
		}

		if collectorInstance, initErrors := init(
			collectorArgs,
			&collectorCfg,
		); len(initErrors) > 0 {

			dgr.cfg.erroredCollectors = append(dgr.cfg.erroredCollectors, collectorArgs[0])
			for _, e := range initErrors {
				argErrs = append(argErrs, fmt.Sprintf(
					"Initialization of collector '%s' failed: %s",
					collectorArgs[0],
					e,
				))
			}
		} else {
			dgr.chainedCollectors[collectorNum-1] = collectorInstance
		}
	}

	return
}

type compatIpfsArgs struct {
	CidVersion       int    `getopt:"--cid-version"`
	InlineActive     bool   `getopt:"--inline"`
	InlineLimit      int    `getopt:"--inline-limit"`
	UseRawLeaves     bool   `getopt:"--raw-leaves"`
	UpgradeV0CID     bool   `getopt:"--upgrade-cidv0-in-output"`
	TrickleCollector bool   `getopt:"--trickle"`
	Chunker          string `getopt:"--chunker"`
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
		cfg.hashFunc = "sha2-256"
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

	// ignore everything compat if a collector is already given
	if !cfg.optSet.IsSet("collectors") {
		// either trickle or fixed-outdegree, go-ipfs doesn't understand much else
		if ipfsOpts.TrickleCollector {
			cfg.requestedCollectors = "trickle_max-direct-leaves=174_max-sibling-subgroups=4_unixfs-nul-leaf-compat"
		} else {
			cfg.requestedCollectors = "fixed-outdegree_max-outdegree=174"
		}
	}

	// ignore everything compat if an encoder is already given
	if !cfg.optSet.IsSet("node-encoder") {

		ufsv1EncoderOpts := []string{"unixfsv1", "merkledag-compat-protobuf"}

		if ipfsOpts.CidVersion != 1 {
			if ipfsOpts.UpgradeV0CID && ipfsOpts.CidVersion == 0 {
				ufsv1EncoderOpts = append(ufsv1EncoderOpts, "cidv0")
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
			if ipfsOpts.TrickleCollector {
				ufsv1EncoderOpts = append(ufsv1EncoderOpts, "unixfs-leaf-decorator-type=0")
			} else {
				ufsv1EncoderOpts = append(ufsv1EncoderOpts, "unixfs-leaf-decorator-type=2")
			}
		}

		cfg.requestedNodeEncoder = strings.Join(ufsv1EncoderOpts, "_")
	}

	// ignore everything compat if a chunker is already given
	if !cfg.optSet.IsSet("chunkers") {

		if strings.HasPrefix(ipfsOpts.Chunker, "size") {
			sizeopts := strings.Split(ipfsOpts.Chunker, "-")
			if sizeopts[0] == "size" {
				if len(sizeopts) == 1 {
					cfg.requestedChunkers = "fixed-size_262144"
				} else if len(sizeopts) == 2 {
					cfg.requestedChunkers = "fixed-size_" + sizeopts[1]
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
					cfg.requestedChunkers = fmt.Sprintf("rabin_rabin-preset=GoIPFSv0_state-target=0_state-mask-bits=%s_min-size=%s_max-size=%s",
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
					cfg.requestedChunkers = "buzhash_hash-table=GoIPFSv0_state-target=0_state-mask-bits=17_min-size=131072_max-size=524288"
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

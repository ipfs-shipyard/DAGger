package dagger

import (
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"

	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/util"

	"github.com/ipfs/go-qringbuf"
)

// the code as-written expects the steps to be numerically ordered
var textstatsDistributionPercentiles = [...]int{3, 10, 25, 50, 95}

// The bit reduction is to make the internal seen map smaller memory-wise
// That many bits are taken off the *end* of any non-identity CID
// We could remove it, but for now there's no reason to, and as an extra
// benefit it makes the murmur3 case *way* easier to code
const seenHashSize = 128 / 8

type blockPostProcessResult struct {
}

type seenBlocks map[[seenHashSize]byte]uniqueBlockStats
type seenRoots map[[seenHashSize]byte][]byte

func seenKey(b *block.Header) *[seenHashSize]byte {
	if b == nil ||
		b.IsInlined() ||
		b.DummyHashed() {
		return nil
	}

	cid := b.Cid()
	var id [seenHashSize]byte
	copy(
		id[:],
		cid[(len(cid)-seenHashSize):],
	)

	return &id
}

type statSummary struct {
	EventType string `json:"event"`
	Dag       struct {
		Nodes   int64 `json:"nodes"`
		Size    int64 `json:"wireSize"`
		Payload int64 `json:"payload"`
	} `json:"logicalDag"`
	Streams  int64        `json:"subStreams"`
	Roots    []rootStats  `json:"roots,omitempty"`
	Layers   []layerStats `json:"layers,omitempty"`
	SysStats struct {
		ArgvExpanded []string `json:"argvExpanded"`
		ArgvInitial  []string `json:"argvInitial"`
		qringbuf.Stats
		ElapsedNsecs int64 `json:"elapsedNanoseconds"`

		// getrusage() section
		CpuUserNsecs int64 `json:"cpuUserNanoseconds"`
		CpuSysNsecs  int64 `json:"cpuSystemNanoseconds"`
		MaxRssBytes  int64 `json:"maxMemoryUsed"`
		MinFlt       int64 `json:"cacheMinorFaults"`
		MajFlt       int64 `json:"cacheMajorFaults"`
		BioRead      int64 `json:"blockIoReads,omitempty"`
		BioWrite     int64 `json:"blockIoWrites,omitempty"`
		Sigs         int64 `json:"signalsReceived,omitempty"`
		CtxSwYield   int64 `json:"contextSwitchYields"`
		CtxSwForced  int64 `json:"contextSwitchForced"`

		// for context
		PageSize  int    `json:"pageSize"`
		NumCPU    int    `json:"cpuCount"`
		GoVersion string `json:"goVersion"`
	} `json:"sys"`
}
type layerStats struct {
	label     string
	LongLabel string `json:"label"`

	// the map is used to construct the array for display at the very end
	countTracker    map[int]*sameSizeBlockStats
	BlockSizeCounts []sameSizeBlockStats `json:"distinctlySizedBlockCounts"`
}
type rootStats struct {
	Cid         string `json:"cid"`
	SizeDag     uint64 `json:"wireSize"`
	SizePayload uint64 `json:"payload"`
	Dup         bool   `json:"duplicate,omitempty"`
}
type sameSizeBlockStats struct {
	CountUniqueBlocksAtSize int64 `json:"count"`
	SizeBlock               int   `json:"blockSize"`
	CountRootBlocksAtSize   int64 `json:"roots,omitempty"`
}
type uniqueBlockStats struct {
	sizeData  int // recorded but not aggregated for output at present
	sizeBlock int
	seenAt    seenTimesAt
	*blockPostProcessResult
}
type generatedBy struct {
	generatorIndex int
	localLayer     int
}
type seenTimesAt map[generatedBy]int64

func (dgr *Dagger) OutputSummary() {

	// no stats emitters - nowhere to output
	if dgr.cfg.emitters[emStatsText] == nil && dgr.cfg.emitters[emStatsJsonl] == nil {
		return
	}

	smr := &dgr.statSummary
	var totalUCount, totalUWeight, leafUWeight, leafUCount, sparseUWeight, sparseUCount int64

	if dgr.seenBlocks != nil && len(dgr.seenBlocks) > 0 {
		layers := make(map[generatedBy]*layerStats, 10) // if more than 10 layers - something odd is going on

		for sk, b := range dgr.seenBlocks {
			totalUCount++
			totalUWeight += int64(b.sizeBlock)

			// An identical block could be emitted by multiple generators ( e.g. trickle could )
			// Take the lowest-sorting one
			gens := make([]generatedBy, 0, len(b.seenAt))
			for g := range b.seenAt {
				gens = append(gens, g)
			}
			sortGenerators(gens)

			if _, exist := layers[gens[0]]; !exist {
				layers[gens[0]] = &layerStats{
					countTracker: make(map[int]*sameSizeBlockStats, 256), // SANCHECK: somewhat arbitrary
				}
			}
			if _, exist := layers[gens[0]].countTracker[b.sizeBlock]; !exist {
				layers[gens[0]].countTracker[b.sizeBlock] = &sameSizeBlockStats{
					SizeBlock: b.sizeBlock,
				}

			}
			layers[gens[0]].countTracker[b.sizeBlock].CountUniqueBlocksAtSize++

			if _, root := dgr.seenRoots[sk]; root {
				layers[gens[0]].countTracker[b.sizeBlock].CountRootBlocksAtSize++
			}
		}

		genInOrder := make([]generatedBy, 0, len(layers))
		for g := range layers {
			genInOrder = append(genInOrder, g)
		}
		sortGenerators(genInOrder)

		for i, g := range genInOrder {

			if g.generatorIndex == 0 {
				if g.localLayer == 0 {
					layers[g].LongLabel = "DataBlocks"
					layers[g].label = "DB"
					for s, c := range layers[g].countTracker {
						leafUWeight += c.CountUniqueBlocksAtSize * int64(s)
						leafUCount += c.CountUniqueBlocksAtSize
					}
				} else {
					layers[g].LongLabel = "SparseBlocks"
					layers[g].label = "SB"
					for s, c := range layers[g].countTracker {
						sparseUWeight += c.CountUniqueBlocksAtSize * int64(s)
						sparseUCount += c.CountUniqueBlocksAtSize
					}
				}
			} else {
				layers[g].LongLabel = fmt.Sprintf("LinkingLayer%d", i+1)
				layers[g].label = fmt.Sprintf("L%d", i+1)
			}

			for _, c := range layers[g].countTracker {
				layers[g].BlockSizeCounts = append(layers[g].BlockSizeCounts, *c)
			}
			sort.Slice(layers[g].BlockSizeCounts, func(i, j int) bool {
				return layers[g].BlockSizeCounts[i].SizeBlock < layers[g].BlockSizeCounts[j].SizeBlock
			})

			smr.Layers = append(smr.Layers, *layers[g])
		}
	}

	if statsJosnlOut := dgr.cfg.emitters[emStatsJsonl]; statsJosnlOut != nil {
		// emit the JSON last, so that piping to e.g. `jq` works nicer
		defer func() {

			// because the golang encoder is garbage
			if smr.Layers == nil {
				smr.Layers = []layerStats{}
			}
			if smr.Roots == nil {
				smr.Roots = []rootStats{}
			}

			json, err := json.Marshal(smr)
			if err != nil {
				log.Fatalf("Encoding stats-jsonl failed: %s", err)
			}

			if _, err := fmt.Fprintf(statsJosnlOut, "%s\n", json); err != nil {
				log.Fatalf("Emitting '%s' failed: %s", emStatsJsonl, err)
			}
		}()
	}

	statsTextOut := dgr.cfg.emitters[emStatsText]
	if statsTextOut == nil {
		return
	}

	var substreamsDesc string
	if dgr.cfg.MultipartStream {
		substreamsDesc = fmt.Sprintf(
			" from %s substreams",
			util.Commify64(dgr.statSummary.Streams),
		)
	}

	writeTextOutf := func(f string, args ...interface{}) {
		if _, err := fmt.Fprintf(statsTextOut, f, args...); err != nil {
			log.Fatalf("Emitting '%s' failed: %s", emStatsText, err)
		}
	}

	writeTextOutf(
		"\nProcessing took %0.2f seconds using %0.2f vCPU and %0.2f MiB peak memory"+
			"\nPerforming %s system reads using %0.2f vCPU at about %0.2f MiB/s"+
			"\nIngesting payload of:%17s bytes%s\n\n",

		float64(smr.SysStats.ElapsedNsecs)/
			1000000000,

		float64(smr.SysStats.CpuUserNsecs)/
			float64(smr.SysStats.ElapsedNsecs),

		float64(smr.SysStats.MaxRssBytes)/
			(1024*1024),

		util.Commify64(smr.SysStats.ReadCalls),

		float64(smr.SysStats.CpuSysNsecs)/
			float64(smr.SysStats.ElapsedNsecs),

		(float64(smr.Dag.Payload)/(1024*1024))/
			(float64(smr.SysStats.ElapsedNsecs)/1000000000),

		util.Commify64(smr.Dag.Payload),

		substreamsDesc,
	)

	if smr.Dag.Nodes > 0 {
		writeTextOutf(
			"Forming DAG covering:%17s bytes across %s nodes\n",
			util.Commify64(smr.Dag.Size), util.Commify64(smr.Dag.Nodes),
		)
	}

	if len(smr.Layers) == 0 {
		return
	}

	descParts := make([]string, 0, 32)

	descParts = append(descParts, fmt.Sprintf(
		"Dataset deduped into:%17s bytes over %s unique leaf nodes\n",
		util.Commify64(leafUWeight), util.Commify64(leafUCount),
	))

	if len(smr.Layers) > 1 {
		descParts = append(descParts, fmt.Sprintf(
			"Linked as streams by:%17s bytes over %s unique DAG-PB nodes\n"+
				"Taking a grand-total:%17s bytes, ",
			util.Commify64(totalUWeight-leafUWeight-sparseUWeight), util.Commify64(totalUCount-leafUCount-sparseUCount),
			util.Commify64(totalUWeight),
		))
	} else {
		descParts = append(descParts, fmt.Sprintf("%44s", ""))
	}

	descParts = append(descParts, fmt.Sprintf(
		"%.02f%% of original, %.01fx smaller\n"+
			` Roots\Counts\Sizes:`,
		100*float64(totalUWeight)/float64(smr.Dag.Payload),
		float64(smr.Dag.Payload)/float64(totalUWeight),
	))

	for i, val := range textstatsDistributionPercentiles {
		if i == 0 {
			descParts = append(descParts, fmt.Sprintf(" %5d%%", val))
		} else {
			descParts = append(descParts, fmt.Sprintf(" %8d%%", val))
		}
	}
	descParts = append(descParts, " |      Avg\n")

	for _, ls := range smr.Layers {
		descParts = append(descParts, distributionForLayer(ls))
	}

	writeTextOutf("%s\n", strings.Join(descParts, ""))
}

func sortGenerators(g []generatedBy) {
	if len(g) > 1 {
		sort.Slice(g, func(i, j int) bool {
			if g[i].generatorIndex != g[j].generatorIndex {
				return g[i].generatorIndex > g[j].generatorIndex
			}
			return g[i].localLayer > g[j].localLayer
		})
	}
}

func distributionForLayer(l layerStats) (distLine string) {
	var uWeight, uCount, roots int64

	for s, c := range l.countTracker {
		uCount += c.CountUniqueBlocksAtSize
		uWeight += c.CountUniqueBlocksAtSize * int64(s)
		roots += c.CountRootBlocksAtSize
	}

	distChunks := make([][]byte, len(textstatsDistributionPercentiles))

	for i, step := range textstatsDistributionPercentiles {
		threshold := 1 + int64(float64(uCount*int64(step))/100)

		// outright skip this position if the next threshold is identical
		if i+1 < len(textstatsDistributionPercentiles) &&
			threshold == 1+int64(float64(uCount*int64(textstatsDistributionPercentiles[i+1]))/100) {
			continue
		}

		var runningCount int64
		for _, sc := range l.BlockSizeCounts {
			runningCount += sc.CountUniqueBlocksAtSize
			if runningCount >= threshold {
				distChunks[i] = util.Commify(sc.SizeBlock)
				break
			}
		}
	}

	dist := make([]byte, 0, len(distChunks)*10)
	for _, formattedSize := range distChunks {
		dist = append(dist, fmt.Sprintf(" %9s", formattedSize)...)
	}

	var layerCounts string
	if roots > 0 {
		rootStr := fmt.Sprintf("{%d}", roots)

		layerCounts = fmt.Sprintf(
			fmt.Sprintf("%%s%%%ds", 13-len(rootStr)),
			rootStr,
			util.Commify64(uCount),
		)
	} else {
		layerCounts = fmt.Sprintf("%13s", util.Commify64(uCount))
	}

	return fmt.Sprintf(
		"%s%3s:%s |%9s\n",
		layerCounts,
		l.label,
		dist,
		util.Commify64(
			uWeight/uCount,
		),
	)
}

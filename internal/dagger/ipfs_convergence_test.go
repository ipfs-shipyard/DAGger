package dagger

import (
	"encoding/csv"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/exp/rand"

	"github.com/ipfs-shipyard/DAGger/constants"
	"github.com/ipfs-shipyard/DAGger/internal/dagger/block"
)

// base command => expected cid => file
type convergenceTestMatrix map[string]map[string]string

func TestGoIpfsConvergence(t *testing.T) {

	matrix := parseConvergenceData("../../maint/misc/convergence_rawdata.tsv")

	for cmd := range matrix {

		// avoid parallelization surprises
		cmd := cmd

		// the base convergence args are always the same
		args := []string{
			"dolphin-dongs",
			"--emit-stderr=stats-text",
			"--emit-stdout=stats-jsonl,roots-jsonl",
			"--process-nul-inputs",
			"--multipart",
		}
		baselen := len(args)

		if !constants.VeryLongTests && rand.Intn(2) == 1 {
		}

		args = append(args, "--ipfs-add-compatible-command")
		args = append(args, cmd)

		t.Run(
			strings.Join(args[baselen:], " "),
			func(t *testing.T) {
				t.Parallel()

				var tuples struct {
					compressedInputFiles []string
					expectedCIDs         []string
				}

				for cid, path := range matrix[cmd] {

					// what to skip
					if (!constants.LongTests && strings.Contains(path, "rand_")) ||
						(!constants.VeryLongTests && strings.Contains(path, "large_repeat_")) ||
						!fileExists(path) {
						continue
					}

					tuples.compressedInputFiles = append(tuples.compressedInputFiles, path)
					tuples.expectedCIDs = append(tuples.expectedCIDs, cid)
				}

				unpacker := exec.Command("../../tmp/maintbin/dezstd", tuples.compressedInputFiles...)
				dataIn, pipeErr := unpacker.StdoutPipe()
				if pipeErr != nil {
					log.Fatalf("Failed pipe setup: %s", pipeErr)
				}
				if err := unpacker.Start(); err != nil {
					log.Fatalf("Decompressor start failed: %s", err)
				}

				defer func() {
					if err := unpacker.Wait(); err != nil {
						log.Fatalf("Decompressor did not shut down correctly: %s", err)
					}
				}()

				roots := make(chan *block.Header, 128)

				cfg, _ := ParseOpts(args)
				streamErr := ProcessReader(
					cfg,
					dataIn,
					roots,
				)

				if streamErr != nil {
					t.Fatalf("Unexpected stream processing error: %s", streamErr)
				}

				for i, wantCid := range tuples.expectedCIDs {
					r := <-roots
					if r.CidBase32() != wantCid {
						t.Fatalf(
							"Expected root CID %s for %s, but instead generated %s",
							wantCid,
							tuples.compressedInputFiles[i],
							r.CidBase32(),
						)
					}
				}
			},
		)
	}
}

func fileExists(fn string) bool {
	stat, _ := os.Stat(fn)
	return stat != nil && stat.Mode().IsRegular()
}

func parseConvergenceData(convDataFn string) convergenceTestMatrix {

	convFh, err := os.Open(convDataFn)
	if err != nil {
		log.Fatalf("Unable to open convergence list '%s': %s", convDataFn, err)
	}

	tsv := csv.NewReader(convFh)
	tsv.Comma = '\t'

	matrix := make(convergenceTestMatrix)

	for {
		record, err := tsv.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			log.Fatalf("Failed reading '%s': %s", convDataFn, err)
		}

		fields := make(map[string]string)
		for _, s := range record {
			pieces := strings.Split(s, ":")
			fields[string(pieces[0])] = strings.Join(pieces[1:], ":")
		}

		// fields["Trickle"] != "false" ||
		// fields["CidVer"] != "1" ||
		// fields["Inlining"] != "0" ||
		// strings.Contains(fields["Chunker"], "buz") ||
		// !strings.Contains(fields["Data"], "uicro") {
		if fields["Impl"] != "go" {
			continue
		}

		if _, exists := matrix[fields["Cmd"]]; !exists {
			matrix[fields["Cmd"]] = make(map[string]string)
		}

		matrix[fields["Cmd"]][fields["CID"]] = "../../maint/testdata/" + fields["Data"]
	}

	return matrix
}

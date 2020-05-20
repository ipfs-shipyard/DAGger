package dagger

import (
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/ipfs-shipyard/DAGger/internal/constants"
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

				events := make(chan IngestionEvent, 128)

				go NewFromArgv(args).ProcessReader(
					dataIn,
					events,
				)

				type rootEvent struct {
					Cid string
				}
				var cidNum int
				for {
					ev, chanOpen := <-events
					if !chanOpen {
						break
					} else if ev.Type == ErrorString {
						t.Fatalf("Unexpected stream processing error: %s", ev.Body)
					} else if ev.Type == NewRootJsonl {
						var r rootEvent
						if err := json.Unmarshal([]byte(ev.Body), &r); err != nil {
							t.Fatalf("Unexpected event unmarshal error: %s", err)
						}

						if r.Cid != tuples.expectedCIDs[cidNum] {
							t.Fatalf(
								"Expected root CID %s for %s, but instead generated %s",
								tuples.expectedCIDs[cidNum],
								tuples.compressedInputFiles[cidNum],
								r.Cid,
							)
						}

						cidNum++
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
		// !strings.Contains(fields["Data"], "zero") ||
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

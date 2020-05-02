.PHONY: $(MAKECMDGOALS)

DAGLD_STRIP=-s -w -buildid=
DAGGC_NOBOUNDCHECKS=-B -C

DAGMOD=github.com/ipfs-shipyard/DAGger
DAGMOD_EXTRA=github.com/ipfs/go-qringbuf

build:
	mkdir -p bin/
	go build --trimpath \
		"-ldflags=all=$(DAGLD_STRIP)" \
		-o bin/ ./cmd/*

	go build --trimpath \
		-tags nosanitychecks \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		"-ldflags=all=$(DAGLD_STRIP)" \
		-o bin/stream-dagger-nochecks ./cmd/stream-dagger

	mkdir -p tmp/pprof
	go build -tags profiling "-ldflags=-X $(DAGMOD)/internal/dagger/util.profileOutDir=$(shell pwd)/tmp/pprof" \
		"-gcflags=all=-l" \
		-o bin/stream-dagger-profiling ./cmd/stream-dagger

analyze-all:
	go build \
		$(addsuffix /...="-m -m",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

analyze-all-nosanchecks:
	go build \
		-tags nosanitychecks \
		$(addsuffix /...="-m -m",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

analyze-bound-checks:
	go build \
		$(addsuffix /...="--d=ssa/check_bce/debug=1",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

serve-latest-pprof-cpu:
	go tool pprof -http=:9090 tmp/pprof/latest_cpu.prof

serve-latest-pprof-heap:
	go tool pprof -http=:9090 tmp/pprof/latest_heap.prof

test: build-maint
	# anything above 32 and we blow through > 256 open file handles
	go test -timeout=0 -parallel=32 -count=1 -failfast ./...

build-maint:
	mkdir -p tmp/maintbin
	# build the maint tools without boundchecks to speed things up
	go build --trimpath \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		"-ldflags=all=$(DAGLD_RELEASE)" \
		-o tmp/maintbin/ ./maint/...

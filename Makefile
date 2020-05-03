.PHONY: $(MAKECMDGOALS)

DAGLD_STRIP=-s -w -buildid=
DAGGC_NOBOUNDCHECKS=-B -C

DAGMOD=github.com/ipfs-shipyard/DAGger
DAGMOD_EXTRA=github.com/ipfs/go-qringbuf

NOSANCHECKS=-tags 'nosanchecks_DAGger nosanchecks_qringbuf'

build:
	mkdir -p tmp/pprof
	mkdir -p bin/

	go build \
		--trimpath "-ldflags=all=$(DAGLD_STRIP)" \
		-o bin/stream-dagger ./cmd/stream-dagger

	go build \
		$(NOSANCHECKS) \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		--trimpath "-ldflags=all=$(DAGLD_STRIP)" \
		-o bin/stream-dagger-nochecks ./cmd/stream-dagger

	go build \
		"-gcflags=all=-l" \
		-tags profile "-ldflags=-X $(DAGMOD)/internal/dagger/util.profileOutDir=$(shell pwd)/tmp/pprof" \
		-o bin/stream-dagger-writepprof ./cmd/stream-dagger

	go build \
		$(NOSANCHECKS) \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		"-gcflags=all=-l" \
		-tags profile "-ldflags=-X $(DAGMOD)/internal/dagger/util.profileOutDir=$(shell pwd)/tmp/pprof" \
		-o bin/stream-dagger-writepprof-nochecks ./cmd/stream-dagger

analyze-all:
	go build \
		$(addsuffix /...="-m -m",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

analyze-all-nochecks:
	go build \
		$(NOSANCHECKS) \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		$(addsuffix /...="-m -m",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

analyze-bound-checks:
	go build \
		$(addsuffix /...="--d=ssa/check_bce/debug=1",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

serve-latest-pprof-cpu:
	go tool pprof -http=:9090 tmp/pprof/latest_cpu.prof

serve-latest-pprof-allocs:
	go tool pprof -http=:9090 -alloc_objects tmp/pprof/latest_allocs.prof

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

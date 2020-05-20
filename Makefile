.PHONY: $(MAKECMDGOALS)

DAGGO=go
DAGLD_STRIP=-gcflags -trimpath="$(shell pwd)" -ldflags=all="-s -w -buildid="
DAGGC_NOBOUNDCHECKS=-B -C

DAGMOD=github.com/ipfs-shipyard/DAGger
DAGMOD_EXTRA=github.com/ipfs/go-qringbuf

DAGTAG_NOSANCHECKS=nosanchecks_DAGger nosanchecks_qringbuf

### !!! Needs to be set when using padfinder_rure
###     See https://github.com/BurntSushi/rure-go#install
# export CGO_LDFLAGS=-L$(HOME)/devel/regex/target/release
# DAGTAG_PADFINDER_TYPE=padfinder_rure

build-all: build
	mkdir -p tmp/pprof

	$(DAGGO) build \
		-tags "$(DAGTAG_PADFINDER_TYPE) $(DAGTAG_NOSANCHECKS)" \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		$(DAGLD_STRIP) \
		-o bin/stream-dagger-nochecks ./cmd/stream-dagger

	$(DAGGO) build \
		-tags "profile $(DAGTAG_PADFINDER_TYPE)" "-ldflags=-X $(DAGMOD)/internal/dagger/util.profileOutDir=$(shell pwd)/tmp/pprof" \
		"-gcflags=all=-l" \
		-o bin/stream-dagger-writepprof ./cmd/stream-dagger

	$(DAGGO) build \
		-tags "profile $(DAGTAG_PADFINDER_TYPE) $(DAGTAG_NOSANCHECKS)" "-ldflags=-X $(DAGMOD)/internal/dagger/util.profileOutDir=$(shell pwd)/tmp/pprof" \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		"-gcflags=all=-l" \
		-o bin/stream-dagger-writepprof-nochecks ./cmd/stream-dagger

build:
	mkdir -p bin/

	$(DAGGO) build \
		-tags "$(DAGTAG_PADFINDER_TYPE)" \
		$(DAGLD_STRIP) \
		-o bin/stream-dagger ./cmd/stream-dagger


test: build-maint
	# anything above 32 and we blow through > 256 open file handles
	$(DAGGO) test -tags "$(DAGTAG_PADFINDER_TYPE)" -timeout=0 -parallel=32 -count=1 -failfast ./...

build-maint:
	mkdir -p tmp/maintbin
	# build the maint tools without boundchecks to speed things up
	$(DAGGO) build \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		"-ldflags=all=$(DAGLD_RELEASE)" \
		-o tmp/maintbin/dezstd ./maint/src/dezstd


analyze-all:
	$(DAGGO) build \
		-tags "$(DAGTAG_PADFINDER_TYPE)" \
		$(addsuffix /...="-m -m",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

analyze-all-nochecks:
	$(DAGGO) build \
		-tags "$(DAGTAG_PADFINDER_TYPE) $(DAGTAG_NOSANCHECKS)" \
		"-gcflags=all=$(DAGGC_NOBOUNDCHECKS)" \
		$(addsuffix /...="-m -m",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )

analyze-bound-checks:
	$(DAGGO) build \
		-tags "$(DAGTAG_PADFINDER_TYPE)" \
		$(addsuffix /...="-d=ssa/check_bce/debug=1",$(addprefix -gcflags=,$(DAGMOD) $(DAGMOD_EXTRA))) \
		-o /dev/null ./cmd/stream-dagger 2>&1 | ( [ -t 1 ] && less -SRI || cat )


serve-latest-pprof-cpu:
	$(DAGGO) tool pprof -http=:9090 tmp/pprof/latest_cpu.prof

serve-latest-pprof-allocs:
	$(DAGGO) tool pprof -http=:9090 -alloc_objects tmp/pprof/latest_allocs.prof


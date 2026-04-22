.PHONY: test test-serial test-static test-all demo demo-newssim bench viz json search all cli-search cli-trip cli-build cli-ingest cli-info serve mct-inspect mct-inspect-styled mct-replay mct-inspect-search sysimage app juliac

# Fast dev test loop — parallel runner (~47s), skips JET + Aqua. Override the
# worker count with ITINSEARCH_TEST_WORKERS=N (default 4).
test:
	julia --project=. test/runtests_parallel.jl

# Serial test runner — skips JET + Aqua but runs files one at a time.
# Useful when debugging a worker-specific failure in the parallel runner,
# or when running on a machine where spawning workers is undesirable.
test-serial:
	ITINSEARCH_SKIP_STATIC=1 julia --project=. -e 'using Pkg; Pkg.test()'

# Static analysis only (JET + Aqua). Runs in ~25s against a warm precompile cache.
test-static:
	ITINSEARCH_ONLY_STATIC=1 julia --project=. -e 'using Pkg; Pkg.test()'

# Full suite — everything, serial, including JET + Aqua. Use for CI / pre-push.
test-all:
	julia --project=. -e 'using Pkg; Pkg.test()'

# Full demo: load, search, CSV, JSON, visualizations (3 days)
demo:
	julia --project=. scripts/demo.jl

# Demo using NewSSIM CSV path (1 day, demo sample data)
demo-newssim:
	julia --project=. scripts/demo.jl --newssim

# Benchmarks
bench:
	julia --project=. benchmark/run_benchmarks.jl

# Regenerate visualizations only (network map, timeline, trips, ref table)
# Usage: make viz [DATE=2026-03-18]
viz:
	julia --project=. scripts/viz_only.jl $(DATE)

# Search and write JSON output only (full + compact)
# Usage: make json [DATE=2026-03-18] [DAYS=3]
json:
	julia --project=. scripts/json_only.jl $(DATE) $(DAYS)

# Single OD search with all outputs (CSV, JSON, HTML table, network map)
# Usage: make search ORG=ORD DST=LHR DATE=2026-03-20
#        make search ORG=ORD DATE=2026-03-20            (all destinations)
search:
	julia --project=. scripts/search.jl $(ORG) $(DST) $(DATE)

# CLI commands
# Usage: make cli-search ORG=ORD DST=LHR DATE=2026-03-20
#        make cli-search ORG=ORD,DEN DST=LHR,LAX DATE=2026-03-20
#        make cli-trip LEGS="ORD LHR 2026-03-20 LHR ORD 2026-03-27"
#        make cli-build DATE=2026-03-20
cli-search:
	julia --project=. bin/itinsearch.jl search $(ORG) $(DST) $(DATE) $(EXTRA)

cli-trip:
	julia --project=. bin/itinsearch.jl trip $(LEGS) $(EXTRA)

cli-build:
	julia --project=. bin/itinsearch.jl build --date $(DATE) $(EXTRA)

cli-ingest:
	julia --project=. bin/itinsearch.jl ingest $(EXTRA)

cli-info:
	julia --project=. bin/itinsearch.jl info $(EXTRA)

# REST API server
# Usage: make serve DATE=2026-03-20
#        make serve DATE=2026-03-20 PORT=9090
PORT ?= 8080
serve:
	julia --project=. bin/itinsearch.jl serve --date $(DATE) --port $(PORT) $(EXTRA)

# MCT audit inspector (interactive)
# Usage: make mct-inspect FILE=data/input/UA_Misconnect_Report.csv
#        make mct-inspect FILE=data/input/UAOA_Misconnect_Report.csv
FILE ?= data/input/UA_Misconnect_Report.csv
mct-inspect:
	julia --project=. scripts/mct_inspect.jl $(FILE) $(EXTRA)

# MCT audit inspector with Term.jl styled output (colored panels and tables)
# Usage: make mct-inspect-styled FILE=data/input/UA_Misconnect_Report.csv
mct-inspect-styled:
	julia --project=. -e 'using Term; include("scripts/mct_inspect.jl")' -- $(FILE) $(EXTRA)

# MCT replay (write comparison CSV)
# Usage: make mct-replay FILE=data/input/UA_Misconnect_Report.csv
#        make mct-replay FILE=data/input/UA_Misconnect_Report.csv EXTRA="--detailed"
mct-replay:
	julia --project=. scripts/mct_inspect.jl $(FILE) --replay $(EXTRA)

# MCT inspector — search mode: run a search, then inspect each connection
# in each result interactively (a 2-stop itinerary contributes both
# connections in sequence; step with the same n/p/c/b commands as misconnect
# rows).  This loads the full schedule, so first invocation takes ~12s.
# Usage: make mct-inspect-search ORG=ORD DST=LHR DATE=2026-03-20
mct-inspect-search:
	julia --project=. scripts/mct_inspect.jl --search $(ORG) $(DST) $(DATE) $(EXTRA)

# PackageCompiler builds (uses test suite as precompile workload)
sysimage:
	julia --project=. build/build.jl sysimage

app:
	julia --project=. build/build.jl app

juliac:
	julia --project=. build/build.jl juliac

all: test bench demo

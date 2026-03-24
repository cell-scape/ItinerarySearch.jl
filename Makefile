.PHONY: test demo bench viz json search all cli-search cli-trip cli-build cli-ingest cli-info sysimage app juliac

# Run full test suite
test:
	julia --project=. -e 'using Pkg; Pkg.test()'

# Full demo: load, search, PSV, JSON, visualizations (3 days)
demo:
	julia --project=. scripts/demo.jl

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

# Single OD search with all outputs (PSV, JSON, HTML table, network map)
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

# PackageCompiler builds (uses test suite as precompile workload)
sysimage:
	julia --project=. build/build.jl sysimage

app:
	julia --project=. build/build.jl app

juliac:
	julia --project=. build/build.jl juliac

all: test bench demo

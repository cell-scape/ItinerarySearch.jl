.PHONY: test demo bench viz json search all

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

all: test bench demo

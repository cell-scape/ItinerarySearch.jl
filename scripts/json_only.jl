#!/usr/bin/env julia
# scripts/json_only.jl — Search and write JSON output only (full + compact)
#
# Usage:
#   julia --project=. scripts/json_only.jl                    # defaults
#   julia --project=. scripts/json_only.jl 2026-03-20         # specific date
#   julia --project=. scripts/json_only.jl 2026-03-20 3       # date + number of days

using ItinerarySearch
using Dates

include("common.jl")

start_date = length(ARGS) >= 1 ? Date(ARGS[1]) : Date(2026, 3, 18)
n_days     = length(ARGS) >= 2 ? parse(Int, ARGS[2]) : 3

println("ItinerarySearch — JSON Output")
println("="^50)
println("  Start date: $(start_date)")
println("  Days:       $(n_days)")
println()

od_pairs = DEFAULT_OD_PAIRS
origins = [o for (o, _) in od_pairs]
dests   = [d for (_, d) in od_pairs]
outdir = "data/output"
mkpath(outdir)

config = SearchConfig()
store = DuckDBStore()
load_schedule!(store, config)

for day_offset in 0:(n_days - 1)
    target = start_date + Day(day_offset)
    t0 = time()

    graph = build_graph!(store, config, target)
    ctx = RuntimeContext(
        config = config,
        constraints = SearchConstraints(),
        itn_rules = build_itn_rules(config),
    )

    # Full JSON
    json_file = joinpath(outdir, "legs_index_$(target).json")
    json = itinerary_legs_json(graph.stations, ctx;
        origins=origins, destinations=dests, dates=target,
    )
    write(json_file, json)

    # Compact JSON
    compact_file = joinpath(outdir, "legs_index_$(target)_compact.json")
    compact = itinerary_legs_json(graph.stations, ctx;
        origins=origins, destinations=dests, dates=target, compact=true,
    )
    write(compact_file, compact)

    dt = round(time() - t0; digits=1)
    println("[$(target)] full=$(round(filesize(json_file)/1024; digits=0))KB compact=$(round(filesize(compact_file)/1024; digits=0))KB ($(dt)s)")
end

close(store)
println("\nDone!")

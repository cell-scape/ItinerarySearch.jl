# benchmark/bench_schedule_pure.jl — pure-search benchmarks (search phase only)
#
# Ingest + graph build happen ONCE outside the timed block, so these timings
# reflect only the search work. Parallelism gains are visible here without
# Amdahl pollution from serial ingest/build. Compare with bench_schedule.jl,
# which measures real-world wall time for a one-shot invocation including all
# setup costs.
#
# For regression tracking, run under multiple thread counts:
#   for t in 1 2 4 auto; do
#     JULIA_NUM_THREADS=$t julia --project=. benchmark/bench_schedule_pure.jl
#   done
#
# Add results to benchmark/RESULTS.md under "Pure search (isolated)".

using Chairmarks
using Dates
using ItinerarySearch

"""
    bench_schedule_pure()

Single-date pure-search benchmarks. Runs two scenarios:
1. `search_schedule(graph, universe_ua)` — `:direct UA`, 1 date
2. `search_schedule(graph, universe_all)` — `:direct all`, 1 date

Setup (ingest + graph build + universe enumeration) happens before the timed
block, so timings measure search work only.
"""
function bench_schedule_pure()
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    if !isfile(newssim_path)
        @error "demo dataset not found at $newssim_path"
        return
    end
    target = Date(2026, 2, 25)

    # Pay setup ONCE — all @be blocks below measure only the search phase.
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)

        universe_ua  = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], false)
        universe_all = ItinerarySearch._universe_from_carriers_direct(store, target, nothing, false)

        println("\n── Pure-Search Benchmarks (single date) ──")
        println("  nthreads=", Threads.nthreads())

        println("\n[bench-pure] search_schedule(graph, universe_ua) :direct UA ($(length(universe_ua.tuples)) markets)")
        r1 = @be search_schedule($graph, $universe_ua) seconds=3
        display(r1)
        ms1 = minimum(r1).time * 1000
        println("\n  min: $(round(ms1; digits=1)) ms")

        println("\n[bench-pure] search_schedule(graph, universe_all) :direct all ($(length(universe_all.tuples)) markets)")
        r2 = @be search_schedule($graph, $universe_all) seconds=3
        display(r2)
        ms2 = minimum(r2).time * 1000
        println("\n  min: $(round(ms2; digits=1)) ms")

        println("\n── Summary (min ms) ──")
        println("  RESULT nthreads=$(Threads.nthreads()) pure_direct_UA=$(round(ms1; digits=1))")
        println("  RESULT nthreads=$(Threads.nthreads()) pure_direct_all=$(round(ms2; digits=1))")
    finally
        close(store)
    end
end

"""
    bench_schedule_multidate_window()

Multi-date pure-search benchmark. Builds a wide-window graph spanning three
dates via `build_graph_for_window`, then searches a UA-filtered universe that
spans all three dates. Demonstrates the graph-reuse pattern: one build, many
searches across dates.

Setup (ingest + graph build + universe enumeration) happens before the timed
block.
"""
function bench_schedule_multidate_window()
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    if !isfile(newssim_path)
        @error "demo dataset not found at $newssim_path"
        return
    end
    # Three consecutive dates in the demo dataset's valid range.
    dates = Date(2026, 2, 25):Day(1):Date(2026, 2, 27)

    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph_for_window(store, config, dates)

        # Universe spanning all three dates, UA-filtered.
        universe_tuples = Tuple{String,String,Date}[]
        for d in dates
            partial = ItinerarySearch._universe_from_carriers_direct(store, d, ["UA"], false)
            append!(universe_tuples, partial.tuples)
        end
        universe = MarketUniverse(universe_tuples)

        println("\n── Pure-Search Benchmark (wide window, 3 dates) ──")
        println("  nthreads=", Threads.nthreads())
        println("\n[bench-pure] search_schedule(wide_graph, multi_date_universe) UA × 3 dates ($(length(universe.tuples)) markets)")
        r = @be search_schedule($graph, $universe) seconds=3
        display(r)
        ms = minimum(r).time * 1000
        println("\n  min: $(round(ms; digits=1)) ms")

        println("\n── Summary (min ms) ──")
        println("  RESULT nthreads=$(Threads.nthreads()) pure_direct_UA_3dates=$(round(ms; digits=1))")
    finally
        close(store)
    end
end

if abspath(PROGRAM_FILE) == @__FILE__
    bench_schedule_pure()
    bench_schedule_multidate_window()
end

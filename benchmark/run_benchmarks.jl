#!/usr/bin/env julia
# benchmark/run_benchmarks.jl — One-button benchmark suite
#
# Usage: make bench

using Dates

println("ItinerarySearch Benchmarks — $(today())")
println("="^60)

include("bench_ingest.jl")
include("bench_graph.jl")
include("bench_markets.jl")

using ItinerarySearch

config = SearchConfig()
if isfile(config.ssim_path)
    # Phase 1: Ingest
    bench_ingest(config.ssim_path)
    bench_full_pipeline(config)

    # Phase 2: Load schedule once for graph benchmarks
    println("\nLoading schedule for graph benchmarks...")
    store = DuckDBStore()
    load_schedule!(store, config)
    stats = table_stats(store)
    println("  Legs: $(stats.legs), Stations: $(stats.stations), MCT: $(stats.mct)")

    # Phase 3: Graph build + connection stats
    graph = bench_graph_build(store, config)
    bench_connection_build(store, config)
    bench_rule_chain(store, config)

    # Phase 4: DFS search
    bench_search(graph, store)

    # Phase 5: itinerary_legs (primary output interface)
    bench_itinerary_legs(graph, store)
    bench_itinerary_legs_multi(graph, store)

    # Phase 6: JSON serialization
    bench_json_output(graph, store)

    # Phase 7: Resolution
    bench_resolve(graph, store)

    # Phase 8: MCT lookup
    bench_mct_lookup(graph)

    # Phase 9: Trip search
    bench_trip_search(graph, store)

    # Phase 10: Logging overhead
    bench_logging_overhead(store, config)

    close(store)

    # Phase 11: Startup / load time
    println("\n── Startup Time ──")
    t_load = @elapsed using ItinerarySearch  # already loaded, measures re-import overhead
    println("  Module already loaded (re-import): $(round(t_load * 1000; digits=1))ms")
    sysimage_path = joinpath(@__DIR__, "..", "build", "ItinerarySearch.so")
    if Sys.isapple()
        sysimage_path = joinpath(@__DIR__, "..", "build", "ItinerarySearch.dylib")
    end
    if isfile(sysimage_path)
        sz = round(filesize(sysimage_path) / 1024^2; digits=1)
        println("  Sysimage: $sysimage_path ($sz MB)")
    else
        println("  Sysimage: not built (run `make sysimage` to create)")
    end
    println("  PrecompileTools: enabled (exercises core paths at precompile time)")

    # Phase 12: Parallel market search
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    if isfile(newssim_path)
        bench_market_search(newssim_path)
    else
        println("\nSkipping market search benchmark (sample_newssim.csv.gz not found).")
    end
else
    println("Demo data not found. Run extract_demo_data.jl first.")
    println("Skipping benchmarks.")
end

println("\n" * "="^60)
println("Done!")

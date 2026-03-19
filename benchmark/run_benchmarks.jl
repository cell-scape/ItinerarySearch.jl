#!/usr/bin/env julia
# benchmark/run_benchmarks.jl — One-button benchmark suite

using Dates

println("ItinerarySearch Benchmarks — $(today())")
println("="^60)

include("bench_ingest.jl")
include("bench_graph.jl")

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

    # Phase 3: Graph build
    graph = bench_graph_build(store, config)

    # Phase 4: Connection build stats
    bench_connection_build(store, config)

    # Phase 5: Layer 1
    bench_layer1(graph)

    # Phase 6: Search (without and with Layer 1)
    bench_search(graph, store)

    # Phase 7: Trip search
    bench_trip_search(graph, store)

    close(store)
else
    println("Demo data not found. Run extract_demo_data.jl first.")
    println("Skipping benchmarks.")
end

println("\n" * "="^60)
println("Done!")

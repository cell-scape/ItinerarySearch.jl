#!/usr/bin/env julia
# benchmark/run_benchmarks.jl — One-button benchmark suite

using Dates

println("ItinerarySearch Benchmarks — $(today())")
println("="^50)

include("bench_ingest.jl")

using ItinerarySearch

config = SearchConfig()
if isfile(config.ssim_path)
    bench_ingest(config.ssim_path)
    bench_full_pipeline(config)
else
    println("Demo data not found. Run extract_demo_data.jl first.")
    println("Skipping benchmarks.")
end

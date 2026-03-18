# benchmark/bench_ingest.jl — Ingest benchmarks

using ItinerarySearch
using Chairmarks

function bench_ingest(ssim_path::String)
    println("Benchmarking SSIM ingest: $ssim_path")
    b = @be begin
        store = DuckDBStore()
        ingest_ssim!(store, ssim_path)
        close(store)
    end
    display(b)
    println()
end

function bench_full_pipeline(config::SearchConfig)
    println("Benchmarking full pipeline:")
    b = @be begin
        store = DuckDBStore()
        load_schedule!(store, config)
        close(store)
    end
    display(b)
    println()
end

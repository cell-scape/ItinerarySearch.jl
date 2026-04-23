# benchmark/bench_markets.jl — Parallel market search benchmarks
#
# NOTE: search_markets() includes ingest + graph build + DFS search in one call.
# The ingest/build phase (~93% of wall time) is serial in both modes; only the
# per-market DFS search phase parallelizes. Speedup at this scale is therefore
# modest. Production gains emerge with many dates or pre-built graphs.

using ItinerarySearch
using Chairmarks
using Dates

function bench_market_search(newssim_path::String)
    markets = [
        ("ORD", "LHR"), ("ORD", "FRA"), ("ORD", "CDG"),
        ("EWR", "LHR"), ("EWR", "FRA"), ("EWR", "AMS"),
        ("IAH", "LHR"), ("IAH", "FRA"), ("IAH", "MAD"),
        ("SFO", "LHR"), ("SFO", "FRA"), ("SFO", "MUC"),
    ]
    target = Date(2026, 2, 25)

    println("\n── Parallel Market Search ($(length(markets)) markets, target=$target) ──")
    println("  Threads: $(Threads.nthreads())")

    println("\n  Sequential (parallel_markets=false):")
    b_seq = @be search_markets($newssim_path; markets=$markets, dates=$target,
                               parallel_markets=false) seconds=3
    display(b_seq)

    println("\n  Parallel (parallel_markets=true, nthreads=$(Threads.nthreads())):")
    b_par = @be search_markets($newssim_path; markets=$markets, dates=$target,
                               parallel_markets=true) seconds=3
    display(b_par)

    seq_ms = minimum(b_seq).time * 1000
    par_ms = minimum(b_par).time * 1000
    speedup = seq_ms / par_ms
    println("\n  Sequential (min):  $(round(seq_ms; digits=1)) ms")
    println("  Parallel   (min):  $(round(par_ms; digits=1)) ms")
    println("  Speedup:           $(round(speedup; digits=2))×")
    println("  (Search DFS is ~7% of wall time; ingest+build dominates)")
end

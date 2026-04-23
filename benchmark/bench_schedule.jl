# benchmark/bench_schedule.jl — benchmarks for Feature B (search_schedule)
# and the Feature A tuple-dispatch vector form.
#
# Runs under a fixed JULIA_NUM_THREADS (set by the caller) and prints
# labelled timings so a driver can parse and collate results across thread
# counts.
#
# NOTE: `:direct` universe is the default and representative. `:connected`
# is NOT exercised here — on the demo dataset it produces ~88k markets for
# UA, which would run for hours. Correctness of `:connected` is covered by
# the universe-enumeration tests.

using Chairmarks
using Dates
using ItinerarySearch

function bench_schedule()
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    if !isfile(newssim_path)
        println("Skipping bench_schedule: $newssim_path not found.")
        return
    end
    target = Date(2026, 2, 25)
    tuples = [
        ("ORD", "LHR", target),
        ("EWR", "LHR", target),
        ("ORD", "FRA", target),
        ("EWR", "FRA", target),
    ]

    println("\n── Schedule / Tuple-Dispatch Benchmarks ──")
    println("  nthreads=$(Threads.nthreads())")

    println("\n[bench] search_schedule :direct UA")
    b1 = @be search_schedule($newssim_path; dates=$target, carriers=["UA"],
                             universe=:direct) seconds=3
    display(b1)
    ms1 = minimum(b1).time * 1000
    println("\n  min: $(round(ms1; digits=1)) ms")

    println("\n[bench] search_schedule :direct all")
    b2 = @be search_schedule($newssim_path; dates=$target, carriers=nothing,
                             universe=:direct) seconds=3
    display(b2)
    ms2 = minimum(b2).time * 1000
    println("\n  min: $(round(ms2; digits=1)) ms")

    println("\n[bench] search_markets tuple vector (4 markets)")
    b3 = @be search_markets($newssim_path, $tuples) seconds=3
    display(b3)
    ms3 = minimum(b3).time * 1000
    println("\n  min: $(round(ms3; digits=1)) ms")

    println("\n── Summary (min ms) ──")
    println("  RESULT nthreads=$(Threads.nthreads()) schedule_direct_UA=$(round(ms1; digits=1))")
    println("  RESULT nthreads=$(Threads.nthreads()) schedule_direct_all=$(round(ms2; digits=1))")
    println("  RESULT nthreads=$(Threads.nthreads()) markets_tuple4=$(round(ms3; digits=1))")
end

# Callable from `julia --project=. benchmark/bench_schedule.jl`
if abspath(PROGRAM_FILE) == @__FILE__
    bench_schedule()
end

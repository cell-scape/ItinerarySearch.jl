include("_test_setup.jl")

@testset "parallel markets — failure isolation" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    @assert isfile(newssim_path) "demo dataset missing: $(newssim_path)"

    # NOTE on the "bad" market choice:
    # A station code like "XYZ" (3 chars) does NOT throw — StationCode is
    # InlineString3 which accepts 3 chars, and search_itineraries returns
    # Itinerary[] for unknown stations rather than raising. To reliably exercise
    # the try/catch-→sentinel path we need an input that genuinely throws.
    # A >3-char origin forces the StationCode(origin) constructor to raise an
    # ArgumentError ("string too large (N) to convert to InlineStrings.String3"),
    # which is the exception we want to catch and wrap in MarketSearchFailure.
    markets = [
        ("ORD", "LHR"),       # real market
        ("TOOLONG", "LHR"),   # bad origin (>3 chars) — forces StationCode to throw
        ("EWR", "LHR"),       # real market
    ]

    target = Date(2026, 2, 25)

    results = search_markets(
        newssim_path;
        markets, dates=target,
        parallel_markets=true,
    )

    # Real markets returned itinerary vectors (may be empty but typed correctly).
    @test results[("ORD", "LHR", target)] isa Vector{Itinerary}
    @test results[("EWR", "LHR", target)] isa Vector{Itinerary}

    # Bad market returned a failure sentinel.
    bad_key = ("TOOLONG", "LHR", target)
    @test haskey(results, bad_key)
    @test results[bad_key] isa MarketSearchFailure
    fail = results[bad_key]
    @test fail.market == bad_key
    @test fail.exception isa Exception
    @test !isempty(fail.backtrace)
    @test fail.worker_slot >= 0       # parallel: >=1; sequential fallback: 0
    @test fail.elapsed_ms >= 0.0

    # failed_markets helper finds exactly one failure.
    @test length(failed_markets(results)) == 1

    # is_failure dispatch is correct.
    @test is_failure(results[bad_key])
    @test !is_failure(results[("ORD", "LHR", target)])
end

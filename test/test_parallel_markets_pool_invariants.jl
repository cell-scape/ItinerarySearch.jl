include("_test_setup.jl")

@testset "parallel markets — worker pool invariants" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    @assert isfile(newssim_path) "demo dataset missing: $(newssim_path)"

    # Inject a bad market (7-char string breaks StationCode) to force one
    # worker into the catch branch. All workers' contexts must still return
    # to the pool (finally block). Other markets must still complete.
    markets = [
        ("ORD", "LHR"),
        ("TOOLONG", "LHR"),     # bad — forces StationCode to throw
        ("EWR", "LHR"),
        ("ORD", "FRA"),
    ]

    results = search_markets(
        newssim_path;
        markets, dates=Date(2026, 2, 25),
        parallel_markets=true,
    )

    # Every market must have exactly one entry (no dropped or duplicate results).
    for (origin, dest) in markets
        key = (origin, dest, Date(2026, 2, 25))
        @test haskey(results, key)
    end
    @test length(results) == length(markets)

    # The bad market produced a sentinel; the others produced real results.
    @test results[("TOOLONG", "LHR", Date(2026, 2, 25))] isa MarketSearchFailure
    @test results[("ORD", "LHR", Date(2026, 2, 25))] isa Vector{Itinerary}
    @test results[("EWR", "LHR", Date(2026, 2, 25))] isa Vector{Itinerary}
    @test results[("ORD", "FRA", Date(2026, 2, 25))] isa Vector{Itinerary}
end

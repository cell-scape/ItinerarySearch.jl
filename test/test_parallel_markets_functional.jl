include("_test_setup.jl")

@testset "parallel markets — functional equivalence with sequential" begin
    # Use the demo dataset for a deterministic, bounded-size run.
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    @assert isfile(newssim_path) "demo dataset missing: $(newssim_path)"

    markets = [
        ("ORD", "FRA"),
        ("ORD", "LHR"),
        ("EWR", "LHR"),
    ]
    target = Date(2026, 2, 25)

    seq = search_markets(
        newssim_path;
        markets, dates=target,
        parallel_markets=false,
    )
    par = search_markets(
        newssim_path;
        markets, dates=target,
        parallel_markets=true,
    )

    @test keys(seq) == keys(par)

    for k in keys(seq)
        # Both should be Vector{Itinerary} (no failures expected on demo data).
        @test seq[k] isa Vector{Itinerary}
        @test par[k] isa Vector{Itinerary}
        @test length(seq[k]) == length(par[k])
    end
end

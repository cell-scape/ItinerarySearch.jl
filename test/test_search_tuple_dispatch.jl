include("_test_setup.jl")

@testset "search_itineraries — tuple-dispatch" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    @assert isfile(newssim_path)

    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)
        ctx = RuntimeContext(
            config = config,
            constraints = SearchConstraints(),
            itn_rules = build_itn_rules(config),
        )

        @testset "single tuple form" begin
            canonical = search_itineraries(graph.stations, StationCode("ORD"), StationCode("LHR"), target, ctx)
            tuple_form = search_itineraries(graph.stations, ("ORD", "LHR", target), ctx)
            @test length(tuple_form) == length(canonical)
        end

        @testset "vector of tuples — preserves input order" begin
            tuples = [("ORD", "LHR", target), ("EWR", "LHR", target), ("ORD", "FRA", target)]

            # Snapshot expected lengths BEFORE the batch call so the comparison
            # doesn't alias the same ctx.results buffer the batch returns from.
            expected_lengths = [
                length(search_itineraries(graph.stations, StationCode(t[1]), StationCode(t[2]), t[3], ctx))
                for t in tuples
            ]

            results = search_itineraries(graph.stations, tuples, ctx)
            @test results isa Vector{Vector{Itinerary}}
            @test length(results) == 3
            for i in eachindex(tuples)
                @test length(results[i]) == expected_lengths[i]
            end

            # Assert results are NOT aliased — modifying results[1] must not
            # affect results[2].
            @test results[1] !== results[2]
            @test results[2] !== results[3]
        end

        @testset "empty vector returns empty output" begin
            empty_result = search_itineraries(graph.stations, Tuple{String,String,Date}[], ctx)
            @test empty_result isa Vector{Vector{Itinerary}}
            @test isempty(empty_result)
        end
    finally
        close(store)
    end
end

@testset "search — tuple-dispatch" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    target2 = Date(2026, 2, 26)

    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()

        @testset "single tuple form" begin
            # Both calls use source=:newssim — the test fixture is newssim-ingested.
            canonical = search(store, StationCode("ORD"), StationCode("LHR"), target;
                               config, source=:newssim)
            tuple_form = search(store, ("ORD", "LHR", target); config, source=:newssim)
            @test length(tuple_form) == length(canonical)
        end

        @testset "vector of tuples — preserves input order + no aliasing" begin
            tuples = [
                ("ORD", "LHR", target),
                ("EWR", "LHR", target),
                ("ORD", "LHR", target2),    # different date
            ]

            # Snapshot expected lengths BEFORE the batch call to avoid aliasing
            # the ctx.results buffer the batch returns from.
            # Use the single-tuple dispatch with source=:newssim — test fixture
            # is newssim-ingested, so pass explicitly now that the default is :ssim.
            expected_lengths = [
                length(search(store, t; config, source=:newssim))
                for t in tuples
            ]

            results = search(store, tuples; config, source=:newssim)
            @test results isa Vector{Vector{Itinerary}}
            @test length(results) == 3
            for i in eachindex(tuples)
                @test length(results[i]) == expected_lengths[i]
            end

            # Identity check: the three results must not alias each other.
            @test results[1] !== results[2]
            @test results[2] !== results[3]
        end

        @testset "empty vector returns empty output" begin
            results = search(store, Tuple{String,String,Date}[]; config, source=:newssim)
            @test results isa Vector{Vector{Itinerary}}
            @test isempty(results)
        end
    finally
        close(store)
    end
end

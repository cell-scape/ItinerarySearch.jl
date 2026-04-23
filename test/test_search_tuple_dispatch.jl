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
            results = search_itineraries(graph.stations, tuples, ctx)
            @test results isa Vector{Vector{Itinerary}}
            @test length(results) == 3

            # Each result matches the canonical single-tuple call at the same position
            for (i, t) in enumerate(tuples)
                canonical = search_itineraries(graph.stations, StationCode(t[1]), StationCode(t[2]), t[3], ctx)
                @test length(results[i]) == length(canonical)
            end
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

include("_test_setup.jl")

@testset "_search_markets_*_all_dates — prebuilt_graph skips build" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        prebuilt = build_graph!(store, config, target; source=:newssim)

        markets = [("ORD", "LHR"), ("EWR", "LHR")]
        results_from_build = Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}()
        results_lock_1 = ReentrantLock()
        ItinerarySearch._search_markets_sequential_all_dates(
            config, store, [target], markets, :newssim,
            Function[], nothing, results_from_build, results_lock_1,
            UInt128(0), UInt64(0), nothing,
        )

        results_from_prebuilt = Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}()
        results_lock_2 = ReentrantLock()
        ItinerarySearch._search_markets_sequential_all_dates(
            config, store, [target], markets, :newssim,
            Function[], nothing, results_from_prebuilt, results_lock_2,
            UInt128(0), UInt64(0), prebuilt,
        )

        @test keys(results_from_build) == keys(results_from_prebuilt)
        for k in keys(results_from_build)
            @test length(results_from_build[k]) == length(results_from_prebuilt[k])
        end
    finally
        close(store)
    end
end

@testset "dispatcher accepts store=nothing when prebuilt_graph supplied" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    prebuilt = try
        ingest_newssim!(store, newssim_path)
        build_graph!(store, SearchConfig(), target; source=:newssim)
    finally
        close(store)
    end
    # Store is closed. Dispatcher must still work because prebuilt_graph is supplied.
    results = Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}()
    results_lock = ReentrantLock()
    ItinerarySearch._search_markets_sequential_all_dates(
        SearchConfig(), nothing, [target], [("ORD", "LHR")], :newssim,
        Function[], nothing, results, results_lock,
        UInt128(0), UInt64(0), prebuilt,
    )
    @test haskey(results, ("ORD", "LHR", target))
end

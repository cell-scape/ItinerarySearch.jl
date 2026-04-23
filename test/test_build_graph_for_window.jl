include("_test_setup.jl")

@testset "build_graph_for_window — single date" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph_for_window(store, config, [target])
        @test graph.window_start == target
        @test graph.window_end >= target + Day(config.max_days - 1)
    finally
        close(store)
    end
end

@testset "build_graph_for_window — 3-date range" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    dates = Date(2026, 2, 25):Day(1):Date(2026, 2, 27)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph_for_window(store, config, dates)
        @test graph.window_start == first(dates)
        @test graph.window_end >= last(dates) + Day(config.max_days - 1)
    finally
        close(store)
    end
end

@testset "build_graph_for_window — unsorted date vector" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    dates = [Date(2026, 2, 27), Date(2026, 2, 25), Date(2026, 2, 26)]   # unsorted
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph_for_window(store, config, dates)
        @test graph.window_start == Date(2026, 2, 25)
        @test graph.window_end >= Date(2026, 2, 27) + Day(config.max_days - 1)
    finally
        close(store)
    end
end

@testset "build_graph_for_window — empty dates throws" begin
    store = DuckDBStore()
    try
        config = SearchConfig()
        @test_throws ArgumentError build_graph_for_window(store, config, Date[])
    finally
        close(store)
    end
end

@testset "build_graph_for_window — end-to-end with search_schedule(graph, universe)" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    dates = Date(2026, 2, 25):Day(1):Date(2026, 2, 26)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph_for_window(store, config, dates)

        # Build a universe spanning both dates
        universe_tuples = Tuple{String,String,Date}[]
        for d in dates
            partial = ItinerarySearch._universe_from_carriers_direct(store, d, ["UA"], false)
            # Take just a few markets per date to keep the test fast
            append!(universe_tuples, partial.tuples[1:min(3, length(partial.tuples))])
        end
        universe = MarketUniverse(universe_tuples)

        results = search_schedule(graph, universe)
        # Results should have entries for at least some of the requested markets
        @test !isempty(results)
    finally
        close(store)
    end
end

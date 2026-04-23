include("_test_setup.jl")

@testset "search_schedule(graph, universe) — functional equivalence" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    # Baseline: store-form call
    store_baseline = DuckDBStore()
    baseline_results = try
        ingest_newssim!(store_baseline, newssim_path)
        search_schedule(store_baseline; dates=target, carriers=["UA"])
    finally
        close(store_baseline)
    end

    # Graph form: pre-build graph + pre-compute universe, then search
    store = DuckDBStore()
    graph_results = try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)
        universe = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], false)
        search_schedule(graph, universe)
    finally
        close(store)
    end

    @test Set(keys(baseline_results)) == Set(keys(graph_results))
    for k in keys(baseline_results)
        @test length(baseline_results[k]) == length(graph_results[k])
    end
end

@testset "search_schedule(graph, universe) — window validation throws" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    far_future = Date(2030, 1, 1)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)

        bad_universe = MarketUniverse([("ORD", "LHR", far_future)])
        @test_throws ArgumentError search_schedule(graph, bad_universe)
    finally
        close(store)
    end
end

@testset "search_schedule(graph, universe) — empty universe returns empty dict" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)
        empty_universe = MarketUniverse(Tuple{String,String,Date}[])
        results = search_schedule(graph, empty_universe)
        @test isempty(results)
    finally
        close(store)
    end
end

@testset "search_schedule(graph, universe) — sink callback drains results" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)
        universe = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], false)

        count_ref = Ref(0)
        count_lock = ReentrantLock()
        sink = (market, result) -> begin
            lock(count_lock) do
                count_ref[] += 1
            end
        end

        results = search_schedule(graph, universe; sink)
        @test isempty(results)
        @test count_ref[] == length(universe.tuples)
    finally
        close(store)
    end
end

@testset "search_schedule(graph, universe) — root SpanEvent name is :search_schedule" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        config = SearchConfig()
        graph = build_graph!(store, config, target; source=:newssim)
        universe = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], false)

        events = SpanEvent[]
        events_lock = ReentrantLock()
        event_sink = ev -> begin
            if ev isa SpanEvent
                lock(events_lock) do
                    push!(events, ev)
                end
            end
        end

        _ = search_schedule(graph, universe; event_sinks=[event_sink])

        root_starts = filter(sp -> sp.kind === :start && sp.name === :search_schedule, events)
        @test length(root_starts) == 1
        root = root_starts[1]
        @test root.parent_span_id == 0
        @test root.attributes[:universe_mode] === :prebuilt
        @test haskey(root.attributes, :market_count)
        @test haskey(root.attributes, :date_count)
    finally
        close(store)
    end
end

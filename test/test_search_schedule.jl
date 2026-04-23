include("_test_setup.jl")

@testset "search_schedule — path form, :direct, carriers=nothing" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    results = search_schedule(newssim_path; dates=target, carriers=nothing, universe=:direct)
    @test results isa Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}
    @test !isempty(results)
    @test all(k -> k[3] == target, keys(results))
end

@testset "search_schedule — path form, :direct, UA filter" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    ua_results = search_schedule(newssim_path; dates=target, carriers=["UA"])
    all_results = search_schedule(newssim_path; dates=target, carriers=nothing)
    @test Set(keys(ua_results)) ⊆ Set(keys(all_results))
    # Demo dataset is UA-heavy; accept equality (UA covers all direct markets)
    # as well as the proper-subset case that would arise on a multi-carrier set.
    @test length(keys(ua_results)) <= length(keys(all_results))
end

@testset "search_schedule — store form matches path form" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    path_results = search_schedule(newssim_path; dates=target, carriers=["UA"])

    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        store_results = search_schedule(store; dates=target, carriers=["UA"])
        @test Set(keys(store_results)) == Set(keys(path_results))
    finally
        close(store)
    end
end

@testset "search_schedule — sink callback drains results" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    count_ref = Ref(0)
    count_lock = ReentrantLock()
    sink = (market, result) -> begin
        lock(count_lock) do
            count_ref[] += 1
        end
    end

    results = search_schedule(newssim_path; dates=target, carriers=["UA"], sink=sink)
    @test isempty(results)          # dict drained into sink
    @test count_ref[] > 0           # sink was invoked

    baseline = search_schedule(newssim_path; dates=target, carriers=["UA"])
    @test count_ref[] == length(baseline)
end

@testset "search_schedule — multi-date sweep" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    # Demo dataset starts on 2026-02-25 — pick two consecutive in-range dates.
    date1 = Date(2026, 2, 25)
    date2 = Date(2026, 2, 26)

    results = search_schedule(newssim_path; dates=[date1, date2], carriers=["UA"])
    dates_in_keys = Set(k[3] for k in keys(results))
    @test dates_in_keys == Set([date1, date2])
end

@testset "search_schedule — :connected universe at max_stops=0 degenerates to :direct" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    # At max_stops=0 only nonstops are valid, so :connected == :direct in terms
    # of which markets appear. This exercises the :connected code path without
    # paying the cost of a full connecting-itineraries enumeration.
    direct = search_schedule(newssim_path; dates=target, carriers=["UA"],
                              universe=:direct, max_stops=0)
    connected = search_schedule(newssim_path; dates=target, carriers=["UA"],
                                 universe=:connected, max_stops=0)

    @test Set(keys(direct)) == Set(keys(connected))
end

@testset "search_schedule — invalid universe symbol throws" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    @test_throws ArgumentError search_schedule(newssim_path; dates=target, universe=:bogus)
end

@testset "search_schedule — SpanEvent root name is :search_schedule" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)

    events = SpanEvent[]
    events_lock = ReentrantLock()
    event_sink = ev -> begin
        if ev isa SpanEvent
            lock(events_lock) do
                push!(events, ev)
            end
        end
    end

    _ = search_schedule(newssim_path; dates=target, carriers=["UA"], event_sinks=[event_sink])

    root_starts = filter(sp -> sp.kind === :start && sp.name === :search_schedule, events)
    @test length(root_starts) == 1
    root = root_starts[1]
    @test root.parent_span_id == 0
    @test haskey(root.attributes, :universe_mode)
    @test root.attributes[:universe_mode] === :direct
    @test haskey(root.attributes, :carriers)
    @test haskey(root.attributes, :include_codeshare)

    # Dispatcher must NOT emit its own :search_markets root when sweep owns it.
    search_markets_starts = filter(sp -> sp.kind === :start && sp.name === :search_markets, events)
    @test isempty(search_markets_starts)
end

include("_test_setup.jl")

@testset "parallel markets — cache warmth preserved across markets on a worker" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    @assert isfile(newssim_path) "demo dataset missing: $(newssim_path)"

    # Force sequential path so one ctx handles all markets.
    markets = [
        ("ORD", "LHR"),
        ("ORD", "FRA"),
        ("EWR", "LHR"),
        ("EWR", "FRA"),
        ("ORD", "CDG"),
    ]
    target = Date(2026, 2, 25)

    events = Vector{SpanEvent}()
    events_lock = ReentrantLock()
    collect_sink = ev -> begin
        if ev isa SpanEvent
            lock(events_lock) do
                push!(events, ev)
            end
        end
    end

    _ = search_markets(
        newssim_path;
        markets, dates=target,
        parallel_markets=false,
        event_sinks=[collect_sink],
    )

    # One :end per market + one root :end. Filter per-market :end events.
    market_ends = filter(ev -> ev.kind === :end && ev.name === :market_search, events)
    @test length(market_ends) == length(markets)

    # If caches were reset per-market, later markets might fail or return empty
    # results. We assert every market succeeded — a failure here signals a
    # cache-reset regression.
    @test all(ev.status === :ok for ev in market_ends)
end

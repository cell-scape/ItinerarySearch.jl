include("_test_setup.jl")

@testset "parallel markets — OTel span schema" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")

    markets = [
        ("ORD", "LHR"),
        ("EWR", "LHR"),
    ]
    target = Date(2026, 2, 25)

    # Capture events by injecting a collector sink via event_sinks kwarg.
    collected = Vector{Any}()
    collected_lock = ReentrantLock()
    collect_sink = ev -> begin
        lock(collected_lock) do
            push!(collected, ev)
        end
    end

    _ = search_markets(
        newssim_path;
        markets, dates=target,
        parallel_markets=true,
        event_sinks=[collect_sink],
    )

    spans = filter(ev -> ev isa SpanEvent, collected)
    @test !isempty(spans)

    # Exactly one trace id across all spans.
    @test length(unique(sp.trace_id for sp in spans)) == 1
    @test all(sp.trace_id != 0 for sp in spans)
    @test all(sp.span_id != 0 for sp in spans)

    # Exactly one root (search_markets) with parent_span_id == 0.
    root_starts = filter(sp -> sp.kind === :start && sp.name === :search_markets, spans)
    @test length(root_starts) == 1
    @test root_starts[1].parent_span_id == 0
    root_span_id = root_starts[1].span_id

    # Per-market spans have parent_span_id == root_span_id.
    market_starts = filter(sp -> sp.kind === :start && sp.name === :market_search, spans)
    @test length(market_starts) == length(markets)
    @test all(sp.parent_span_id == root_span_id for sp in market_starts)

    # Every :start has a matching :end with the same span_id.
    start_ids = Set(sp.span_id for sp in spans if sp.kind === :start)
    end_ids   = Set(sp.span_id for sp in spans if sp.kind === :end)
    @test start_ids == end_ids

    # End.unix_nano > Start.unix_nano for each span_id pair.
    by_id = Dict{UInt64, NamedTuple}()
    for sp in spans
        pair = get(by_id, sp.span_id, (start_ns=typemax(Int64), end_ns=typemin(Int64)))
        if sp.kind === :start
            by_id[sp.span_id] = (start_ns=sp.unix_nano, end_ns=pair.end_ns)
        else
            by_id[sp.span_id] = (start_ns=pair.start_ns, end_ns=sp.unix_nano)
        end
    end
    for (_, pair) in by_id
        @test pair.end_ns > pair.start_ns
    end

    # Statuses are :ok or :error only.
    @test all(sp.status ∈ (:ok, :error) for sp in spans if sp.kind === :end)
end

# test/test_getting_started_examples.jl — Smoke tests for every code block in
# the `docs/src/getting-started.md` tutorial and the NewSSIM-based
# `README.md` quick-start blocks.
#
# Each testset wraps one tutorial block verbatim and asserts only structural
# properties of the result (type / shape / non-emptiness). Content assertions
# (specific counts, specific keys, timing) are deliberately avoided so the
# tests stay stable across dataset refreshes and parallelism changes.
#
# The testsets are intentionally self-contained — each owns its own
# `newssim_path`, its own store/graph where relevant, and its own `close(store)`
# — so a failure in one block does not cascade into the next.
#
# If a testset fails, the fix is to REPAIR THE TUTORIAL EXAMPLE (in
# `docs/src/getting-started.md` or `README.md`), not to soften the assertion.
# That is the whole point of this file: to catch prose-doc rot.

include("_test_setup.jl")

# ============================================================================
# docs/src/getting-started.md — tutorial examples
# ============================================================================

@testset "getting-started §2 — Quick Start" begin
    using ItinerarySearch, Dates

    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_schedule(newssim_path;
        dates    = Date(2026, 2, 25),
        carriers = ["UA"],
    )

    length(results)                                          # number of markets
    results[("ORD", "LHR", Date(2026, 2, 25))]               # itineraries for one market

    @test results isa Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}
    @test !isempty(results)
    @test haskey(results, ("ORD", "LHR", Date(2026, 2, 25)))
end

@testset "getting-started §4.1 — path form (single date)" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_schedule(newssim_path;
        dates    = Date(2026, 2, 25),
        carriers = ["UA"],
    )
    length(results)                                     # → 1678 markets on this dataset

    @test results isa Dict
    @test !isempty(results)
end

@testset "getting-started §4.1 — path form (vector of dates)" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_schedule(newssim_path;
        dates    = [Date(2026, 2, 25), Date(2026, 2, 26), Date(2026, 2, 27)],
        carriers = ["UA"],
    )

    @test results isa Dict
    @test !isempty(results)
    # structural: at least one key per supplied date
    @test length(unique(k[3] for k in keys(results))) >= 1
end

@testset "getting-started §4.1 — store form (reuse ingested data)" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        # (optional) ingest_mct!(store, "path/to/mct.dat")

        results_ua = search_schedule(store; dates = Date(2026, 2, 25), carriers = ["UA"])
        results_aa = search_schedule(store; dates = Date(2026, 2, 25), carriers = ["AA"])

        @test results_ua isa Dict
        @test results_aa isa Dict
        @test !isempty(results_ua)
    finally
        close(store)
    end
end

@testset "getting-started §4.1 — graph form (reuse a pre-built graph)" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        graph    = build_graph!(store, SearchConfig(), Date(2026, 2, 25); source = :newssim)
        universe = ItinerarySearch._universe_from_carriers_direct(
            store, Date(2026, 2, 25), ["UA"], false,
        )
        results = search_schedule(graph, universe)
        length(results)

        @test results isa Dict
        @test !isempty(results)
        @test universe isa MarketUniverse
    finally
        close(store)
    end
end

@testset "getting-started §4.1 — :direct vs :connected universe" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    # :direct — fast, narrow
    direct = search_schedule(newssim_path;
        dates = Date(2026, 2, 25), carriers = ["UA"], universe = :direct,
    )

    # :connected — wider, slower (BFS along connections)
    connected = search_schedule(newssim_path;
        dates = Date(2026, 2, 25), carriers = ["UA"], universe = :connected,
    )

    @test direct isa Dict
    @test connected isa Dict
    @test !isempty(direct)
    @test !isempty(connected)
    # structural: connected universe is at least as large as direct
    @test length(connected) >= length(direct)
end

@testset "getting-started §4.1 — streaming via sink" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    count = Ref(0)
    sink = function(market, result)
        # `market` is (origin, dest, date); `result` is either Vector{Itinerary}
        # or MarketSearchFailure. Write to file, push to a queue, whatever suits
        # your streaming policy — just avoid growing memory.
        count[] += is_failure(result) ? 0 : 1
        return nothing
    end

    search_schedule(newssim_path;
        dates    = Date(2026, 2, 25),
        carriers = ["UA"],
        sink     = sink,
    )

    count[]   # number of markets streamed

    # Structural: the sink callback was invoked at least once.
    @test count[] > 0
end

@testset "getting-started §4.2 — search_markets kwargs form" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_markets(newssim_path;
        markets = [("ORD", "LHR"), ("DEN", "LAX")],
        dates   = Date(2026, 2, 25),
    )

    results[("ORD", "LHR", Date(2026, 2, 25))]     # itineraries for one market

    @test results isa Dict
    @test haskey(results, ("ORD", "LHR", Date(2026, 2, 25)))
    @test haskey(results, ("DEN", "LAX", Date(2026, 2, 25)))
end

@testset "getting-started §4.2 — tuple dispatch, single tuple" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_markets(newssim_path, ("ORD", "LHR", Date(2026, 2, 25)))

    @test results isa Dict
    @test haskey(results, ("ORD", "LHR", Date(2026, 2, 25)))
end

@testset "getting-started §4.2 — tuple dispatch, vector of tuples" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_markets(newssim_path, [
        ("ORD", "LHR", Date(2026, 2, 25)),
        ("EWR", "LHR", Date(2026, 2, 26)),
    ])
    # results has exactly 2 keys: ("ORD","LHR",Date(2026,2,25)) and ("EWR","LHR",Date(2026,2,26))

    @test results isa Dict
    @test length(results) == 2
    @test haskey(results, ("ORD", "LHR", Date(2026, 2, 25)))
    @test haskey(results, ("EWR", "LHR", Date(2026, 2, 26)))
end

@testset "getting-started §4.3 — search (single tuple on a pre-ingested store)" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        itns = search(store, ("ORD", "LHR", Date(2026, 2, 25)); source = :newssim)
        length(itns)

        @test itns isa Vector{Itinerary}
    finally
        close(store)
    end
end

@testset "getting-started §5 — handling results" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_schedule(newssim_path; dates = Date(2026, 2, 25), carriers = ["UA"])

    # Partition successes from failures
    fails = failed_markets(results)
    length(fails)                    # 0 on the demo dataset

    # Test a single value
    v = results[("ORD", "LHR", Date(2026, 2, 25))]
    is_failure(v)                    # false → v is a Vector{Itinerary}

    @test fails isa Vector{MarketSearchFailure}
    @test v isa Union{Vector{Itinerary}, MarketSearchFailure}
end

@testset "getting-started §6 — force sequential execution" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    results = search_schedule(newssim_path;
        dates            = Date(2026, 2, 25),
        carriers         = ["UA"],
        parallel_markets = false,
    )

    @test results isa Dict
    @test !isempty(results)
end

@testset "getting-started §7 — SearchConfig construction patterns" begin
    # Defaults
    config = SearchConfig()

    # Keyword overrides
    config = SearchConfig(max_stops = 3, max_elapsed_minutes = 2880)

    # From a Dict (e.g. YAML or environment-derived)
    config = SearchConfig(Dict(:max_stops => 3, :interline => "all"))

    @test config isa SearchConfig
    @test config.max_stops == 3
end

@testset "getting-started §7 — load_config from file" begin
    config = load_config(joinpath(pkgdir(ItinerarySearch), "config", "defaults.json"))

    @test config isa SearchConfig
end

@testset "getting-started §7 — DEFAULT_CIRCUITY_TIERS value" begin
    # The tutorial shows the REPL form: `julia> DEFAULT_CIRCUITY_TIERS`.
    # Strip the prompt and verify the exported constant has the documented shape.
    tiers = DEFAULT_CIRCUITY_TIERS

    @test tiers isa Vector{CircuityTier}
    @test !isempty(tiers)
    @test length(tiers) == 4
end

@testset "getting-started §7 — circuity_check_scope variants" begin
    a = SearchConfig(circuity_check_scope = :both)         # default
    b = SearchConfig(circuity_check_scope = :connection)   # prune early, skip itinerary
    c = SearchConfig(circuity_check_scope = :itinerary)    # defer to full-path check

    @test a isa SearchConfig
    @test b isa SearchConfig
    @test c isa SearchConfig
    @test a.circuity_check_scope == :both
    @test b.circuity_check_scope == :connection
    @test c.circuity_check_scope == :itinerary
end

@testset "getting-started §7 — strict SearchConstraints with tier overrides" begin
    strict = SearchConstraints(
        defaults = ParameterSet(
            circuity_tiers = [
                CircuityTier(500.0, 1.8),   # 0–500 mi
                CircuityTier(Inf,   1.2),   # 500+ mi — tight long-haul
            ],
            max_circuity   = 1.4,           # global ceiling, applied after tier lookup
        ),
    )

    @test strict isa SearchConstraints
    @test length(strict.defaults.circuity_tiers) == 2
end

@testset "getting-started §7 — schedule window override" begin
    config = SearchConfig(leading_days = 2, trailing_days = 3)

    @test config isa SearchConfig
    @test config.leading_days == 2
    @test config.trailing_days == 3
end

@testset "getting-started §8 — Observability via event_sinks" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    events = ItinerarySearch.SpanEvent[]
    event_lock = ReentrantLock()
    collector = function(ev::ItinerarySearch.SpanEvent)
        lock(event_lock) do
            push!(events, ev)
        end
        return nothing
    end

    results = search_schedule(newssim_path;
        dates       = Date(2026, 2, 25),
        carriers    = ["UA"],
        event_sinks = Function[collector],
    )

    length(events)           # start + end for each span emitted during the sweep
    events[1].name           # :search_schedule (root span)

    @test results isa Dict
    @test !isempty(events)
    @test first(events) isa ItinerarySearch.SpanEvent
end

@testset "getting-started §9 — search_itineraries, the DFS primitive" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    using DataFrames

    store = DuckDBStore()
    ingest_newssim!(store, newssim_path)

    target_date = Date(2026, 2, 25)
    config      = SearchConfig()
    graph       = build_graph!(store, config, target_date; source = :newssim)

    ctx = RuntimeContext(
        config      = config,
        constraints = SearchConstraints(),
        itn_rules   = build_itn_rules(config),
    )

    itineraries = copy(search_itineraries(
        graph.stations,
        StationCode("ORD"),
        StationCode("LHR"),
        target_date,
        ctx,
    ))

    close(store)

    @test itineraries isa Vector{Itinerary}
end

@testset "getting-started §9 — DataFrame wrappers" begin
    # The tutorial's wrapper block depends on an `itineraries` vector from the
    # preceding §9 search_itineraries example. For self-containment, recreate
    # that vector inline before running the wrapper calls verbatim.
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        target_date = Date(2026, 2, 25)
        config      = SearchConfig()
        graph       = build_graph!(store, config, target_date; source = :newssim)
        ctx = RuntimeContext(
            config      = config,
            constraints = SearchConstraints(),
            itn_rules   = build_itn_rules(config),
        )
        itineraries = copy(search_itineraries(
            graph.stations,
            StationCode("ORD"),
            StationCode("LHR"),
            target_date,
            ctx,
        ))

        # ── Verbatim tutorial block ──
        # One row per leg per itinerary — tidy/long format with MCT audit columns
        legs_df    = itinerary_legs_df(itineraries)

        # One row per itinerary — summary totals and joined flight-id strings
        summary_df = itinerary_summary_df(itineraries)

        # Wide pivot: legN_*/cnxN_* columns, side-by-side comparable
        pivot_df   = itinerary_pivot_df(itineraries; max_legs = 3)

        @test legs_df isa DataFrames.AbstractDataFrame
        @test summary_df isa DataFrames.AbstractDataFrame
        @test pivot_df isa DataFrames.AbstractDataFrame
    finally
        close(store)
    end
end

@testset "getting-started §9 — build_graph! manual build" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    store = DuckDBStore()
    ingest_newssim!(store, newssim_path)

    graph = build_graph!(store, SearchConfig(), Date(2026, 2, 25); source = :newssim)
    graph.build_stats                # instrumentation counts

    close(store)

    @test graph isa FlightGraph
    @test graph.build_stats isa BuildStats
end

@testset "getting-started §9 — build_graph_for_window multi-date amortization" begin
    newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

    store = DuckDBStore()
    ingest_newssim!(store, newssim_path)

    dates = [Date(2026, 2, 25), Date(2026, 2, 26), Date(2026, 2, 27)]
    graph = build_graph_for_window(store, SearchConfig(), dates)

    # Now feed a pre-computed universe straight to search_schedule(graph, universe):
    universe_tuples = Tuple{String,String,Date}[]
    for date in dates
        u = ItinerarySearch._universe_from_carriers_direct(store, date, ["UA"], false)
        append!(universe_tuples, u.tuples)
    end
    universe = MarketUniverse(universe_tuples)
    results  = search_schedule(graph, universe)

    close(store)

    @test graph isa FlightGraph
    @test universe isa MarketUniverse
    @test results isa Dict
    @test !isempty(results)
end

@testset "getting-started §9 — RuntimeContext direct construction" begin
    ctx = RuntimeContext(
        config      = SearchConfig(),
        constraints = SearchConstraints(),
        itn_rules   = build_itn_rules(SearchConfig()),
    )

    @test ctx isa RuntimeContext
end

# ============================================================================
# README.md — Quick Start examples (NewSSIM-based blocks)
# ============================================================================

@testset "README — search_markets convenience wrapper" begin
    # From README "NewSSIM Library Usage" section — first code block.
    using ItinerarySearch
    using Dates

    _newssim = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")
    _mct     = joinpath(pkgdir(ItinerarySearch), "data", "demo", "mct_demo.dat")

    results = search_markets(_newssim;
        markets  = [("ORD","LHR"), ("DEN","LAX"), ("IAH","EWR")],
        dates    = [Date(2026, 2, 26)],
        mct_path = _mct,
        max_stops = 2,
    )

    # Results are keyed by (origin, dest, date).
    # Values are Union{Vector{Itinerary}, MarketSearchFailure} — use is_failure(v) to
    # detect failed markets, or failed_markets(results) to extract them all at once.
    for ((org, dst, date), itns) in results
        is_failure(itns) && continue   # skip failed markets
        println("$(org)→$(dst) $(date): $(length(itns)) itineraries")
        for itn in itns
            println("  $(itn.num_stops)-stop, $(itn.elapsed_time) min, circuity $(round(itn.circuity; digits=2))x")
        end
    end

    @test results isa Dict
    @test !isempty(results)
end

@testset "README — low-level ingest_newssim + per-date graph loop" begin
    # From README "NewSSIM Library Usage" section — second code block.
    using ItinerarySearch
    using Dates

    _newssim = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")
    _mct     = joinpath(pkgdir(ItinerarySearch), "data", "demo", "mct_demo.dat")

    config = SearchConfig(max_stops=2)
    store  = DuckDBStore()

    ingest_newssim!(store, _newssim)
    ingest_mct!(store, _mct)

    ctx = RuntimeContext(
        config=config, constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
    )

    for target in [Date(2026, 2, 26)]
        graph = build_graph!(store, config, target; source=:newssim)
        for (origin, dest) in [(StationCode("ORD"), StationCode("LHR")),
                               (StationCode("DEN"), StationCode("LAX"))]
            itns = search_itineraries(graph.stations, origin, dest, target, ctx)
            println("$(origin)→$(dest) $(target): $(length(itns)) itineraries")
        end
    end

    close(store)

    @test ctx isa RuntimeContext
    @test config isa SearchConfig
end

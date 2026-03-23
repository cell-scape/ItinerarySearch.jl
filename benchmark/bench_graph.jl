# benchmark/bench_graph.jl — Graph build, search, and output benchmarks

using ItinerarySearch
using Chairmarks
using Dates

function bench_graph_build(store::DuckDBStore, config::SearchConfig)
    target = Date(2026, 3, 20)
    println("\n── Graph Build (target=$target) ──")
    b = @be build_graph!(store, config, $target)
    display(b)
    println()
    return build_graph!(store, config, target)
end

function bench_connection_build(store::DuckDBStore, config::SearchConfig)
    target = Date(2026, 3, 20)
    println("\n── Connection Build (isolated) ──")
    graph = build_graph!(store, config, target)
    n_cnx = sum(stn.stats.num_connections for (_, stn) in graph.stations; init=Int32(0))
    println("  Connections: $n_cnx across $(length(graph.stations)) stations")
    println("  Build time (from build_graph!): $(round(graph.build_stats.build_time_ns / 1e6; digits=1)) ms")
end

function bench_search(graph, store::DuckDBStore)
    target = Date(2026, 3, 20)
    config = SearchConfig()
    constraints = SearchConstraints()

    ods = [
        (StationCode("ORD"), StationCode("LHR"), "ORD→LHR (hub-to-hub intl)"),
        (StationCode("SFO"), StationCode("EWR"), "SFO→EWR (transcon)"),
        (StationCode("IAH"), StationCode("EWR"), "IAH→EWR (domestic)"),
        (StationCode("DEN"), StationCode("LAX"), "DEN→LAX (short domestic)"),
    ]

    println("\n── DFS Search ──")
    for (org, dst, label) in ods
        haskey(graph.stations, org) || continue
        haskey(graph.stations, dst) || continue

        ctx = RuntimeContext(
            config=config, constraints=constraints,
            itn_rules=build_itn_rules(config),
        )

        # Warmup
        search_itineraries(graph.stations, org, dst, target, ctx)

        b = @be begin
            empty!(ctx.results)
            search_itineraries(graph.stations, $org, $dst, $target, ctx)
        end
        n = length(ctx.results)
        print("  $label: $n itineraries  ")
        display(b)
    end
end

function bench_itinerary_legs(graph, store::DuckDBStore)
    target = Date(2026, 3, 20)
    config = SearchConfig()

    println("\n── itinerary_legs (single OD) ──")

    ods = [
        (StationCode("DEN"), StationCode("LAX"), "DEN→LAX"),
        (StationCode("ORD"), StationCode("SFO"), "ORD→SFO"),
        (StationCode("ORD"), StationCode("LHR"), "ORD→LHR"),
    ]

    for (org, dst, label) in ods
        haskey(graph.stations, org) || continue
        haskey(graph.stations, dst) || continue

        ctx = RuntimeContext(
            config=config, constraints=SearchConstraints(),
            itn_rules=build_itn_rules(config),
        )

        # Warmup
        itinerary_legs(graph.stations, org, dst, target, ctx)

        b = @be itinerary_legs(graph.stations, $org, $dst, $target, ctx)
        refs = itinerary_legs(graph.stations, org, dst, target, ctx)
        n_legs = sum(length(r.legs) for r in refs; init=0)
        print("  $label: $(length(refs)) itins, $(n_legs) legs  ")
        display(b)
    end
end

function bench_itinerary_legs_multi(graph, store::DuckDBStore)
    target = Date(2026, 3, 20)
    config = SearchConfig()

    println("\n── itinerary_legs_multi (5 paired ODs) ──")

    ctx = RuntimeContext(
        config=config, constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
    )

    origins = ["DEN", "ORD", "IAH", "ORD", "LFT"]
    dests   = ["LAX", "SFO", "EWR", "LHR", "YYZ"]

    # Warmup
    itinerary_legs_multi(graph.stations, ctx; origins=origins, destinations=dests, dates=target)

    b = @be itinerary_legs_multi(graph.stations, ctx; origins=$origins, destinations=$dests, dates=$target)
    result = itinerary_legs_multi(graph.stations, ctx; origins=origins, destinations=dests, dates=target)
    total_itins = sum(length(itins) for (_, od) in result for (_, dd) in od for (_, itins) in dd; init=0)
    print("  $(total_itins) total itineraries  ")
    display(b)
end

function bench_json_output(graph, store::DuckDBStore)
    target = Date(2026, 3, 20)
    config = SearchConfig()

    println("\n── JSON Output ──")

    ctx = RuntimeContext(
        config=config, constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
    )

    origins = ["DEN", "ORD"]
    dests   = ["LAX", "LHR"]

    # Full JSON
    itinerary_legs_json(graph.stations, ctx; origins=origins, destinations=dests, dates=target)
    b = @be itinerary_legs_json(graph.stations, ctx; origins=$origins, destinations=$dests, dates=$target)
    json = itinerary_legs_json(graph.stations, ctx; origins=origins, destinations=dests, dates=target)
    print("  Full JSON: $(round(length(json)/1024; digits=0))KB  ")
    display(b)

    # Compact JSON
    b = @be itinerary_legs_json(graph.stations, ctx; origins=$origins, destinations=$dests, dates=$target, compact=true)
    compact = itinerary_legs_json(graph.stations, ctx; origins=origins, destinations=dests, dates=target, compact=true)
    print("  Compact JSON: $(round(length(compact)/1024; digits=0))KB  ")
    display(b)
end

function bench_resolve(graph, store::DuckDBStore)
    target = Date(2026, 3, 20)
    config = SearchConfig()

    println("\n── resolve_leg / resolve_legs ──")

    ctx = RuntimeContext(
        config=config, constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
    )

    refs = itinerary_legs(graph.stations, StationCode("DEN"), StationCode("LAX"), target, ctx)
    isempty(refs) && return

    # Single leg from graph
    key = refs[1].legs[1]
    resolve_leg(key, graph)  # warmup
    b = @be resolve_leg($key, graph)
    print("  resolve_leg (graph): ")
    display(b)

    # Single leg from store
    resolve_leg(key, store)  # warmup
    b = @be resolve_leg($key, store)
    print("  resolve_leg (store): ")
    display(b)

    # Full itinerary from graph
    ref = refs[end]  # multi-stop
    resolve_legs(ref, graph)  # warmup
    b = @be resolve_legs($ref, graph)
    print("  resolve_legs (graph, $(length(ref.legs)) legs): ")
    display(b)

    # Full itinerary from store
    resolve_legs(ref, store)  # warmup
    b = @be resolve_legs($ref, store)
    print("  resolve_legs (store, $(length(ref.legs)) legs): ")
    display(b)
end

function bench_trip_search(graph, store::DuckDBStore)
    target_out = Date(2026, 3, 20)
    target_ret = Date(2026, 3, 27)

    println("\n── Trip Search (round-trip ORD→SFO) ──")
    haskey(graph.stations, StationCode("ORD")) || return
    haskey(graph.stations, StationCode("SFO")) || return

    config = SearchConfig()
    ctx = RuntimeContext(
        config=config, constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
    )

    legs = [
        TripLeg(origin=StationCode("ORD"), destination=StationCode("SFO"), date=target_out),
        TripLeg(origin=StationCode("SFO"), destination=StationCode("ORD"), date=target_ret, min_stay=60*12),
    ]

    # Warmup
    search_trip(store, graph, legs, ctx; max_per_leg=50, max_trips=100)

    b = @be search_trip($store, graph, $legs, ctx; max_per_leg=50, max_trips=100)
    trips = search_trip(store, graph, legs, ctx; max_per_leg=50, max_trips=100)
    println("  $(length(trips)) trips found")
    display(b)
    println()

    if !isempty(trips)
        best = trips[1]
        println("  Best: score=$(round(best.score; digits=1)), $(best.trip_type), $(best.total_elapsed)min")
    end
end

function bench_mct_lookup(graph)
    println("\n── MCT Lookup ──")
    lookup = graph.mct_lookup

    # Typical lookup
    lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"), StationCode("ORD"), MCT_DD)
    b = @be lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"), StationCode("ORD"), MCT_DD)
    print("  Station standard (ORD DD): ")
    display(b)

    # Global default fallback
    b = @be lookup_mct(lookup, AirlineCode("XX"), AirlineCode("XX"), StationCode("ZZZ"), MCT_II)
    print("  Global default fallback: ")
    display(b)
end

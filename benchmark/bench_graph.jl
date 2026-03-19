# benchmark/bench_graph.jl — Graph build, Layer 1, and search benchmarks

using ItinerarySearch
using Chairmarks
using Dates

function bench_graph_build(store::DuckDBStore, config::SearchConfig)
    target = Date(2026, 3, 20)
    println("\n── Graph Build (target=$target) ──")
    b = @be build_graph!(store, config, $target)
    display(b)
    println()

    # Return the graph for downstream benchmarks
    return build_graph!(store, config, target)
end

function bench_layer1(graph)
    println("\n── Layer 1 Build ──")
    # Reset layer1 each iteration
    b = @be begin
        empty!(graph.layer1)
        graph.layer1_built = false
        build_layer1!(graph)
    end
    display(b)
    println()

    n = sum(length(v) for (_, v) in graph.layer1; init=0)
    println("  Layer 1 entries: $n")
end

function bench_connection_build(store::DuckDBStore, config::SearchConfig)
    target = Date(2026, 3, 20)
    println("\n── Connection Build (isolated) ──")

    # Pre-build graph without connections to isolate connection building
    graph = build_graph!(store, config, target)
    n_cnx = sum(stn.stats.num_connections for (_, stn) in graph.stations; init=Int32(0))
    println("  Connections: $n_cnx across $(length(graph.stations)) stations")
    println("  Build time (from build_graph!): $(round(graph.build_stats.build_time_ns / 1e6; digits=1)) ms")
end

function bench_search(graph, store::DuckDBStore)
    target = Date(2026, 3, 20)

    # Pick some representative OD pairs
    ods = [
        (StationCode("ORD"), StationCode("LHR"), "ORD→LHR (hub-to-hub intl)"),
        (StationCode("SFO"), StationCode("EWR"), "SFO→EWR (transcon)"),
        (StationCode("IAH"), StationCode("LHR"), "IAH→LHR (intl)"),
        (StationCode("ORD"), StationCode("IAD"), "ORD→IAD (domestic short)"),
    ]

    config = SearchConfig()
    constraints = SearchConstraints()

    println("\n── DFS Search (without Layer 1) ──")
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

    if graph.layer1_built
        println("\n── DFS Search (with Layer 1) ──")
        for (org, dst, label) in ods
            haskey(graph.stations, org) || continue
            haskey(graph.stations, dst) || continue

            ctx = RuntimeContext(
                config=config, constraints=constraints,
                itn_rules=build_itn_rules(config),
                layer1_built=graph.layer1_built,
                layer1=graph.layer1,
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
end

function bench_trip_search(graph, store::DuckDBStore)
    target_out = Date(2026, 3, 20)
    target_ret = Date(2026, 3, 27)

    println("\n── Trip Search (round-trip ORD→LHR) ──")
    haskey(graph.stations, StationCode("ORD")) || return
    haskey(graph.stations, StationCode("LHR")) || return

    config = SearchConfig()
    ctx = RuntimeContext(
        config=config, constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
        layer1_built=graph.layer1_built,
        layer1=graph.layer1,
    )

    legs = [
        TripLeg(origin=StationCode("ORD"), destination=StationCode("LHR"), date=target_out),
        TripLeg(origin=StationCode("LHR"), destination=StationCode("ORD"), date=target_ret, min_stay=1440),
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

# scripts/common.jl — Shared setup for all demo/search scripts
#
# Loads schedule, builds graph, creates search context.
# Expects the caller to have `using ItinerarySearch, Dates` already.

function setup_environment(; target_date::Date = Date(2026, 3, 18), config_path::Union{String,Nothing} = nothing)
    config = config_path !== nothing ? load_config(config_path) : SearchConfig()
    constraints = config_path !== nothing ? load_constraints(config_path) : SearchConstraints()
    store = DuckDBStore()

    try
        load_schedule!(store, config)
    catch e
        if isa(e, SystemError) || isa(e, ArgumentError)
            println("Data not found. Run extract_demo_data.jl first:")
            println("  julia --project=. scripts/extract_demo_data.jl")
            exit(1)
        end
        rethrow(e)
    end

    stats = table_stats(store)
    println("Schedule: $(stats.legs) legs, $(stats.stations) stations, $(stats.mct) MCT rules")

    graph = build_graph!(store, config, target_date)
    build_ms = round(graph.build_stats.build_time_ns / 1.0e6; digits=0)
    bs = graph.build_stats
    println("Graph: $(length(graph.stations)) stations, $(length(graph.legs)) legs, built in $(build_ms)ms")
    println("  Connections: $(bs.total_connections), Pairs: $(bs.total_pairs_evaluated), MCT lookups: $(bs.mct_lookups)")
    geo = graph.geo_stats
    println("  Geo: $(length(geo.by_metro)) metros, $(length(geo.by_country)) countries, $(length(geo.by_region)) regions")

    ctx = RuntimeContext(
        config = config,
        constraints = constraints,
        itn_rules = build_itn_rules(config; constraints=constraints),
    )

    return (; config, store, graph, ctx, constraints)
end

# Default curated OD pairs
const DEFAULT_OD_PAIRS = Tuple{StationCode,StationCode}[
    (StationCode("DEN"), StationCode("LAX")),
    (StationCode("ORD"), StationCode("SFO")),
    (StationCode("IAH"), StationCode("EWR")),
    (StationCode("ORD"), StationCode("LHR")),
    (StationCode("LFT"), StationCode("YYZ")),
]

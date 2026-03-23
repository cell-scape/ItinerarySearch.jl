#!/usr/bin/env julia
# scripts/demo.jl — Zero-config demo: load data, show stats, query itineraries
#
# Usage: julia --project=. scripts/demo.jl

using ItinerarySearch
using Dates

println("ItinerarySearch.jl — Demo")
println("="^50)

# Create store with all defaults (loads demo data)
println("\nLoading demo data...")
config = SearchConfig()
store = DuckDBStore()

try
    load_schedule!(store, config)
catch e
    if isa(e, SystemError) || isa(e, ArgumentError)
        println("\nDemo data not found. Run extract_demo_data.jl first:")
        println("  julia --project=. scripts/extract_demo_data.jl")
        println("\nOr specify custom paths:")
        println("  SSIM_PATH=... MCT_PATH=... julia --project=. scripts/demo.jl")
        exit(1)
    end
    rethrow(e)
end

# Show stats
stats = table_stats(store)
println("\nTable Statistics:")
println("  Legs:          $(stats.legs)")
println("  DEI records:   $(stats.dei)")
println("  Stations:      $(stats.stations)")
println("  MCT rules:     $(stats.mct)")
println("  Expanded legs: $(stats.expanded_legs)")
println("  Segments:      $(stats.segments)")
println("  Markets:       $(stats.markets)")

# Example queries
println("\n" * "="^50)
println("Example Queries")
println("="^50)

# Station lookup
stn = query_station(store, StationCode("ORD"))
if stn !== nothing
    println("\nStation ORD: $(stn.metro_area), $(stn.country) ($(stn.lat), $(stn.lng))")
end

# Market distance
mkt_dist = query_market_distance(store, StationCode("ORD"), StationCode("LHR"))
if mkt_dist !== nothing
    println("ORD↔LHR market distance: $(round(mkt_dist; digits=1)) miles")
end

# Show config
println("\n" * "="^50)
println("Configuration")
println("="^50)
println("  Backend: $(config.backend)")
println("  Max stops: $(config.max_stops)")
println("  Max connection: $(config.max_connection_minutes) min")
println("  Circuity factor: $(config.circuity_factor)")
println("  Scope: $(config.scope)")
println("  Interline: $(config.interline)")

# ── Sample run: 10 random ODs × 3 days ───────────────────────────────────────

using Random
Random.seed!(42)

outdir = config.graph_export_path
mkpath(outdir)

start_date = Date(2026, 3, 18)
n_days = 3

# Build graph once for first date to pick OD pairs from available stations
first_graph = build_graph!(store, config, start_date)
all_stations = collect(keys(first_graph.stations))

# Curated OD pairs covering nonstop, 1-stop, and 2-stop itineraries
od_pairs = Tuple{StationCode,StationCode}[
    (StationCode("DEN"), StationCode("LAX")),   # short domestic — nonstop + 1-stop + 2-stop
    (StationCode("ORD"), StationCode("SFO")),   # transcon — nonstop + 1-stop + 2-stop
    (StationCode("IAH"), StationCode("EWR")),   # domestic — nonstop + 1-stop + 2-stop
    (StationCode("ORD"), StationCode("LHR")),   # hub-to-hub intl — 1-stop + 2-stop
    (StationCode("LFT"), StationCode("YYZ")),   # small city — 1-stop + 2-stop
]

println("\nSelected OD pairs:")
for (o, d) in od_pairs
    println("  $(o) → $(d)")
end

for day_offset in 0:(n_days - 1)
    target = start_date + Day(day_offset)
    t_day = time()
    println("\n" * "="^50)
    println("[$(target)] Building graph...")

    graph = build_graph!(store, config, target)
    build_ms = round(graph.build_stats.build_time_ns / 1.0e6; digits=0)

    # Layer 1 is disabled by default (experimental — may help in distributed scenarios)
    # if !import_layer1!(store, graph)
    #     build_layer1!(graph)
    #     export_layer1!(store, graph)
    # end

    println("[$(target)] $(length(graph.stations)) stations, $(length(graph.legs)) legs, built in $(build_ms)ms")

    # Write legs operating on this date
    legs_file = joinpath(outdir, "legs_$(target).psv")
    n_legs = open(legs_file, "w") do io
        write_legs(io, graph, target)
    end
    println("[$(target)] $(n_legs) legs → $(legs_file)")

    # Search selected OD pairs
    ctx = RuntimeContext(
        config = config,
        constraints = SearchConstraints(),
        itn_rules = build_itn_rules(config),
        layer1_built = graph.layer1_built,
        layer1 = graph.layer1,
    )

    itns_file = joinpath(outdir, "itineraries_$(target).psv")
    total_itns = 0
    total_rows = 0
    open(itns_file, "w") do io
        write_itineraries(io, Itinerary[], graph, target; header=true)

        for (origin, dest) in od_pairs
            t0 = time()
            itineraries = search_itineraries(graph.stations, origin, dest, target, ctx)
            dt = round(time() - t0; digits=2)

            if !isempty(itineraries)
                n = write_itineraries(io, copy(itineraries), graph, target; header=false)
                total_itns += length(itineraries)
                total_rows += n
                println("[$(target)]   $(origin)→$(dest): $(length(itineraries)) itineraries ($(n) rows) in $(dt)s")
            else
                println("[$(target)]   $(origin)→$(dest): no results ($(dt)s)")
            end
        end
    end

    # Write compact leg index — use flexible multi-search interface
    origins = [o for (o, _) in od_pairs]
    dests   = [d for (_, d) in od_pairs]

    result = itinerary_legs_multi(graph.stations, ctx;
        origins      = origins,
        destinations = dests,
        dates        = target,
    )

    # Write per-OD PSV files
    legs_dir = joinpath(outdir, "legs_index")
    mkpath(legs_dir)
    psv_header = join(["itinerary", "leg_pos", "row_number", "record_serial",
                        "airline", "flt_no", "operational_suffix", "itin_var",
                        "itin_var_overflow", "leg_seq", "svc_type",
                        "codeshare_airline", "codeshare_flt_no",
                        "org", "dst"], "|")
    for (dt, org_dict) in result
        for (org_s, dst_dict) in org_dict
            for (dst_s, itineraries) in dst_dict
                fname = joinpath(legs_dir, "$(org_s)_$(dst_s)_$(dt).psv")
                n_rows = 0
                open(fname, "w") do io
                    println(io, psv_header)
                    for (itn_idx, itn_ref) in enumerate(itineraries)
                        for (leg_pos, k) in enumerate(itn_ref.legs)
                            println(io, join([itn_idx, leg_pos,
                                              Int(k.row_number), Int(k.record_serial),
                                              strip(String(k.airline)), Int(k.flt_no),
                                              k.operational_suffix, Int(k.itin_var),
                                              k.itin_var_overflow, Int(k.leg_seq), k.svc_type,
                                              strip(String(k.codeshare_airline)), Int(k.codeshare_flt_no),
                                              strip(String(k.org)), strip(String(k.dst))], "|"))
                            n_rows += 1
                        end
                    end
                end
                println("[$(target)]   Leg index: $(org_s)→$(dst_s) $(length(itineraries)) itins, $(n_rows) rows → $(fname)")
            end
        end
    end

    # Write JSON for the same search
    json_file = joinpath(outdir, "legs_index_$(target).json")
    json = itinerary_legs_json(graph.stations, ctx;
        origins      = origins,
        destinations = dests,
        dates        = target,
    )
    write(json_file, json)
    println("[$(target)]   JSON: $(round(filesize(json_file) / 1024; digits=0))KB → $(json_file)")

    day_elapsed = round(time() - t_day; digits=1)
    println("[$(target)] Total: $(total_itns) itineraries, $(total_rows) rows → $(itns_file) ($(day_elapsed)s)")
end

# ── Generate visualizations for first day ─────────────────────────────────────
viz_dir = joinpath("data", "viz")
println("\n" * "="^50)
println("Generating visualizations → $(viz_dir)/")

target = start_date
graph  = build_graph!(store, config, target)

ctx_viz = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
    layer1_built = graph.layer1_built,
    layer1      = graph.layer1,
)

# Collect sample itineraries from all OD pairs on first day
sample_itns = Itinerary[]
for (origin, dest) in od_pairs
    itns = search_itineraries(graph.stations, origin, dest, target, ctx_viz)
    append!(sample_itns, copy(itns)[1:min(5, length(itns))])
    length(sample_itns) >= 20 && break
end

# Network map with highlighted sample itineraries
net_map_file = joinpath(viz_dir, "network_$(target).html")
viz_network_map(net_map_file, graph, target;
    itineraries = sample_itns,
    title       = "Flight Network — $(target)",
)
println("  Network map  → $(net_map_file)")

# Timeline for sample itineraries
timeline_file = joinpath(viz_dir, "timeline_$(target).html")
viz_timeline(timeline_file, sample_itns;
    title = "Itinerary Timeline — $(target)",
)
println("  Timeline     → $(timeline_file)")

# Trip comparison — use a known hub pair for reliable results
trip_od = (StationCode("ORD"), StationCode("SFO"))
trip_legs = [
    TripLeg(origin=trip_od[1], destination=trip_od[2], date=target),
    TripLeg(origin=trip_od[2], destination=trip_od[1], date=target + Day(3), min_stay=60*12),
]
trips = search_trip(store, graph, trip_legs, ctx_viz; max_trips=20)
trips_file = joinpath(viz_dir, "trips_$(target).html")
viz_trip_comparison(trips_file, trips;
    title  = "Trip Comparison: $(trip_od[1])↔$(trip_od[2]) — $(target)",
    top_n  = 10,
)
println("  Trip chart   → $(trips_file) ($(length(trips)) trips)")

close(store)
println("\nDone!")

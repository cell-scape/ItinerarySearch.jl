#!/usr/bin/env julia
# scripts/demo.jl — Zero-config demo: load data, show stats, query itineraries
#
# Usage: julia --project=. scripts/demo.jl              # SSIM path (default)
#        julia --project=. scripts/demo.jl --newssim    # NewSSIM CSV path

using ItinerarySearch
using Dates
using DBInterface

# Parse --newssim flag
use_newssim = "--newssim" in ARGS

println("ItinerarySearch.jl — Demo ($(use_newssim ? "NewSSIM CSV" : "SSIM") path)")
println("="^50)

# Create store with all defaults (loads demo data)
# Enable structured JSON logging and event log for the demo
println("\nLoading demo data...")
outdir_base = "data/output"
mkpath(outdir_base)

config = SearchConfig(
    log_json_path=joinpath(outdir_base, "demo.log"),
    log_level=:info,
    event_log_enabled=true,
    event_log_path=joinpath(outdir_base, "demo_events.jsonl"),
)
store = DuckDBStore()
graph_source = :ssim

if use_newssim
    # NewSSIM CSV path — load denormalized CSV + MCT
    newssim_path = joinpath("data", "demo", "sample_newssim.csv")
    mct_path = joinpath("data", "demo", "mct_demo.dat")
    if !isfile(newssim_path)
        println("\nNewSSIM demo data not found: $(newssim_path)")
        exit(1)
    end
    n = ingest_newssim!(store, newssim_path)
    println("  Loaded $(n) rows from NewSSIM CSV")
    ingest_mct!(store, mct_path)
    graph_source = :newssim
else
    # SSIM fixed-width path (default)
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
end

# Show stats
if use_newssim
    result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM newssim")
    n_newssim = first(result).n
    println("\nTable Statistics (NewSSIM):")
    println("  NewSSIM rows:  $(n_newssim)")
else
    stats = table_stats(store)
    println("\nTable Statistics:")
    println("  Legs:          $(stats.legs)")
    println("  DEI records:   $(stats.dei)")
    println("  Stations:      $(stats.stations)")
    println("  MCT rules:     $(stats.mct)")
    println("  Expanded legs: $(stats.expanded_legs)")
    println("  Segments:      $(stats.segments)")
    println("  Markets:       $(stats.markets)")
end

# Example queries
println("\n" * "="^50)
println("Example Queries")
println("="^50)

# Station lookup
if use_newssim
    stn = ItinerarySearch.query_newssim_station(store, StationCode("ORD"))
else
    stn = query_station(store, StationCode("ORD"))
end
if stn !== nothing
    println("\nStation ORD: $(stn.city), $(stn.country) ($(stn.latitude), $(stn.longitude))")
end

# Market distance (only available in SSIM path)
if !use_newssim
    mkt_dist = query_market_distance(store, StationCode("ORD"), StationCode("LHR"))
    if mkt_dist !== nothing
        println("ORD↔LHR market distance: $(round(mkt_dist; digits=1)) miles")
    end
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

start_date = use_newssim ? Date(2026, 2, 26) : Date(2026, 3, 18)
n_days = use_newssim ? 1 : 3

# Build graph once for first date to pick OD pairs from available stations
first_graph = build_graph!(store, config, start_date; source=graph_source)
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

for day_offset in 0:(n_days-1)
    target = start_date + Day(day_offset)
    t_day = time()
    println("\n" * "="^50)
    println("[$(target)] Building graph...")

    graph = build_graph!(store, config, target; source=graph_source)
    build_ms = round(graph.build_stats.build_time_ns / 1.0e6; digits=0)

    println("[$(target)] $(length(graph.stations)) stations, $(length(graph.legs)) legs, built in $(build_ms)ms")

    # Write legs operating on this date
    legs_file = joinpath(outdir, "legs_$(target).csv")
    n_legs = open(legs_file, "w") do io
        write_legs(io, graph, target)
    end
    println("[$(target)] $(n_legs) legs → $(legs_file)")

    # Search selected OD pairs
    ctx = RuntimeContext(
        config=config,
        constraints=SearchConstraints(),
        itn_rules=build_itn_rules(config),
    )

    itns_file = joinpath(outdir, "itineraries_$(target).csv")
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
    dests = [d for (_, d) in od_pairs]

    result = itinerary_legs_multi(graph.stations, ctx;
        origins=origins,
        destinations=dests,
        dates=target,
    )

    # Write per-OD CSV files
    legs_dir = joinpath(outdir, "legs_index")
    mkpath(legs_dir)
    csv_header = join(["itinerary", "leg_pos", "row_number", "record_serial",
            "carrier", "flight_number", "operational_suffix", "itinerary_var_id",
            "itinerary_var_overflow", "leg_sequence_number", "service_type",
            "administrating_carrier", "administrating_carrier_flight_number",
            "departure_station", "arrival_station"], ",")
    for (dt, org_dict) in result
        for (org_s, dst_dict) in org_dict
            for (dst_s, itineraries) in dst_dict
                fname = joinpath(legs_dir, "$(org_s)_$(dst_s)_$(dt).csv")
                n_rows = 0
                open(fname, "w") do io
                    println(io, csv_header)
                    for (itn_idx, itn_ref) in enumerate(itineraries)
                        for (leg_pos, k) in enumerate(itn_ref.legs)
                            println(io, join([itn_idx, leg_pos,
                                    Int(k.row_number), Int(k.record_serial),
                                    strip(String(k.carrier)), Int(k.flight_number),
                                    k.operational_suffix, Int(k.itinerary_var_id),
                                    k.itinerary_var_overflow, Int(k.leg_sequence_number), k.service_type,
                                    strip(String(k.administrating_carrier)), Int(k.administrating_carrier_flight_number),
                                    strip(String(k.departure_station)), strip(String(k.arrival_station))], ","))
                            n_rows += 1
                        end
                    end
                end
                println("[$(target)]   Leg index: $(org_s)→$(dst_s) $(length(itineraries)) itins, $(n_rows) rows → $(fname)")
            end
        end
    end

    # Write full JSON (with leg keys)
    json_file = joinpath(outdir, "legs_index_$(target).json")
    json = itinerary_legs_json(graph.stations, ctx;
        origins=origins, destinations=dests, dates=target,
    )
    write(json_file, json)
    println("[$(target)]   JSON (full): $(round(filesize(json_file) / 1024; digits=0))KB → $(json_file)")

    # Write compact JSON (summary only — flights, stops, no leg details)
    compact_file = joinpath(outdir, "legs_index_$(target)_compact.json")
    compact_json = itinerary_legs_json(graph.stations, ctx;
        origins=origins, destinations=dests, dates=target, compact=true,
    )
    write(compact_file, compact_json)
    println("[$(target)]   JSON (compact): $(round(filesize(compact_file) / 1024; digits=0))KB → $(compact_file)")

    # Interactive HTML table of ItineraryRefs
    ref_table_file = joinpath("data", "viz", "itinerary_refs_$(target).html")
    viz_itinerary_refs(ref_table_file, result;
        title="Itinerary References — $(target)",
    )
    println("[$(target)]   Ref table → $(ref_table_file)")

    # ── Tier 1 Instrumentation Summary ──────────────────────────────────────
    bs = graph.build_stats
    println("[$(target)] ── Build Stats ──")
    println("[$(target)]   Pairs evaluated: $(bs.total_pairs_evaluated)")
    println("[$(target)]   MCT lookups: $(bs.mct_lookups) (exc=$(bs.mct_exceptions) std=$(bs.mct_standards) def=$(bs.mct_defaults) supp=$(bs.mct_suppressions))")
    if bs.mct_lookups > 0
        println("[$(target)]   MCT avg time: $(round(bs.mct_avg_time; digits=1)) min")
    end
    cache_entries = length(ctx.mct_cache)
    if cache_entries > 0
        cache_hits = bs.mct_lookups - cache_entries
        println("[$(target)]   MCT cache: $(cache_entries) entries, $(cache_hits) hits ($(round(cache_hits / max(1, bs.mct_lookups) * 100; digits=0))%)")
    end
    if !isempty(bs.rule_pass)
        println("[$(target)]   Rules: $(sum(bs.rule_pass)) pass, $(sum(bs.rule_fail)) fail")
    end

    # Geographic stats
    geo = graph.geo_stats
    println("[$(target)]   Geo: $(length(geo.by_metro)) metros, $(length(geo.by_state)) states, $(length(geo.by_country)) countries, $(length(geo.by_region)) regions")

    # Search stats
    ss = ctx.search_stats
    println("[$(target)] ── Search Stats ──")
    println("[$(target)]   Queries: $(ss.queries), Paths found: $(ss.paths_found), Rejected: $(ss.paths_rejected)")
    println("[$(target)]   Max depth: $(ss.max_depth_reached)")
    println("[$(target)]   By stops: nonstop=$(ss.paths_by_stops[1]) 1-stop=$(ss.paths_by_stops[2]) 2-stop=$(ss.paths_by_stops[3]) 3+=$(ss.paths_by_stops[4])")
    println("[$(target)]   Search time: $(round(ss.search_time_ns / 1e6; digits=1)) ms")

    day_elapsed = round(time() - t_day; digits=1)
    println("[$(target)] Total: $(total_itns) itineraries, $(total_rows) rows → $(itns_file) ($(day_elapsed)s)")
end

# ── Generate visualizations for first day ─────────────────────────────────────
viz_dir = joinpath("data", "viz")
println("\n" * "="^50)
println("Generating visualizations → $(viz_dir)/")

target = start_date
graph = build_graph!(store, config, target; source=graph_source)

ctx_viz = RuntimeContext(
    config=config,
    constraints=SearchConstraints(),
    itn_rules=build_itn_rules(config),
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
    itineraries=sample_itns,
    title="Flight Network — $(target)",
)
println("  Network map  → $(net_map_file)")

# Timeline for sample itineraries
timeline_file = joinpath(viz_dir, "timeline_$(target).html")
viz_timeline(timeline_file, sample_itns;
    title="Itinerary Timeline — $(target)",
)
println("  Timeline     → $(timeline_file)")

# Trip comparison — use a known hub pair for reliable results
trip_od = (StationCode("ORD"), StationCode("SFO"))
trip_legs = [
    TripLeg(origin=trip_od[1], destination=trip_od[2], date=target),
    TripLeg(origin=trip_od[2], destination=trip_od[1], date=target + Day(3), min_stay=60 * 12),
]
trips = search_trip(store, graph, trip_legs, ctx_viz; max_trips=20)
trips_file = joinpath(viz_dir, "trips_$(target).html")
viz_trip_comparison(trips_file, trips;
    title="Trip Comparison: $(trip_od[1])↔$(trip_od[2]) — $(target)",
    top_n=10,
)
println("  Trip chart   → $(trips_file) ($(length(trips)) trips)")

close(store)

# ── Observability output summary ─────────────────────────────────────────────
println("\n" * "="^50)
println("Observability Output")
println("="^50)

log_path = config.log_json_path
if isfile(log_path)
    n_log = countlines(log_path)
    sz_log = round(filesize(log_path) / 1024; digits=0)
    println("  JSON log:   $(n_log) lines, $(sz_log)KB → $(log_path)")
else
    println("  JSON log:   (not generated)")
end

evt_path = config.event_log_path
if isfile(evt_path)
    n_evt = countlines(evt_path)
    sz_evt = round(filesize(evt_path) / 1024; digits=0)
    println("  Event log:  $(n_evt) events, $(sz_evt)KB → $(evt_path)")
else
    println("  Event log:  (not generated)")
end

println("\n" * "="^50)
println("CLI Quick Reference")
println("="^50)
println("  Search:   julia --project=. bin/itinsearch.jl search ORD LHR 2026-03-20")
println("  Trip:     julia --project=. bin/itinsearch.jl trip ORD LHR 2026-03-20 LHR ORD 2026-03-27")
println("  Build:    julia --project=. bin/itinsearch.jl build --date 2026-03-20")
println("  Ingest:   julia --project=. bin/itinsearch.jl ingest")
println("  Info:     julia --project=. bin/itinsearch.jl info")
println("  Sysimage: make sysimage  (then use --sysimage=build/ItinerarySearch.so)")
println()
println("  NewSSIM:  julia --project=. bin/itinsearch.jl --newssim data/demo/sample_newssim.csv search ORD LHR 2026-02-26")
println("  Demo:     make demo              (SSIM path)")
println("            make demo-newssim      (NewSSIM CSV path)")

println("\nDone!")

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

# Pick 10 random OD pairs from stations with departures
active = [s for s in all_stations if !isempty(first_graph.stations[s].departures)]
od_pairs = Tuple{StationCode,StationCode}[]
while length(od_pairs) < 10 && length(active) >= 2
    o = rand(active)
    d = rand(active)
    o == d && continue
    (o, d) in od_pairs && continue
    push!(od_pairs, (o, d))
end

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

    # Layer 1: import cached or build fresh
    if !import_layer1!(store, graph)
        build_layer1!(graph)
        export_layer1!(store, graph)
    end

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

    day_elapsed = round(time() - t_day; digits=1)
    println("[$(target)] Total: $(total_itns) itineraries, $(total_rows) rows → $(itns_file) ($(day_elapsed)s)")
end

close(store)
println("\nDone!")

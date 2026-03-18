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
d = query_market_distance(store, StationCode("ORD"), StationCode("LHR"))
if d !== nothing
    println("ORD↔LHR market distance: $(round(d; digits=1)) miles")
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

# ── Graph Engine ───────────────────────────────────────────────────────────────

println("\n" * "="^50)
println("Graph Engine")
println("="^50)

# Build the flight graph — use a date within the demo data range
target = Date(2026, 3, 20)
println("\nBuilding graph for $(target)...")
graph = build_graph!(store, config, target)

println("\nGraph Statistics:")
println("  Stations:    $(length(graph.stations))")
println("  Legs:        $(length(graph.legs))")
println("  Segments:    $(length(graph.segments))")
total_cnx = sum(stn.stats.num_connections for (_, stn) in graph.stations; init = Int32(0))
println("  Connections: $(total_cnx)")
println("  Build time:  $(round(graph.build_stats.build_time_ns / 1.0e6; digits=1)) ms")
println("  Window:      $(graph.window_start) – $(graph.window_end)")

# ── Itinerary Search ───────────────────────────────────────────────────────────

println("\n" * "="^50)
println("Itinerary Search")
println("="^50)

station_codes = collect(keys(graph.stations))
if length(station_codes) >= 2
    # Try ORD→LHR if both stations are present; otherwise use first/last in key list
    origin = StationCode("ORD") in station_codes ? StationCode("ORD") : station_codes[1]
    dest   = StationCode("LHR") in station_codes ? StationCode("LHR") : station_codes[end]

    println("\nSearching $(origin) → $(dest) on $(target)...")

    ctx = RuntimeContext(
        config = config,
        constraints = SearchConstraints(),
        itn_rules = build_itn_rules(config),
    )

    itineraries = copy(search_itineraries(graph.stations, origin, dest, target, ctx))

    println("Found $(length(itineraries)) itineraries")

    if !isempty(itineraries)
        # Show first few itineraries via Base.show
        n_show = min(5, length(itineraries))
        println("\nTop $(n_show) itineraries:")
        for i in 1:n_show
            println("  $(i). $(itineraries[i])")
        end

        # Wide format summary
        wide = itinerary_wide_format(itineraries)
        println("\nWide format (first $(n_show) rows):")
        for i in 1:n_show
            w = wide[i]
            println(
                "  $(i). $(w.flights)" *
                " | $(w.num_stops) stops" *
                " | $(w.elapsed_time) min" *
                " | $(round(w.total_distance; digits=0)) mi" *
                " | circ=$(round(w.circuity; digits=2))",
            )
        end

        # Long format summary
        long_rows = itinerary_long_format(itineraries)
        println("\nLong format: $(length(long_rows)) total leg rows across $(length(itineraries)) itineraries")
    end

    # Search stats
    println("\nSearch Statistics:")
    println("  Paths found:    $(ctx.search_stats.paths_found)")
    println("  Paths rejected: $(ctx.search_stats.paths_rejected)")
    println("  By stops (0,1,2,3+): $(ctx.search_stats.paths_by_stops[1:4])")
else
    println("\nNot enough stations for search demo (need >= 2, have $(length(station_codes)))")
end

close(store)
println("\nDone!")

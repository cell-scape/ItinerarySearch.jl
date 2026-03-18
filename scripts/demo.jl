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
    println("\nStation ORD: $(stn.city), $(stn.country) ($(stn.lat), $(stn.lng))")
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

close(store)
println("\nDone!")

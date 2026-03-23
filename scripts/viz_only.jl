#!/usr/bin/env julia
# scripts/viz_only.jl — Regenerate visualizations from cached search results
#
# Usage: julia --project=. scripts/viz_only.jl [DATE]

using ItinerarySearch
using Dates

include("common.jl")

target = length(ARGS) >= 1 ? Date(ARGS[1]) : Date(2026, 3, 18)

println("ItinerarySearch — Visualization Only")
println("="^50)
println("  Date: $(target)")
println()

env = setup_environment(; target_date=target)

viz_dir = joinpath("data", "viz")
mkpath(viz_dir)

od_pairs = DEFAULT_OD_PAIRS

# Search and build ItineraryRef table
origins = [o for (o, _) in od_pairs]
dests   = [d for (_, d) in od_pairs]
result = itinerary_legs_multi(env.graph.stations, env.ctx;
    origins=origins, destinations=dests, dates=target,
)

# ItineraryRef HTML table
ref_file = joinpath(viz_dir, "itinerary_refs_$(target).html")
viz_itinerary_refs(ref_file, result;
    title = "Itinerary References — $(target)",
)
println("  Ref table    → $(ref_file)")

# Network map
sample_itns = Itinerary[]
for (origin, dest) in od_pairs
    itns = search_itineraries(env.graph.stations, origin, dest, target, env.ctx)
    append!(sample_itns, copy(itns)[1:min(5, length(itns))])
    length(sample_itns) >= 20 && break
end

net_map_file = joinpath(viz_dir, "network_$(target).html")
viz_network_map(net_map_file, env.graph, target;
    itineraries = sample_itns,
    title       = "Flight Network — $(target)",
)
println("  Network map  → $(net_map_file)")

# Timeline
timeline_file = joinpath(viz_dir, "timeline_$(target).html")
viz_timeline(timeline_file, sample_itns;
    title = "Itinerary Timeline — $(target)",
)
println("  Timeline     → $(timeline_file)")

# Trip comparison
trip_od = (StationCode("ORD"), StationCode("SFO"))
trip_legs = [
    TripLeg(origin=trip_od[1], destination=trip_od[2], date=target),
    TripLeg(origin=trip_od[2], destination=trip_od[1], date=target + Day(3), min_stay=60*12),
]
trips = search_trip(env.store, env.graph, trip_legs, env.ctx; max_trips=20)
trips_file = joinpath(viz_dir, "trips_$(target).html")
viz_trip_comparison(trips_file, trips;
    title  = "Trip Comparison: $(trip_od[1])↔$(trip_od[2]) — $(target)",
    top_n  = 10,
)
println("  Trip chart   → $(trips_file) ($(length(trips)) trips)")

close(env.store)
println("\nDone!")

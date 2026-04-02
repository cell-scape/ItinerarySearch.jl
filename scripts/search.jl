#!/usr/bin/env julia
# scripts/search.jl — Single OD search with all outputs
#
# Usage:
#   julia --project=. scripts/search.jl ORD LHR 2026-03-20
#   julia --project=. scripts/search.jl ORD LHR              # defaults to today-ish date
#   julia --project=. scripts/search.jl ORD                   # all destinations from ORD

using ItinerarySearch
using Dates

include("common.jl")

# Parse args
origin = length(ARGS) >= 1 ? ARGS[1] : "ORD"
dest   = length(ARGS) >= 2 ? ARGS[2] : nothing
target = length(ARGS) >= 3 ? Date(ARGS[3]) : Date(2026, 3, 20)

println("ItinerarySearch — Search")
println("="^50)
println("  Origin:      $(origin)")
println("  Destination: $(dest === nothing ? "ALL" : dest)")
println("  Date:        $(target)")
println()

env = setup_environment(; target_date=target)

outdir = "data/output"
viz_dir = "data/viz"

# Search
origins_arg = origin
dests_arg = dest === nothing ? nothing : dest
result = itinerary_legs_multi(env.graph.stations, env.ctx;
    origins=origins_arg, destinations=dests_arg, dates=target,
)

# Count results
total_itins = 0
total_ods = 0
for (_d, org_dict) in result, (_o, dst_dict) in org_dict, (_ds, itins) in dst_dict
    global total_ods += 1
    global total_itins += length(itins)
end
println("\nFound $(total_itins) itineraries across $(total_ods) OD pairs")

# Write CSV files
legs_dir = joinpath(outdir, "legs_index")
mkpath(legs_dir)
csv_header = join(["itinerary", "leg_pos", "row_number", "record_serial",
                    "carrier", "flight_number", "operational_suffix", "itinerary_var_id",
                    "itinerary_var_overflow", "leg_sequence_number", "service_type",
                    "operating_carrier", "operating_flight_number",
                    "departure_station", "arrival_station"], ",")
for (dt, org_dict) in result
    for (org_s, dst_dict) in org_dict
        for (dst_s, itineraries) in dst_dict
            fname = joinpath(legs_dir, "$(org_s)_$(dst_s)_$(dt).csv")
            open(fname, "w") do io
                println(io, csv_header)
                for (itn_idx, itn_ref) in enumerate(itineraries)
                    for (leg_pos, k) in enumerate(itn_ref.legs)
                        println(io, join([itn_idx, leg_pos,
                                          Int(k.row_number), Int(k.record_serial),
                                          strip(String(k.carrier)), Int(k.flight_number),
                                          k.operational_suffix, Int(k.itinerary_var_id),
                                          k.itinerary_var_overflow, Int(k.leg_sequence_number), k.service_type,
                                          strip(String(k.operating_carrier)), Int(k.operating_flight_number),
                                          strip(String(k.departure_station)), strip(String(k.arrival_station))], ","))
                    end
                end
            end
            println("  CSV: $(org_s)→$(dst_s) → $(fname)")
        end
    end
end

# Write JSON (full + compact)
od_label = dest === nothing ? origin : "$(origin)_$(dest)"
json_file = joinpath(outdir, "search_$(od_label)_$(target).json")
json = itinerary_legs_json(env.graph.stations, env.ctx;
    origins=origins_arg, destinations=dests_arg, dates=target,
)
write(json_file, json)
println("  JSON (full):    $(round(filesize(json_file)/1024; digits=0))KB → $(json_file)")

compact_file = joinpath(outdir, "search_$(od_label)_$(target)_compact.json")
compact = itinerary_legs_json(env.graph.stations, env.ctx;
    origins=origins_arg, destinations=dests_arg, dates=target, compact=true,
)
write(compact_file, compact)
println("  JSON (compact): $(round(filesize(compact_file)/1024; digits=0))KB → $(compact_file)")

# Write interactive HTML table
mkpath(viz_dir)
ref_file = joinpath(viz_dir, "search_$(od_label)_$(target).html")
viz_itinerary_refs(ref_file, result;
    title = "Search: $(origin)$(dest === nothing ? " → ALL" : " → $(dest)") — $(target)",
)
println("  HTML table:     $(ref_file)")

# Network map with highlighted itineraries (first 20)
sample_itns = Itinerary[]
for (o, d) in [(StationCode(origin), StationCode(dest === nothing ? "LAX" : dest))]
    haskey(env.graph.stations, o) || continue
    haskey(env.graph.stations, d) || continue
    itns = search_itineraries(env.graph.stations, o, d, target, env.ctx)
    append!(sample_itns, copy(itns)[1:min(20, length(itns))])
end
if !isempty(sample_itns)
    map_file = joinpath(viz_dir, "search_$(od_label)_$(target)_map.html")
    viz_network_map(map_file, env.graph, target;
        itineraries=sample_itns,
        title="Network: $(origin)$(dest === nothing ? "" : " → $(dest)") — $(target)",
    )
    println("  Network map:    $(map_file)")
end

close(env.store)
println("\nDone!")

#!/usr/bin/env julia
# scripts/benchmark_quick.jl — Quick benchmark of graph build, MCT lookup, and search

using ItinerarySearch, Dates, Chairmarks, InlineStrings
import ItinerarySearch: lookup_mct, MCT_DD, AirlineCode, FlightNumber

println("Extended Rules Benchmark — $(today())")
println("="^60)

config = SearchConfig()
store = DuckDBStore()
println("Loading schedule...")
load_schedule!(store, config)

target = Date(2026, 3, 18)

println("\n── Graph Build ──")
t0 = time_ns()
graph = build_graph!(store, config, target)
build_ms = (time_ns() - t0) / 1e6
bs = graph.build_stats
println("  Stations: $(length(graph.stations)), Legs: $(length(graph.legs))")
println("  Connections: $(bs.total_connections)")
println("  Build time: $(round(build_ms; digits=0))ms")
println("  Pairs evaluated: $(bs.total_pairs_evaluated)")
if bs.total_pairs_evaluated > 0
    ns_per_pair = build_ms * 1e6 / bs.total_pairs_evaluated
    println("  ns/pair: $(round(ns_per_pair; digits=0))")
end
println("  MCT: lookups=$(bs.mct_lookups) supp=$(bs.mct_suppressions) exc=$(bs.mct_exceptions) std=$(bs.mct_standards) def=$(bs.mct_defaults)")

# MCT lookup microbenchmarks
println("\n── MCT Lookup ──")
lookup = graph.mct_lookup

b = @be lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"),
                    StationCode("ORD"), StationCode("ORD"), MCT_DD)
print("  Station standard (ORD DD): ")
display(b)

b = @be lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"),
                    StationCode("ORD"), StationCode("ORD"), MCT_DD;
                    arr_body='W', dep_body='N',
                    arr_is_codeshare=false, dep_is_codeshare=false,
                    arr_acft_type=InlineString7("789"), dep_acft_type=InlineString7("738"),
                    arr_flt_no=FlightNumber(1234), dep_flt_no=FlightNumber(567),
                    prv_country=InlineString3("GB"), nxt_country=InlineString3("US"),
                    prv_state=InlineString3(""), nxt_state=InlineString3("IL"),
                    prv_region=InlineString3("EUR"), nxt_region=InlineString3("NOA"),
                    target_date=UInt32(20260615))
print("  Full SSIM8 kwargs: ")
display(b)

# Search
println("\n── Search ──")
ctx = RuntimeContext(config=config, constraints=SearchConstraints(),
                     itn_rules=build_itn_rules(config))

for (org, dst) in [("ORD","LHR"), ("DEN","LAX"), ("IAH","EWR"), ("SFO","NRT")]
    t0 = time_ns()
    itns = search_itineraries(graph.stations, StationCode(org), StationCode(dst), target, ctx)
    ms = (time_ns() - t0) / 1e6
    println("  $(org)->$(dst): $(length(itns)) itineraries in $(round(ms; digits=1))ms")
end

close(store)
println("\nDone!")

# build/precompile_workload.jl — Precompile execution script for PackageCompiler
#
# This script exercises the main ItinerarySearch code paths to generate
# native code cache entries. Unlike runtests.jl, it avoids test-only
# dependencies (JET, Aqua, Chairmarks) that aren't available in the
# PackageCompiler build environment.

using ItinerarySearch
using Dates
using InlineStrings
using DuckDB, DBInterface
using JSON3

println("Precompile workload: exercising code paths...")

# ── Type construction ────────────────────────────────────────────────────────

config = SearchConfig()
constraints = SearchConstraints()
ps = ParameterSet()

stn_rec = StationRecord(
    code=StationCode("ORD"), country=InlineString3("US"),
    state=InlineString3("IL"), metro_area=InlineString3("CHI"),
    region=InlineString3("NOA"), lat=41.97, lng=-87.91, utc_offset=Int16(-300))

leg_key = LegKey(
    row_number=UInt64(1), record_serial=UInt32(1),
    airline=AirlineCode("UA"), flt_no=FlightNumber(1234),
    org=StationCode("ORD"), dst=StationCode("LHR"),
    operating_date=UInt32(20260315), dep_time=Minutes(540))

itn_ref = ItineraryRef(
    legs=[leg_key], num_stops=0, elapsed_minutes=Int32(480),
    flight_minutes=Int32(465), layover_minutes=Int32(0),
    distance_miles=Float32(3941), circuity=Float32(1.0))

origin(itn_ref)
destination(itn_ref)
flights_str(itn_ref)
route_str(itn_ref)

# ── MCT lookup ───────────────────────────────────────────────────────────────

lookup = MCTLookup()
result = lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"),
                    StationCode("ORD"), StationCode("ORD"), MCT_DD)
lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"),
           StationCode("ORD"), StationCode("ORD"), MCT_DD;
           arr_body='W', dep_body='N',
           arr_acft_type=InlineString7("789"), dep_acft_type=InlineString7("738"))

# ── Rule chains ──────────────────────────────────────────────────────────────

cnx_rules = build_cnx_rules(config, constraints, lookup)
itn_rules = build_itn_rules(config)

# ── JSON serialization ───────────────────────────────────────────────────────

JSON3.write(result)
JSON3.write(stn_rec)

# ── Observability ────────────────────────────────────────────────────────────

log = EventLog()
emit!(log, PhaseEvent(phase=:test, action=:start))
collect_system_metrics()

# ── DuckDB store + full pipeline ─────────────────────────────────────────────

store = DuckDBStore()

# Insert minimal test data
DBInterface.execute(store.db, """
INSERT INTO legs VALUES (
    1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
    'ORD', 'LHR', 540, 1320, 535, 1325,
    -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
    '2026-06-15', '2026-06-15', 127,
    'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
)
""")
DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)")
DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)")
post_ingest_sql!(store)

stats = table_stats(store)
println("  Tables: legs=$(stats.legs) stations=$(stats.stations)")

# Build graph
graph = build_graph!(store, config, Date(2026, 6, 15))
println("  Graph: $(length(graph.stations)) stations, $(length(graph.legs)) legs")

# Search
ctx = RuntimeContext(
    config=config, constraints=constraints,
    itn_rules=build_itn_rules(config),
)
search_itineraries(graph.stations, StationCode("ORD"), StationCode("LHR"), Date(2026, 6, 15), ctx)
println("  Search: $(ctx.search_stats.paths_found) paths found")

# itinerary_legs + JSON output
refs = itinerary_legs(graph.stations, StationCode("ORD"), StationCode("LHR"), Date(2026, 6, 15), ctx)
json = itinerary_legs_json(graph.stations, ctx;
    origins=["ORD"], destinations=["LHR"], dates=Date(2026, 6, 15))
println("  JSON: $(length(json)) bytes")

# Geographic stats
geo = graph.geo_stats
aggregate_geo_stats(graph.stations)

# CLI parser
ItinerarySearch.CLI._build_parser()

close(store)
println("Precompile workload complete.")

module ItinerarySearch

using ArgParse
using Dates
using InlineStrings
using CEnum
using UUIDs
using JSON3
using Logging
using LoggingExtras
using HTTP
using CSV
using DataFrames

# Type system (dependency order matters)
include("types/aliases.jl")
include("types/enums.jl")
include("types/records.jl")
include("types/status.jl")
include("types/stats.jl")
include("types/constraints.jl")
include("types/graph.jl")
include("config.jl")
include("observe/events.jl")
include("observe/metrics.jl")
include("observe/event_log.jl")
include("observe/sinks.jl")
include("observe/logging.jl")
include("compression.jl")
include("ingest/schemas.jl")
include("store/interface.jl")
include("store/julia_store.jl")
include("store/duckdb_store.jl")
include("store/schedule_queries.jl")
include("ingest/ssim.jl")
include("ingest/mct.jl")
include("ingest/reference.jl")
include("ingest/newssim.jl")
include("ingest/newssim_materialize.jl")
include("graph/mct_lookup.jl")
include("types/mct_trace.jl")
include("graph/mct_decode.jl")
include("graph/mct_trace.jl")
include("audit/mct_audit_log.jl")
include("audit/mct_replay.jl")
include("audit/mct_inspect.jl")
include("graph/rules_cnx.jl")
include("graph/rules_itn.jl")
include("graph/connect.jl")
include("graph/search.jl")
include("graph/builder.jl")
include("output/formats.jl")
include("output/viz.jl")
include("server.jl")
include("cli.jl")

# ── Public API ────────────────────────────────────────────────────────────────
# Only types and functions that library consumers need are exported.
# Everything else is accessible via ItinerarySearch.name or
# import ItinerarySearch: name.

# Types
export StationCode, Itinerary, Trip, TripLeg, FlightGraph, ConnectionRef

# Config
export SearchConfig, load_config, load_constraints
export ScopeMode, SCOPE_ALL, SCOPE_DOM, SCOPE_INTL
export InterlineMode, INTERLINE_ONLINE, INTERLINE_CODESHARE, INTERLINE_ALL

# Store
export DuckDBStore, table_stats, load_schedule!

# Ingest
export ingest_ssim!, ingest_mct!, ingest_newssim!

# Build & search
export build_graph!, search, search_markets
export search_itineraries, search_trip
export RuntimeContext, SearchConstraints, build_itn_rules

# Output
export write_legs, write_itineraries, write_trips
export itinerary_legs, itinerary_legs_multi, itinerary_legs_json

# Visualization
export viz_network_map, viz_timeline, viz_trip_comparison, viz_itinerary_refs

# ── Precompilation workload ────────────────────────────────────────────────────

using PrecompileTools

@setup_workload begin
    # Types and configs used in the workload
    @compile_workload begin
        # Config and constraints
        config = SearchConfig()
        constraints = SearchConstraints()
        ps = ParameterSet()

        # Core type construction
        stn_rec = StationRecord(
            code=StationCode("ORD"), country=InlineString3("US"),
            state=InlineString3("IL"), city=InlineString3("CHI"),
            region=InlineString3("NOA"), latitude=41.97, longitude=-87.91, utc_offset=Int16(-300))
        mct_result = MCTResult(
            time=Minutes(60), queried_status=MCT_DD, matched_status=MCT_DD,
            suppressed=false, source=SOURCE_GLOBAL_DEFAULT, specificity=UInt32(0))
        leg_key = LegKey(
            row_number=UInt64(1), record_serial=UInt32(1),
            carrier=AirlineCode("UA"), flight_number=FlightNumber(1234),
            departure_station=StationCode("ORD"), arrival_station=StationCode("LHR"),
            operating_date=UInt32(20260315), departure_time=Minutes(540))
        itn_ref = ItineraryRef(
            legs=[leg_key], num_stops=0, elapsed_minutes=Int32(480),
            flight_minutes=Int32(465), layover_minutes=Int32(0),
            distance_miles=Float32(3941), circuity=Float32(1.0))

        # Derived accessors
        origin(itn_ref)
        destination(itn_ref)
        flights_str(itn_ref)
        route_str(itn_ref)

        # MCT lookup (in-memory, no DuckDB)
        lookup = MCTLookup()
        lookup_mct(lookup, AirlineCode("UA"), AirlineCode("UA"),
                   StationCode("ORD"), StationCode("ORD"), MCT_DD)

        # Rule chain construction
        cnx_rules = build_cnx_rules(config, constraints, lookup)
        itn_rules = build_itn_rules(config)

        # JSON serialization
        JSON3.write(mct_result)
        JSON3.write(stn_rec)

        # Observability
        EventLog()
        collect_system_metrics()

        # DuckDB store (in-memory, no data files)
        store = DuckDBStore()
        table_stats(store)
        close(store)

        # CLI parser construction
        CLI._build_parser()
    end
end

end # module

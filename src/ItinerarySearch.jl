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
include("graph/mct_lookup.jl")
include("graph/rules_cnx.jl")
include("graph/rules_itn.jl")
include("graph/connect.jl")
include("graph/search.jl")
include("graph/builder.jl")
include("output/formats.jl")
include("output/viz.jl")
include("server.jl")
include("cli.jl")

# Exports — type aliases
export StationCode, AirlineCode, FlightNumber, Minutes, Distance, StatusBits

# Exports — status bits
export DOW_MON, DOW_TUE, DOW_WED, DOW_THU, DOW_FRI, DOW_SAT, DOW_SUN, DOW_MASK
export STATUS_INTERNATIONAL, STATUS_INTERLINE, STATUS_ROUNDTRIP
export STATUS_CODESHARE, STATUS_THROUGH, STATUS_WETLEASE
export is_international, is_interline, is_codeshare, is_roundtrip, is_through, is_wetlease
export dow_bit
export WILDCARD_STATION, WILDCARD_AIRLINE, WILDCARD_COUNTRY, WILDCARD_REGION, WILDCARD_FLIGHTNO
export NO_STATION, NO_AIRLINE, NO_MINUTES, NO_DISTANCE, NO_FLIGHTNO

# Exports — enums
export MCTStatus, MCT_DD, MCT_DI, MCT_ID, MCT_II
export MCTSource, SOURCE_EXCEPTION, SOURCE_STATION_STANDARD, SOURCE_GLOBAL_DEFAULT
export Cabin, CABIN_J, CABIN_O, CABIN_Y
export ScopeMode, SCOPE_ALL, SCOPE_DOM, SCOPE_INTL
export InterlineMode, INTERLINE_ONLINE, INTERLINE_CODESHARE, INTERLINE_ALL
export parse_mct_status, MCT_DEFAULTS

# Exports — record types
export LegKey, ItineraryRef, LegRecord, StationRecord, MCTResult, SegmentRecord
export origin, destination, stops, flights, flights_str, route_str
export flight_id, segment_id, full_id
export pack_date, unpack_date

# Exports — stats types (Subsystem 2 instrumentation)
export StationStats, BuildStats, SearchStats, MCTSelectionRow
export merge_build_stats!, merge_station_stats!
export GeoStats, aggregate_geo_stats

# Exports — constraints
export ParameterSet, MarketOverride, SearchConstraints
export resolve_params

# Exports — graph types (Subsystem 2)
export AbstractGraphNode, AbstractGraphEdge
export GraphStation, GraphLeg, GraphSegment, GraphConnection, Itinerary, Trip
export TripLeg, TripScoringWeights
export nonstop_connection

# Exports — config
export SearchConfig, load_config

# Exports — observe: event types
export SystemMetricsEvent, PhaseEvent, BuildSnapshotEvent, SearchSnapshotEvent, CustomEvent

# Exports — observe: event log
export EventLog, emit!, checkpoint!, with_phase, collect_system_metrics

# Exports — observe: sinks
export JsonlSink, stdout_sink

# Exports — observe: logging
export setup_logger

# Exports — ingest
export ingest_ssim!
export ingest_mct!
export load_airports!, load_regions!, load_oa_control!, load_aircrafts!

# Exports — store interface
export AbstractStore, JuliaStore, DuckDBStore
export load_schedule!, query_legs, query_station, query_mct
export get_departures, get_arrivals
export query_market_distance, query_segment, query_segment_stops, table_stats
export post_ingest_sql!
export query_schedule_legs, query_schedule_segments

# Exports — graph: MCT lookup
export MCTRecord, MCTLookup, MCTCacheKey, lookup_mct, materialize_mct_lookup
export MCT_BIT_ARR_CARRIER, MCT_BIT_DEP_CARRIER
export MCT_BIT_ARR_TERM, MCT_BIT_DEP_TERM
export MCT_BIT_PRV_STN, MCT_BIT_NXT_STN
export MCT_BIT_PRV_COUNTRY, MCT_BIT_NXT_COUNTRY
export MCT_BIT_PRV_REGION, MCT_BIT_NXT_REGION
export MCT_BIT_DEP_BODY, MCT_BIT_ARR_BODY
export MCT_BIT_ARR_CS_IND, MCT_BIT_ARR_CS_OP
export MCT_BIT_DEP_CS_IND, MCT_BIT_DEP_CS_OP
export MCT_BIT_ARR_ACFT_TYPE, MCT_BIT_DEP_ACFT_TYPE
export MCT_BIT_ARR_FLT_RNG, MCT_BIT_DEP_FLT_RNG
export MCT_BIT_PRV_STATE, MCT_BIT_NXT_STATE

# Exports — graph: connection rules
export check_cnx_roundtrip, check_cnx_scope, check_cnx_interline
export check_cnx_opdays, check_cnx_suppcodes, check_cnx_trfrest
export MCTRule, MAFTRule, CircuityRule
export build_cnx_rules
export PASS, FAIL_ROUNDTRIP, FAIL_SCOPE, FAIL_ONLINE, FAIL_CODESHARE, FAIL_INTERLINE
export FAIL_TIME_MIN, FAIL_TIME_MAX, FAIL_OPDAYS, FAIL_SUPPCODE
export FAIL_MAFT, FAIL_CIRCUITY, FAIL_TRFREST

# Exports — graph: itinerary rules
export check_itn_scope, check_itn_opdays, check_itn_circuity
export check_itn_suppcodes, check_itn_maft
export build_itn_rules
export FAIL_ITN_SCOPE, FAIL_ITN_OPDAYS, FAIL_ITN_CIRCUITY
export FAIL_ITN_SUPPCODE, FAIL_ITN_MAFT

# Exports — graph: connection builder
export build_connections_at_station!, build_connections!

# Exports — graph: DFS search
export RuntimeContext, search_itineraries, search_trip, score_trip

# Exports — graph: builder and FlightGraph
export FlightGraph, build_graph!, search

# Exports — output formats
export itinerary_long_format, itinerary_wide_format
export write_legs, write_itineraries, write_trips
export itinerary_legs, itinerary_legs_multi, itinerary_legs_json
export resolve_leg, resolve_segment, resolve_legs

# Exports — visualizations
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
            state=InlineString3("IL"), metro_area=InlineString3("CHI"),
            region=InlineString3("NOA"), lat=41.97, lng=-87.91, utc_offset=Int16(-300))
        mct_result = MCTResult(
            time=Minutes(60), queried_status=MCT_DD, matched_status=MCT_DD,
            suppressed=false, source=SOURCE_GLOBAL_DEFAULT, specificity=UInt32(0))
        leg_key = LegKey(
            row_number=UInt64(1), record_serial=UInt32(1),
            airline=AirlineCode("UA"), flt_no=FlightNumber(1234),
            org=StationCode("ORD"), dst=StationCode("LHR"),
            operating_date=UInt32(20260315), dep_time=Minutes(540))
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

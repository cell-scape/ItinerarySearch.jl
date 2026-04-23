# test/_test_setup.jl — Shared imports used by both the serial runner
# (runtests.jl) and the parallel runner (runtests_parallel.jl). Pulls in every
# internal symbol referenced by any test file, so individual test files don't
# need their own import blocks.

using Test
using ItinerarySearch
using InlineStrings
using Dates
using JSON3
using CSV
import DataFrames

# Internal symbols used by test files — not part of the public API
import ItinerarySearch:
    # Type aliases
    AirlineCode, FlightNumber, Minutes, Distance, StatusBits,
    # Status bits & helpers
    DOW_MON, DOW_TUE, DOW_WED, DOW_THU, DOW_FRI, DOW_SAT, DOW_SUN, DOW_MASK,
    STATUS_INTERNATIONAL, STATUS_INTERLINE, STATUS_ROUNDTRIP,
    STATUS_CODESHARE, STATUS_THROUGH, STATUS_WETLEASE, STATUS_CNX_OP_THROUGH,
    is_international, is_interline, is_codeshare, is_roundtrip, is_through, is_wetlease,
    is_cnx_op_through,
    dow_bit,
    WILDCARD_STATION, WILDCARD_AIRLINE, WILDCARD_COUNTRY, WILDCARD_REGION, WILDCARD_FLIGHTNO,
    NO_STATION, NO_AIRLINE, NO_MINUTES, NO_DISTANCE, NO_FLIGHTNO,
    # Enums
    MCTStatus, MCT_DD, MCT_DI, MCT_ID, MCT_II,
    MCTSource, SOURCE_EXCEPTION, SOURCE_STATION_STANDARD, SOURCE_GLOBAL_DEFAULT,
    Cabin, CABIN_J, CABIN_O, CABIN_Y,
    parse_mct_status, MCT_DEFAULTS,
    # Record types
    LegKey, ItineraryRef, LegRecord, StationRecord, MCTResult, SegmentRecord,
    origin, destination, stops, flights, flights_str, route_str,
    flight_id, segment_id, full_id,
    pack_date, unpack_date,
    # Stats
    StationStats, BuildStats, SearchStats, MCTSelectionRow,
    merge_build_stats!, merge_station_stats!,
    GeoStats, aggregate_geo_stats,
    # Constraints
    ParameterSet, MarketOverride, resolve_params,
    # Graph types
    AbstractGraphNode, AbstractGraphEdge,
    GraphStation, GraphLeg, GraphSegment, GraphConnection,
    TripScoringWeights, nonstop_connection,
    # Observe
    SystemMetricsEvent, PhaseEvent, BuildSnapshotEvent, SearchSnapshotEvent, CustomEvent,
    SpanEvent, TraceContext,
    _new_trace_id, _new_span_id, _unix_nano_now,
    EventLog, emit!, checkpoint!, with_phase, collect_system_metrics,
    JsonlSink, stdout_sink,
    setup_logger,
    # Ingest helpers
    detect_delimiter,
    load_airports!, load_regions!, load_oa_control!, load_aircrafts!,
    # Store internals
    AbstractStore, JuliaStore,
    query_legs, query_station, query_mct,
    get_departures, get_arrivals,
    query_market_distance, query_segment, query_segment_stops,
    post_ingest_sql!,
    query_schedule_legs, query_schedule_segments,
    query_direct_markets_by_carriers, query_codeshare_partners,
    # MCT lookup
    MCTRecord, MCTLookup, MCTCacheKey, lookup_mct, lookup_mct_traced, materialize_mct_lookup,
    MCT_BIT_ARR_CARRIER, MCT_BIT_DEP_CARRIER,
    MCT_BIT_ARR_TERM, MCT_BIT_DEP_TERM,
    MCT_BIT_PRV_STN, MCT_BIT_NXT_STN,
    MCT_BIT_PRV_COUNTRY, MCT_BIT_NXT_COUNTRY,
    MCT_BIT_PRV_REGION, MCT_BIT_NXT_REGION,
    MCT_BIT_DEP_BODY, MCT_BIT_ARR_BODY,
    MCT_BIT_ARR_CS_IND, MCT_BIT_ARR_CS_OP,
    MCT_BIT_DEP_CS_IND, MCT_BIT_DEP_CS_OP,
    MCT_BIT_ARR_ACFT_TYPE, MCT_BIT_DEP_ACFT_TYPE,
    MCT_BIT_ARR_FLT_RNG, MCT_BIT_DEP_FLT_RNG,
    MCT_BIT_PRV_STATE, MCT_BIT_NXT_STATE,
    # MCT bitmask decoder
    decode_matched_fields,
    # MCT audit trace types
    EMPTY_MCT_RESULT, MCTCandidateTrace, MCTTrace, MCTAuditConfig,
    # MCT audit log
    MCTAuditLog, open_audit_log, write_audit_entry!, close_audit_log,
    # MCT replay
    replay_misconnects, parse_misconnect_row,
    # MCT inspector
    mct_inspect, InspectorState,
    # Connection rules
    check_cnx_roundtrip, check_cnx_backtrack, check_cnx_scope, check_cnx_interline,
    check_cnx_opdays, check_cnx_suppcodes, check_cnx_trfrest,
    MCTRule, MAFTRule, CircuityRule, ConnectionTimeRule, ConnectionGeoRule,
    build_cnx_rules,
    PASS, FAIL_ROUNDTRIP, FAIL_SCOPE, FAIL_ONLINE, FAIL_CODESHARE, FAIL_INTERLINE,
    FAIL_TIME_MIN, FAIL_TIME_MAX, FAIL_OPDAYS, FAIL_SUPPCODE,
    FAIL_MAFT, FAIL_CIRCUITY, FAIL_TRFREST, FAIL_BACKTRACK, FAIL_GEO,
    # Itinerary rules
    check_itn_scope, check_itn_opdays, check_itn_circuity_range,
    check_itn_suppcodes, check_itn_maft,
    check_itn_elapsed_range, check_itn_distance_range, check_itn_stops_range,
    check_itn_flight_time, check_itn_layover_time,
    check_itn_carriers, check_itn_interline_dcnx, check_itn_crs_cnx,
    FAIL_ITN_SCOPE, FAIL_ITN_OPDAYS, FAIL_ITN_CIRCUITY,
    FAIL_ITN_SUPPCODE, FAIL_ITN_MAFT,
    FAIL_ITN_ELAPSED, FAIL_ITN_DISTANCE, FAIL_ITN_STOPS,
    FAIL_ITN_FLIGHT_TIME, FAIL_ITN_LAYOVER, FAIL_ITN_CARRIER,
    FAIL_ITN_INTERLINE_DCNX, FAIL_ITN_CRS_CNX,
    # Connection builder
    build_connections_at_station!, build_connections!,
    # Search internals
    score_trip,
    # Output internals (now exported; kept here for historical test imports)
    resolve_leg, resolve_segment, resolve_legs,
    # Failure sentinel
    MarketSearchFailure, is_failure, failed_markets,
    # Universe enumeration
    MarketUniverse

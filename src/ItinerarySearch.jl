module ItinerarySearch

using Dates
using InlineStrings
using CEnum

# Type system (dependency order matters)
include("types/aliases.jl")
include("types/enums.jl")
include("types/records.jl")
include("types/status.jl")
include("types/stats.jl")
include("types/constraints.jl")
include("types/graph.jl")
include("config.jl")
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
export LegRecord, StationRecord, MCTResult, SegmentRecord
export flight_id, segment_id, full_id
export pack_date, unpack_date

# Exports — stats types (Subsystem 2 instrumentation)
export StationStats, BuildStats, SearchStats, MCTSelectionRow
export merge_build_stats!, merge_station_stats!

# Exports — constraints
export ParameterSet, MarketOverride, SearchConstraints
export resolve_params

# Exports — graph types (Subsystem 2)
export GraphStation, GraphLeg, GraphSegment, GraphConnection, Itinerary
export nonstop_connection

# Exports — config
export SearchConfig, load_config

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
export MCTRecord, MCTLookup, lookup_mct, materialize_mct_lookup
export MCT_BIT_ARR_CARRIER, MCT_BIT_DEP_CARRIER
export MCT_BIT_ARR_TERM, MCT_BIT_DEP_TERM
export MCT_BIT_PRV_STN, MCT_BIT_NXT_STN
export MCT_BIT_PRV_COUNTRY, MCT_BIT_NXT_COUNTRY
export MCT_BIT_PRV_REGION, MCT_BIT_NXT_REGION
export MCT_BIT_DEP_BODY, MCT_BIT_ARR_BODY

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

end # module

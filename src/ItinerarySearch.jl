module ItinerarySearch

using Dates
using InlineStrings
using CEnum

# Type system (dependency order matters)
include("types/aliases.jl")
include("types/enums.jl")
include("types/records.jl")
include("config.jl")
include("compression.jl")
include("ingest/schemas.jl")

# Exports — type aliases
export StationCode, AirlineCode, FlightNumber, Minutes, Distance, StatusBits

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

# Exports — config
export SearchConfig, load_config

end # module

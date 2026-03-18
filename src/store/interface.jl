# src/store/interface.jl — AbstractStore trait and required method signatures

"""
    abstract type AbstractStore

Interface for data backends. The graph engine only sees `AbstractStore` —
the concrete backend (DuckDB, Julia vectors, Redshift) is swapped at
construction time via config.

Required methods for any concrete backend:
- `load_schedule!(store, config)` — Load all data from files into the store
- `query_legs(store, origin, destination, date)` — Legs for an O-D pair on a date
- `query_station(store, code)` — Station reference data
- `query_mct(store, arr_carrier, dep_carrier, station, status; kwargs...)` — MCT lookup
- `get_departures(store, station, date)` — All departing legs from a station on a date
- `get_arrivals(store, station, date)` — All arriving legs to a station on a date
- `query_market_distance(store, stn_a, stn_b)` — NDOD market distance
- `query_segment(store, segment_hash)` — Segment aggregates
- `query_segment_stops(store, segment_hash)` — Board/off points for multi-leg segments
- `table_stats(store)` — Row counts for all tables
"""
abstract type AbstractStore end

"""
    `load_schedule!(store::AbstractStore, config::SearchConfig)`
---

# Description
- Load all schedule and reference data from files described in `config` into the store
- Mutates `store` in place; any previously loaded data may be cleared

# Arguments
1. `store::AbstractStore`: the backend to populate
2. `config::SearchConfig`: file paths and search parameters

# Returns
- `::AbstractStore`: the mutated store (for chaining)
"""
function load_schedule!(store::AbstractStore, config::SearchConfig)
    error("load_schedule! not implemented for $(typeof(store))")
end

"""
    `query_legs(store::AbstractStore, origin::StationCode, destination::StationCode, date::Date)`
---

# Description
- Return all legs departing `origin` and arriving `destination` on `date`
- Filters to legs whose `operating_date` matches the packed representation of `date`

# Arguments
1. `store::AbstractStore`: the backend
2. `origin::StationCode`: departure station code
3. `destination::StationCode`: arrival station code
4. `date::Date`: operating date

# Returns
- `::Vector{LegRecord}`: matching leg records (may be empty)
"""
function query_legs(store::AbstractStore, origin::StationCode, destination::StationCode, date::Date)
    error("query_legs not implemented for $(typeof(store))")
end

"""
    `query_station(store::AbstractStore, code::StationCode)`
---

# Description
- Return reference data for a single station by its IATA code

# Arguments
1. `store::AbstractStore`: the backend
2. `code::StationCode`: the IATA airport code

# Returns
- `::Union{StationRecord, Nothing}`: station record, or `nothing` if not found
"""
function query_station(store::AbstractStore, code::StationCode)
    error("query_station not implemented for $(typeof(store))")
end

"""
    `query_mct(store::AbstractStore, arr_carrier::AirlineCode, dep_carrier::AirlineCode,
               station::StationCode, status::MCTStatus; kwargs...)`
---

# Description
- Look up the Minimum Connecting Time for a connection at `station`
- Follows the SSIM8 specificity hierarchy: exception → station standard → global default
- `status` encodes the domestic/international crossing (DD/DI/ID/II)

# Arguments
1. `store::AbstractStore`: the backend
2. `arr_carrier::AirlineCode`: arriving flight carrier
3. `dep_carrier::AirlineCode`: departing flight carrier
4. `station::StationCode`: connecting station
5. `status::MCTStatus`: connection traffic type (MCT_DD, MCT_DI, MCT_ID, MCT_II)

# Returns
- `::MCTResult`: MCT value, source, specificity, and suppression flag
"""
function query_mct(store::AbstractStore, arr_carrier::AirlineCode, dep_carrier::AirlineCode,
                   station::StationCode, status::MCTStatus; kwargs...)
    error("query_mct not implemented for $(typeof(store))")
end

"""
    `get_departures(store::AbstractStore, station::StationCode, date::Date)`
---

# Description
- Return all legs departing from `station` on `date`
- Used by the graph engine to enumerate forward edges from a node

# Arguments
1. `store::AbstractStore`: the backend
2. `station::StationCode`: departure station code
3. `date::Date`: operating date

# Returns
- `::Vector{LegRecord}`: all departing legs (may be empty)
"""
function get_departures(store::AbstractStore, station::StationCode, date::Date)
    error("get_departures not implemented for $(typeof(store))")
end

"""
    `get_arrivals(store::AbstractStore, station::StationCode, date::Date)`
---

# Description
- Return all legs arriving at `station` on `date`
- Used by the graph engine to enumerate backward edges into a node

# Arguments
1. `store::AbstractStore`: the backend
2. `station::StationCode`: arrival station code
3. `date::Date`: operating date

# Returns
- `::Vector{LegRecord}`: all arriving legs (may be empty)
"""
function get_arrivals(store::AbstractStore, station::StationCode, date::Date)
    error("get_arrivals not implemented for $(typeof(store))")
end

"""
    `query_market_distance(store::AbstractStore, stn_a::StationCode, stn_b::StationCode)`
---

# Description
- Return the NDOD market (great-circle) distance between two stations in miles
- Used for circuity filtering during connection building

# Arguments
1. `store::AbstractStore`: the backend
2. `stn_a::StationCode`: first station code
3. `stn_b::StationCode`: second station code

# Returns
- `::Float32`: great-circle distance in miles, or `0f0` if either station is unknown
"""
function query_market_distance(store::AbstractStore, stn_a::StationCode, stn_b::StationCode)
    error("query_market_distance not implemented for $(typeof(store))")
end

"""
    `query_segment(store::AbstractStore, segment_hash::UInt64)`
---

# Description
- Return aggregate segment data for a segment identified by its hash key
- The segment hash encodes airline, flight number, itin variant, operating date, and service type

# Arguments
1. `store::AbstractStore`: the backend
2. `segment_hash::UInt64`: the precomputed segment identity hash

# Returns
- `::Union{SegmentRecord, Nothing}`: segment record, or `nothing` if not found
"""
function query_segment(store::AbstractStore, segment_hash::UInt64)
    error("query_segment not implemented for $(typeof(store))")
end

"""
    `query_segment_stops(store::AbstractStore, segment_hash::UInt64)`
---

# Description
- Return all intermediate board/off points for a multi-leg segment
- For direct (1-leg) segments, returns an empty vector

# Arguments
1. `store::AbstractStore`: the backend
2. `segment_hash::UInt64`: the precomputed segment identity hash

# Returns
- `::Vector{LegRecord}`: individual leg records for the segment's stops (may be empty)
"""
function query_segment_stops(store::AbstractStore, segment_hash::UInt64)
    error("query_segment_stops not implemented for $(typeof(store))")
end

"""
    `table_stats(store::AbstractStore)`
---

# Description
- Return row counts for all tables in the store
- Used for diagnostics, logging, and test assertions

# Arguments
1. `store::AbstractStore`: the backend

# Returns
- `::Dict{String, Int}`: map from table name to row count
"""
function table_stats(store::AbstractStore)
    error("table_stats not implemented for $(typeof(store))")
end

"""
    `query_schedule_legs(store::AbstractStore, window_start::Date, window_end::Date)`
---

# Description
- Return all schedule-level leg records whose effective period overlaps the window
- Returns one record per schedule entry (unexpanded), not per operating date
- `operating_date` is set to `eff_date`; `day_of_week` is 0 (use `frequency`)

# Arguments
1. `store::AbstractStore`: the backend
2. `window_start::Date`: start of the search window (inclusive)
3. `window_end::Date`: end of the search window (inclusive)

# Returns
- `::Vector{LegRecord}`: one record per schedule entry active during the window
"""
function query_schedule_legs(store::AbstractStore, window_start::Date, window_end::Date)
    error("query_schedule_legs not implemented for $(typeof(store))")
end

"""
    `query_schedule_segments(store::AbstractStore, window_start::Date, window_end::Date)`
---

# Description
- Return schedule-level segment aggregates, grouped by segment identity without
  expanding by operating date
- Returns one record per unique segment active during the window

# Arguments
1. `store::AbstractStore`: the backend
2. `window_start::Date`: start of the search window (inclusive)
3. `window_end::Date`: end of the search window (inclusive)

# Returns
- `::Vector{SegmentRecord}`: one record per unique schedule segment in the window
"""
function query_schedule_segments(store::AbstractStore, window_start::Date, window_end::Date)
    error("query_schedule_segments not implemented for $(typeof(store))")
end

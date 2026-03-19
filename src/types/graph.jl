# src/types/graph.jl — Subsystem 2 graph types
#
# Type ordering note: GraphStation and GraphLeg are mutually referential.
# GraphStation holds departure/arrival/connection vectors; GraphLeg holds
# org/dst station references. Both also contain forward references to
# GraphConnection. Julia does not support forward declarations, so the
# circular dependency is broken by typing the intra-graph vectors as
# Vector{Any}. In practice, these vectors only ever hold the stated concrete
# type. If hot-path performance demands it, typed wrapper arrays can be
# introduced later without changing the struct API.
#
# Dependency order:
#   GraphSegment    — no intra-graph references
#   GraphStation    — holds Vector{Any} for departures / arrivals / connections
#   GraphLeg        — references GraphStation + GraphSegment; holds Vector{Any}
#   GraphConnection — references GraphLeg + GraphStation
#   Itinerary       — references GraphConnection (concrete Vector)
#
# Zero-arg defaults: @kwdef generates a zero-arg keyword-dispatch stub for every
# struct it processes. Defining a separate TypeName() outer constructor would
# overwrite that stub and cause a precompilation error. Instead, each struct
# with non-optional record fields uses a module-level const sentinel as the
# @kwdef default expression. Sentinels are declared in the dependency order
# shown above, interleaved with the struct definitions that need them.

# ── Record sentinels (declared before graph structs that embed them) ──────────

const _ZERO_LEG_RECORD = LegRecord(
    airline=AirlineCode(""),
    flt_no=FlightNumber(0),
    operational_suffix=' ',
    itin_var=UInt8(0),
    itin_var_overflow=' ',
    leg_seq=UInt8(0),
    svc_type=' ',
    org=StationCode(""),
    dst=StationCode(""),
    pax_dep=Minutes(0),
    pax_arr=Minutes(0),
    ac_dep=Minutes(0),
    ac_arr=Minutes(0),
    dep_utc_offset=Int16(0),
    arr_utc_offset=Int16(0),
    dep_date_var=Int8(0),
    arr_date_var=Int8(0),
    eqp=InlineString7(""),
    body_type=' ',
    dep_term=InlineString3(""),
    arr_term=InlineString3(""),
    aircraft_owner=AirlineCode(""),
    operating_date=UInt32(0),
    day_of_week=UInt8(0),
    eff_date=UInt32(0),
    disc_date=UInt32(0),
    frequency=UInt8(0),
    mct_status_dep=' ',
    mct_status_arr=' ',
    trc=InlineString15(""),
    trc_overflow=' ',
    record_serial=UInt32(0),
    row_number=UInt64(0),
    segment_hash=UInt64(0),
    distance=Distance(0),
    codeshare_airline=AirlineCode(""),
    codeshare_flt_no=FlightNumber(0),
    dei_10="",
    wet_lease=false,
    dei_127="",
    prbd=InlineString31(""),
)

const _ZERO_STATION_RECORD = StationRecord(
    code=StationCode(""),
    country=InlineString3(""),
    state=InlineString3(""),
    metro_area=InlineString3(""),
    region=InlineString3(""),
    lat=0.0,
    lng=0.0,
    utc_offset=Int16(0),
)

const _ZERO_SEGMENT_RECORD = SegmentRecord(
    segment_hash=UInt64(0),
    airline=AirlineCode(""),
    flt_no=FlightNumber(0),
    op_suffix=' ',
    itin_var=UInt8(0),
    itin_var_overflow=' ',
    svc_type=' ',
    operating_date=UInt32(0),
    num_legs=UInt8(0),
    first_leg_seq=UInt8(0),
    last_leg_seq=UInt8(0),
    segment_org=StationCode(""),
    segment_dst=StationCode(""),
    flown_distance=Distance(0),
    market_distance=Distance(0),
    segment_circuity=Float32(0),
    segment_pax_dep=Minutes(0),
    segment_pax_arr=Minutes(0),
    segment_ac_dep=Minutes(0),
    segment_ac_arr=Minutes(0),
)

const _ZERO_MCT_RESULT = MCTResult(
    time=NO_MINUTES,
    queried_status=MCT_DD,
    matched_status=MCT_DD,
    suppressed=false,
    source=SOURCE_GLOBAL_DEFAULT,
    specificity=UInt32(0),
    mct_id=Int32(0),
)

# ── GraphSegment ──────────────────────────────────────────────────────────────

"""
    mutable struct GraphSegment
---

# Description
- Segment node in the flight network graph; wraps a `SegmentRecord` with its
  constituent leg list and the operating-carrier identity
- `legs` is `Vector{Any}` to break the circular dependency with `GraphLeg`;
  elements are always `GraphLeg` instances at runtime
- `is_codeshare` is `true` when the marketing carrier differs from
  `operating_airline`

# Fields
- `record::SegmentRecord` — precomputed segment-level aggregates from DuckDB
- `legs::Vector{Any}` — ordered list of `GraphLeg` (leg_seq ascending)
- `operating_airline::AirlineCode` — IATA carrier code of the operating carrier
- `operating_flt_no::FlightNumber` — flight number of the operating carrier
- `is_codeshare::Bool` — `true` when marketing ≠ operating carrier
"""
@kwdef mutable struct GraphSegment
    record::SegmentRecord = _ZERO_SEGMENT_RECORD
    legs::Vector{Any} = Any[]
    operating_airline::AirlineCode = NO_AIRLINE
    operating_flt_no::FlightNumber = NO_FLIGHTNO
    is_codeshare::Bool = false
end

# Sentinel used as the default for GraphLeg.segment
const _ZERO_GRAPH_SEGMENT = GraphSegment()

# ── GraphStation ──────────────────────────────────────────────────────────────

"""
    mutable struct GraphStation
---

# Description
- Airport node in the flight network graph
- Holds departure/arrival leg lists, connection references, and always-on
  station-level metrics
- `region` and `country` are cached from `record` for cache-line efficiency
  in the O(n²) connection builder: accessing them directly avoids pointer
  indirection into `record`
- `departures`, `arrivals`, and `connections` are `Vector{Any}` to break the
  circular dependency with `GraphLeg` / `GraphConnection`; elements are always
  `GraphLeg` or `GraphConnection` at runtime

# Fields
- `code::StationCode` — 3-char IATA airport code
- `record::StationRecord` — full reference record (coordinates, timezone, etc.)
- `region::InlineString3` — cached IATA region from `record.region`
- `country::InlineString3` — cached ISO-2 country from `record.country`
- `departures::Vector{Any}` — `GraphLeg` elements: legs departing this station
- `arrivals::Vector{Any}` — `GraphLeg` elements: legs arriving at this station
- `connections::Vector{Any}` — `GraphConnection` elements built at this station
- `stats::StationStats` — always-on per-station instrumentation accumulator
"""
@kwdef mutable struct GraphStation
    code::StationCode = NO_STATION
    record::StationRecord = _ZERO_STATION_RECORD
    region::InlineString3 = InlineString3("")
    country::InlineString3 = InlineString3("")
    departures::Vector{Any} = Any[]
    arrivals::Vector{Any} = Any[]
    connections::Vector{Any} = Any[]
    stats::StationStats = StationStats()
end

# Sentinel used as the default for GraphLeg.org / GraphLeg.dst and
# GraphConnection.station
const _ZERO_GRAPH_STATION = GraphStation()

# ── GraphLeg ──────────────────────────────────────────────────────────────────

"""
    mutable struct GraphLeg
---

# Description
- Flight leg node in the flight network graph
- References its origin/destination `GraphStation` nodes and parent `GraphSegment`
- `connect_to` holds outbound `GraphConnection` elements where this leg is the
  arriving flight; `connect_from` holds inbound connections where this leg departs
- Both connection vectors are `Vector{Any}` to break the circular dependency with
  `GraphConnection`; elements are always `GraphConnection` at runtime

# Fields
- `record::LegRecord` — full leg identity and schedule data from DuckDB
- `org::GraphStation` — origin station node
- `dst::GraphStation` — destination station node
- `segment::GraphSegment` — parent segment (all legs sharing the same flight identity)
- `connect_to::Vector{Any}` — `GraphConnection` elements where this leg is `from_leg`
- `connect_from::Vector{Any}` — `GraphConnection` elements where this leg is `to_leg`
- `distance::Distance` — flown distance in miles; copied from `record.distance` and
  gap-filled from geodesic when zero during `build_graph!`
"""
@kwdef mutable struct GraphLeg
    record::LegRecord = _ZERO_LEG_RECORD
    org::GraphStation = _ZERO_GRAPH_STATION
    dst::GraphStation = _ZERO_GRAPH_STATION
    segment::GraphSegment = _ZERO_GRAPH_SEGMENT
    connect_to::Vector{Any} = Any[]
    connect_from::Vector{Any} = Any[]
    distance::Distance = Distance(0)
end

# Sentinel used as the default for GraphConnection.from_leg / GraphConnection.to_leg
const _ZERO_GRAPH_LEG = GraphLeg()

# ── GraphConnection ───────────────────────────────────────────────────────────

"""
    mutable struct GraphConnection
---

# Description
- Connection edge between two `GraphLeg` nodes at a connect-point `GraphStation`
- A *nonstop self-connection* has `from_leg === to_leg`; this represents a
  passthrough or self-connection used in the DFS search as a nonstop leg
- `mct` is the minimum connecting time (minutes) from the MCT cascade;
  `mxct` is the maximum connecting time (minutes) enforced by `SearchConfig`
- `cnx_time` is the actual available connection time (`to_leg.record.ac_dep -
  from_leg.record.ac_arr`) in minutes
- `valid_from` / `valid_to` / `valid_days` are the intersection of the two legs'
  validity windows and frequency bitmasks; `num_valid_dates` is the count of
  operating dates in the intersection (0 = not yet computed)
- `status` carries `DOW_*` bits (from `valid_days`) plus `STATUS_*` classification
  bits set during `build_connections!`
- `is_through` mirrors `STATUS_THROUGH` in `status` as a fast boolean cache

# Fields
- `from_leg::GraphLeg` — the arriving leg of this connection
- `to_leg::GraphLeg` — the departing leg of this connection
- `station::GraphStation` — the connect-point airport
- `mct::Minutes` — minimum connecting time (from MCT cascade)
- `mxct::Minutes` — maximum connecting time (from `SearchConfig`)
- `cnx_time::Minutes` — actual available connection time
- `status::StatusBits` — DOW + classification bitmask
- `mct_result::MCTResult` — full result of the MCT lookup for this connection
- `is_through::Bool` — true when same flight number, different leg (through-service)
- `valid_from::UInt32` — packed YYYYMMDD start of combined validity window
- `valid_to::UInt32` — packed YYYYMMDD end of combined validity window
- `valid_days::UInt8` — 7-bit DOW bitmask of operating days
- `num_valid_dates::Int16` — count of dates in the validity intersection (0 = uncached)
"""
@kwdef mutable struct GraphConnection
    from_leg::GraphLeg = _ZERO_GRAPH_LEG
    to_leg::GraphLeg = _ZERO_GRAPH_LEG
    station::GraphStation = _ZERO_GRAPH_STATION
    mct::Minutes = NO_MINUTES
    mxct::Minutes = NO_MINUTES
    cnx_time::Minutes = Minutes(0)
    status::StatusBits = StatusBits(0)
    mct_result::MCTResult = _ZERO_MCT_RESULT
    is_through::Bool = false
    valid_from::UInt32 = UInt32(0)
    valid_to::UInt32 = UInt32(0)
    valid_days::UInt8 = UInt8(0)
    num_valid_dates::Int16 = Int16(0)
end

# ── OneStopConnection ─────────────────────────────────────────────────────────

"""
    mutable struct OneStopConnection
---

# Description
- Pre-computed two-connection path sharing a transit leg
- `first` is a connection at the transit leg's origin station;
  `second` is a connection at the transit leg's destination station
- Full path: `first.from_leg.org → via_org → transit_leg → via_dst → second.to_leg.dst`
  (3 legs, 2 stops)
- `valid_from` / `valid_to` / `valid_days` are the intersection of all three
  legs' validity windows and frequency bitmasks

# Fields
- `first::GraphConnection` — first connection (inbound to the transit leg's origin)
- `second::GraphConnection` — second connection (outbound from the transit leg's destination)
- `via_leg::GraphLeg` — the shared transit leg connecting `first` and `second`
- `total_distance::Distance` — sum of all three flown leg distances (miles)
- `valid_from::UInt32` — packed YYYYMMDD start of combined validity window
- `valid_to::UInt32` — packed YYYYMMDD end of combined validity window
- `valid_days::UInt8` — 7-bit DOW bitmask of operating days
"""
@kwdef mutable struct OneStopConnection
    first::GraphConnection = GraphConnection()
    second::GraphConnection = GraphConnection()
    via_leg::GraphLeg = _ZERO_GRAPH_LEG
    total_distance::Distance = Distance(0)
    valid_from::UInt32 = UInt32(0)
    valid_to::UInt32 = UInt32(0)
    valid_days::UInt8 = UInt8(0)
end

"""
    const OneStopIndex

`Dict{Tuple{StationCode, StationCode}, Vector{OneStopConnection}}`

Maps `(origin_station, destination_station)` pairs to all pre-computed
`OneStopConnection` paths between them (Layer 1 index).
"""
const OneStopIndex = Dict{Tuple{StationCode,StationCode},Vector{OneStopConnection}}

# ── Itinerary ─────────────────────────────────────────────────────────────────

"""
    mutable struct Itinerary
---

# Description
- An ordered sequence of `GraphConnection` edges representing a complete travel
  path from origin to destination
- `connections[1].from_leg` is the first departing leg; `connections[end].to_leg`
  is the final arriving leg
- Computed aggregate fields (`elapsed_time`, `num_stops`, etc.) are filled by
  the post-DFS scoring pass in `search_itineraries`
- `circuity` is `total_distance / market_distance`; values > 1 indicate detour

# Fields
- `connections::Vector{GraphConnection}` — ordered path edges (nonstop = 1 element)
- `status::StatusBits` — classification bitmask for the full itinerary
- `elapsed_time::Int32` — total elapsed time from first departure to last arrival (minutes)
- `num_stops::Int16` — number of intermediate stops (0 = nonstop)
- `num_eqp_changes::Int16` — number of equipment/aircraft-type changes
- `total_distance::Distance` — sum of flown leg distances (miles)
- `market_distance::Distance` — great-circle distance from org to dst (miles)
- `circuity::Float32` — `total_distance / market_distance` (1.0 = perfectly direct)
- `num_metros::Int16` — distinct metro areas traversed (incl. org and dst)
- `num_states::Int16` — distinct US states traversed
- `num_countries::Int16` — distinct countries traversed
- `num_regions::Int16` — distinct IATA regions traversed
"""
@kwdef mutable struct Itinerary
    connections::Vector{GraphConnection} = GraphConnection[]
    status::StatusBits = StatusBits(0)
    elapsed_time::Int32 = Int32(0)
    num_stops::Int16 = Int16(0)
    num_eqp_changes::Int16 = Int16(0)
    total_distance::Distance = Distance(0)
    market_distance::Distance = Distance(0)
    circuity::Float32 = Float32(0)
    num_metros::Int16 = Int16(0)
    num_states::Int16 = Int16(0)
    num_countries::Int16 = Int16(0)
    num_regions::Int16 = Int16(0)
end

# ── Convenience constructors ──────────────────────────────────────────────────

"""
    `function GraphStation(record::StationRecord)::GraphStation`
---

# Description
- Constructs a `GraphStation` from a `StationRecord`, caching `region` and
  `country` at the top level for cache-line efficiency during connection building

# Arguments
1. `record::StationRecord`: reference data for the airport

# Returns
- `::GraphStation`: fully initialised station node with empty leg/connection lists

# Examples
```julia
julia> rec = StationRecord(code=StationCode("ORD"), country=InlineString3("US"),
               state=InlineString3("IL"), metro_area=InlineString3("CHI"),
               region=InlineString3("NAM"), lat=41.97, lng=-87.91,
               utc_offset=Int16(-360));
julia> stn = GraphStation(rec);
julia> stn.code == StationCode("ORD")
true
```
"""
function GraphStation(record::StationRecord)
    GraphStation(
        code=record.code,
        record=record,
        region=record.region,
        country=record.country,
    )
end

"""
    `function GraphLeg(record::LegRecord, org::GraphStation, dst::GraphStation)::GraphLeg`
---

# Description
- Constructs a `GraphLeg` from a `LegRecord` and its pre-looked-up origin/destination
  station nodes
- `segment` is left as the zero-sentinel default; callers that build full segment
  linkage should assign `leg.segment` after construction

# Arguments
1. `record::LegRecord`: full leg identity and schedule data
2. `org::GraphStation`: origin station node (must already exist in the graph)
3. `dst::GraphStation`: destination station node (must already exist in the graph)

# Returns
- `::GraphLeg`: leg node with empty `connect_to` / `connect_from` lists

# Examples
```julia
julia> leg = GraphLeg(leg_record, org_station, dst_station);
julia> leg.record.airline == AirlineCode("UA")
true
```
"""
function GraphLeg(record::LegRecord, org::GraphStation, dst::GraphStation)
    GraphLeg(record=record, org=org, dst=dst, distance=record.distance)
end

"""
    `function nonstop_connection(leg::GraphLeg, station::GraphStation)::GraphConnection`
---

# Description
- Constructs a self-connection where `from_leg === to_leg`, representing a nonstop
  flight leg as an edge in the DFS search path
- `cnx_time`, `mct`, and `mxct` are all set to `Minutes(0)` (no connection overhead)
- `valid_from`, `valid_to`, and `valid_days` are copied directly from the leg record
  so the connection's validity window exactly matches the leg's schedule window

# Arguments
1. `leg::GraphLeg`: the nonstop leg (used for both `from_leg` and `to_leg`)
2. `station::GraphStation`: the departing station (the leg's `org`)

# Returns
- `::GraphConnection`: a self-connection edge for use in `Itinerary.connections`

# Examples
```julia
julia> cp = nonstop_connection(leg, org_station);
julia> cp.from_leg === cp.to_leg
true
julia> cp.cnx_time == Minutes(0)
true
```
"""
function nonstop_connection(leg::GraphLeg, station::GraphStation)
    GraphConnection(
        from_leg=leg,
        to_leg=leg,
        station=station,
        cnx_time=Minutes(0),
        mct=Minutes(0),
        mxct=Minutes(0),
        valid_from=leg.record.eff_date,
        valid_to=leg.record.disc_date,
        valid_days=leg.record.frequency,
        num_valid_dates=Int16(0),
    )
end

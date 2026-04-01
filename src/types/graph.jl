# src/types/graph.jl — Subsystem 2 graph types with abstract type hierarchy
#
# Abstract type hierarchy breaks the mutual reference between GraphStation,
# GraphLeg, and GraphConnection without resorting to Vector{Any}.
#
# AbstractGraphNode — supertype for GraphLeg, GraphStation, _UninitNode
# AbstractGraphEdge — supertype for GraphConnection
#
# Fields that hold a single cross-reference to a not-yet-defined concrete type
# are typed AbstractGraphNode (e.g. GraphLeg.org, GraphConnection.from_leg).
# These are single pointer dereferences, acceptable in non-iterated paths.
#
# Vectors iterated in hot loops use CONCRETE element types:
#   GraphStation.departures  :: Vector{GraphLeg}
#   GraphStation.arrivals    :: Vector{GraphLeg}
#   GraphStation.connections :: Vector{GraphConnection}
#   GraphLeg.connect_to      :: Vector{GraphConnection}
#   GraphLeg.connect_from    :: Vector{GraphConnection}
#
# Dependency order (definition order in this file):
#   abstract types + _UninitNode sentinel
#   SegmentRecord sentinel (_ZERO_SEGMENT_RECORD)
#   GraphSegment (legs::Vector{AbstractGraphNode})
#   _ZERO_GRAPH_SEGMENT sentinel
#   MCTResult sentinel (_ZERO_MCT_RESULT)
#   GraphConnection <: AbstractGraphEdge (from_leg/to_leg/station :: AbstractGraphNode)
#   LegRecord sentinel (_ZERO_LEG_RECORD)
#   GraphLeg <: AbstractGraphNode (org/dst :: AbstractGraphNode, connect_to/from :: Vector{GraphConnection})
#   _ZERO_GRAPH_LEG sentinel
#   StationRecord sentinel (_ZERO_STATION_RECORD)
#   GraphStation <: AbstractGraphNode (departures/arrivals :: Vector{GraphLeg}, connections :: Vector{GraphConnection})
#   _ZERO_GRAPH_STATION sentinel
#   Itinerary, TripLeg, TripScoringWeights, Trip
#
# Zero-arg defaults: @kwdef generates a zero-arg keyword-dispatch stub for every
# struct it processes. Defining a separate TypeName() outer constructor would
# overwrite that stub and cause a precompilation error. Instead, each struct
# with non-optional record fields uses a module-level const sentinel as the
# @kwdef default expression. Sentinels are declared in the dependency order
# shown above, interleaved with the struct definitions that need them.

# ── Abstract type hierarchy ──────────────────────────────────────────────────

"""
    abstract type AbstractGraphNode end

Supertype for flight network graph nodes (`GraphLeg`, `GraphStation`).
Used for cross-reference fields where the concrete type is not yet defined
at the point of struct declaration.
"""
abstract type AbstractGraphNode end

"""
    abstract type AbstractGraphEdge end

Supertype for flight network graph edges (`GraphConnection`).
"""
abstract type AbstractGraphEdge end

"""
    mutable struct _UninitNode <: AbstractGraphNode

Minimal sentinel type for `AbstractGraphNode` fields on structs defined before
`GraphLeg` and `GraphStation`. Used only as a default value in `GraphConnection`
keyword constructors.
"""
mutable struct _UninitNode <: AbstractGraphNode end

"""Sentinel value for uninitialised `AbstractGraphNode` fields."""
const _UNINIT_NODE = _UninitNode()

# ── Record sentinels (declared before graph structs that embed them) ──────────

const _ZERO_SEGMENT_RECORD = SegmentRecord(
    segment_hash=UInt64(0),
    carrier=AirlineCode(""),
    flight_number=FlightNumber(0),
    operational_suffix=' ',
    itinerary_var_id=UInt8(0),
    itinerary_var_overflow=' ',
    service_type=' ',
    operating_date=UInt32(0),
    num_legs=UInt8(0),
    first_leg_seq=UInt8(0),
    last_leg_seq=UInt8(0),
    segment_departure_station=StationCode(""),
    segment_arrival_station=StationCode(""),
    flown_distance=Distance(0),
    market_distance=Distance(0),
    segment_circuity=Float32(0),
    segment_passenger_departure_time=Minutes(0),
    segment_passenger_arrival_time=Minutes(0),
    segment_aircraft_departure_time=Minutes(0),
    segment_aircraft_arrival_time=Minutes(0),
)

# ── GraphSegment ──────────────────────────────────────────────────────────────

"""
    mutable struct GraphSegment
---

# Description
- Segment node in the flight network graph; wraps a `SegmentRecord` with its
  constituent leg list and the operating-carrier identity
- `legs` is `Vector{AbstractGraphNode}` because `GraphLeg` is not yet defined;
  elements are always `GraphLeg` instances at runtime
- `is_codeshare` is `true` when the marketing carrier differs from
  `operating_airline`

# Fields
- `record::SegmentRecord` — precomputed segment-level aggregates from DuckDB
- `legs::Vector{AbstractGraphNode}` — ordered list of `GraphLeg` (leg_seq ascending)
- `operating_airline::AirlineCode` — IATA carrier code of the operating carrier
- `operating_flt_no::FlightNumber` — flight number of the operating carrier
- `is_codeshare::Bool` — `true` when marketing ≠ operating carrier
"""
@kwdef mutable struct GraphSegment
    record::SegmentRecord = _ZERO_SEGMENT_RECORD
    legs::Vector{AbstractGraphNode} = AbstractGraphNode[]
    operating_airline::AirlineCode = NO_AIRLINE
    operating_flt_no::FlightNumber = NO_FLIGHTNO
    is_codeshare::Bool = false
end

# Sentinel used as the default for GraphLeg.segment
const _ZERO_GRAPH_SEGMENT = GraphSegment()

const _ZERO_MCT_RESULT = MCTResult(
    time=NO_MINUTES,
    queried_status=MCT_DD,
    matched_status=MCT_DD,
    suppressed=false,
    source=SOURCE_GLOBAL_DEFAULT,
    specificity=UInt32(0),
    mct_id=Int32(0),
)

# ── GraphConnection ───────────────────────────────────────────────────────────

"""
    mutable struct GraphConnection <: AbstractGraphEdge
---

# Description
- Connection edge between two `GraphLeg` nodes at a connect-point `GraphStation`
- A *nonstop self-connection* has `from_leg === to_leg`; this represents a
  passthrough or self-connection used in the DFS search as a nonstop leg
- `from_leg`, `to_leg`, and `station` are typed `AbstractGraphNode` because
  `GraphLeg` and `GraphStation` are defined after `GraphConnection`; at runtime
  they are always `GraphLeg` or `GraphStation` instances respectively
- `mct` is the minimum connecting time (minutes) from the MCT cascade;
  `mxct` is the maximum connecting time (minutes) enforced by `SearchConfig`
- `cnx_time` is the actual available connection time (`to_leg.record.aircraft_departure_time -
  from_leg.record.aircraft_arrival_time`) in minutes
- `valid_from` / `valid_to` / `valid_days` are the intersection of the two legs'
  validity windows and frequency bitmasks; `num_valid_dates` is the count of
  operating dates in the intersection (0 = not yet computed)
- `status` carries `DOW_*` bits (from `valid_days`) plus `STATUS_*` classification
  bits set during `build_connections!`
- `is_through` mirrors `STATUS_THROUGH` in `status` as a fast boolean cache

# Fields
- `from_leg::AbstractGraphNode` — the arriving leg of this connection (always `GraphLeg`)
- `to_leg::AbstractGraphNode` — the departing leg of this connection (always `GraphLeg`)
- `station::AbstractGraphNode` — the connect-point airport (always `GraphStation`)
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
mutable struct GraphConnection <: AbstractGraphEdge
    from_leg::AbstractGraphNode
    to_leg::AbstractGraphNode
    station::AbstractGraphNode
    mct::Minutes
    mxct::Minutes
    cnx_time::Minutes
    status::StatusBits
    mct_result::MCTResult
    is_through::Bool
    valid_from::UInt32
    valid_to::UInt32
    valid_days::UInt8
    num_valid_dates::Int16

    # Keyword constructor (also serves as zero-arg constructor via defaults)
    function GraphConnection(;
        from_leg::AbstractGraphNode=_UNINIT_NODE,
        to_leg::AbstractGraphNode=_UNINIT_NODE,
        station::AbstractGraphNode=_UNINIT_NODE,
        mct::Minutes=NO_MINUTES,
        mxct::Minutes=NO_MINUTES,
        cnx_time::Minutes=Minutes(0),
        status::StatusBits=StatusBits(0),
        mct_result::MCTResult=_ZERO_MCT_RESULT,
        is_through::Bool=false,
        valid_from::UInt32=UInt32(0),
        valid_to::UInt32=UInt32(0),
        valid_days::UInt8=UInt8(0),
        num_valid_dates::Int16=Int16(0),
    )
        new(from_leg, to_leg, station, mct, mxct, cnx_time, status, mct_result,
            is_through, valid_from, valid_to, valid_days, num_valid_dates)
    end
end

# Sentinel: empty nonstop connection (used as default for GraphLeg.nonstop_cp)
const _NO_NONSTOP_CP = GraphConnection()

# ── GraphLeg ──────────────────────────────────────────────────────────────────

const _ZERO_LEG_RECORD = LegRecord(
    carrier=AirlineCode(""),
    flight_number=FlightNumber(0),
    operational_suffix=' ',
    itinerary_var_id=UInt8(0),
    itinerary_var_overflow=' ',
    leg_sequence_number=UInt8(0),
    service_type=' ',
    departure_station=StationCode(""),
    arrival_station=StationCode(""),
    passenger_departure_time=Minutes(0),
    passenger_arrival_time=Minutes(0),
    aircraft_departure_time=Minutes(0),
    aircraft_arrival_time=Minutes(0),
    departure_utc_offset=Int16(0),
    arrival_utc_offset=Int16(0),
    departure_date_variation=Int8(0),
    arrival_date_variation=Int8(0),
    aircraft_type=InlineString7(""),
    body_type=' ',
    departure_terminal=InlineString3(""),
    arrival_terminal=InlineString3(""),
    aircraft_owner=AirlineCode(""),
    operating_date=UInt32(0),
    day_of_week=UInt8(0),
    effective_date=UInt32(0),
    discontinue_date=UInt32(0),
    frequency=UInt8(0),
    dep_intl_dom=' ',
    arr_intl_dom=' ',
    traffic_restriction_for_leg=InlineString15(""),
    traffic_restriction_overflow=' ',
    record_serial=UInt32(0),
    row_number=UInt64(0),
    segment_hash=UInt64(0),
    distance=Distance(0),
    administrating_carrier=AirlineCode(""),
    administrating_carrier_flight_number=FlightNumber(0),
    dei_10="",
    wet_lease=false,
    dei_127="",
    prbd=InlineString31(""),
)

"""
    mutable struct GraphLeg <: AbstractGraphNode
---

# Description
- Flight leg node in the flight network graph
- References its origin/destination stations (typed `AbstractGraphNode` because
  `GraphStation` is not yet defined) and parent `GraphSegment`
- `connect_to` holds outbound `GraphConnection` elements where this leg is the
  arriving flight; `connect_from` holds inbound connections where this leg departs
- Connection vectors use concrete `Vector{GraphConnection}` for hot-path iteration

# Fields
- `record::LegRecord` — full leg identity and schedule data from DuckDB
- `org::AbstractGraphNode` — origin station node (always `GraphStation` at runtime)
- `dst::AbstractGraphNode` — destination station node (always `GraphStation` at runtime)
- `segment::GraphSegment` — parent segment (all legs sharing the same flight identity)
- `connect_to::Vector{GraphConnection}` — connections where this leg is `from_leg`
- `connect_from::Vector{GraphConnection}` — connections where this leg is `to_leg`
- `distance::Distance` — flown distance in miles; copied from `record.distance` and
  gap-filled from geodesic when zero during `build_graph!`
"""
@kwdef mutable struct GraphLeg <: AbstractGraphNode
    record::LegRecord = _ZERO_LEG_RECORD
    org::AbstractGraphNode = _UNINIT_NODE
    dst::AbstractGraphNode = _UNINIT_NODE
    segment::GraphSegment = _ZERO_GRAPH_SEGMENT
    connect_to::Vector{GraphConnection} = GraphConnection[]
    connect_from::Vector{GraphConnection} = GraphConnection[]
    distance::Distance = Distance(0)
    nonstop_cp::GraphConnection = _NO_NONSTOP_CP  # set during build_connections_at_station!
end

# ── GraphStation ──────────────────────────────────────────────────────────────

const _ZERO_STATION_RECORD = StationRecord(
    code=StationCode(""),
    country=InlineString3(""),
    state=InlineString3(""),
    city=InlineString3(""),
    region=InlineString3(""),
    latitude=0.0,
    longitude=0.0,
    utc_offset=Int16(0),
)

"""
    mutable struct GraphStation <: AbstractGraphNode
---

# Description
- Airport node in the flight network graph
- Holds departure/arrival leg lists, connection references, and always-on
  station-level metrics
- `region` and `country` are cached from `record` for cache-line efficiency
  in the O(n^2) connection builder: accessing them directly avoids pointer
  indirection into `record`
- `departures` and `arrivals` use concrete `Vector{GraphLeg}` for hot-path
  iteration; `connections` uses concrete `Vector{GraphConnection}`

# Fields
- `code::StationCode` — 3-char IATA airport code
- `record::StationRecord` — full reference record (coordinates, timezone, etc.)
- `region::InlineString3` — cached IATA region from `record.region`
- `country::InlineString3` — cached ISO-2 country from `record.country`
- `departures::Vector{GraphLeg}` — legs departing this station
- `arrivals::Vector{GraphLeg}` — legs arriving at this station
- `connections::Vector{GraphConnection}` — connections built at this station
- `stats::StationStats` — always-on per-station instrumentation accumulator
"""
@kwdef mutable struct GraphStation <: AbstractGraphNode
    code::StationCode = NO_STATION
    record::StationRecord = _ZERO_STATION_RECORD
    region::InlineString3 = InlineString3("")
    country::InlineString3 = InlineString3("")
    departures::Vector{GraphLeg} = GraphLeg[]
    arrivals::Vector{GraphLeg} = GraphLeg[]
    connections::Vector{GraphConnection} = GraphConnection[]
    stats::StationStats = StationStats()
end

# Sentinel used for testing and default construction
const _ZERO_GRAPH_STATION = GraphStation()

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

# ── Trip Search Input Types ──────────────────────────────────────────────────

"""
    struct TripLeg

One segment of a multi-leg trip search request.

# Fields
- `origin::StationCode` — departure station
- `destination::StationCode` — arrival station
- `date::Date` — travel date for this leg
- `min_stay::Int` — minimum minutes after previous leg arrives (0 = no constraint)
- `max_stay::Int` — maximum minutes after previous leg arrives (0 = no constraint)
"""
@kwdef struct TripLeg
    origin::StationCode = NO_STATION
    destination::StationCode = NO_STATION
    date::Date = Date(2000, 1, 1)
    min_stay::Int = 0
    max_stay::Int = 0
end

"""
    struct TripScoringWeights

Configurable weights for trip scoring. All criteria are minimized — lower score is better.
"""
@kwdef struct TripScoringWeights
    stops::Float64 = 10.0
    eqp_changes::Float64 = 5.0
    carrier_changes::Float64 = 5.0
    flt_no_changes::Float64 = 2.0
    elapsed::Float64 = 1.0
    block_time::Float64 = 0.5
    layover::Float64 = 0.5
    distance::Float64 = 0.1
    circuity::Float64 = 3.0
end

# ── Trip ─────────────────────────────────────────────────────────────────────

"""
    mutable struct Trip

Booking-level journey container grouping one or more `Itinerary` objects.
A one-way trip has 1 itinerary, a round-trip has 2, multi-city has N.

# Fields
- `trip_id::Int32` — unique identifier
- `itineraries::Vector{Itinerary}` — ordered sequence (outbound first, return second, etc.)
- `origin::StationCode` — ultimate origin (first itinerary's departure)
- `destination::StationCode` — ultimate destination (last itinerary's arrival; same as origin for round-trip)
- `trip_type::Symbol` — `:oneway`, `:roundtrip`, or `:multicity`
- `total_elapsed::Int32` — sum of itinerary elapsed times (minutes)
- `total_distance::Distance` — sum of itinerary total distances (miles)
"""
@kwdef mutable struct Trip
    trip_id::Int32 = Int32(0)
    itineraries::Vector{Itinerary} = Itinerary[]
    origin::StationCode = NO_STATION
    destination::StationCode = NO_STATION
    trip_type::Symbol = :oneway
    total_elapsed::Int32 = Int32(0)
    total_distance::Distance = Distance(0)
    score::Float64 = 0.0
end

"""
    `Trip(itineraries::Vector{Itinerary}; trip_id::Int32=Int32(0))::Trip`

Construct a Trip from a sequence of itineraries, inferring origin, destination,
trip type, and aggregate metrics.
"""
function Trip(itineraries::Vector{Itinerary}; trip_id::Int32=Int32(0))
    isempty(itineraries) && return Trip(trip_id=trip_id)

    first_itn = itineraries[1]
    origin = (first_itn.connections[1].from_leg::GraphLeg).org.code

    last_itn = itineraries[end]
    last_cp = last_itn.connections[end]
    destination = (last_cp.to_leg === last_cp.from_leg) ?
        (last_cp.from_leg::GraphLeg).dst.code : (last_cp.to_leg::GraphLeg).dst.code

    trip_type = if length(itineraries) == 1
        :oneway
    elseif origin == destination
        :roundtrip
    else
        :multicity
    end

    total_elapsed = Int32(sum(itn.elapsed_time for itn in itineraries))
    total_distance = sum(itn.total_distance for itn in itineraries; init=Distance(0))

    Trip(
        trip_id=trip_id, itineraries=itineraries,
        origin=origin, destination=destination, trip_type=trip_type,
        total_elapsed=total_elapsed, total_distance=total_distance,
    )
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
               state=InlineString3("IL"), city=InlineString3("CHI"),
               region=InlineString3("NAM"), latitude=41.97, longitude=-87.91,
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
julia> leg.record.carrier == AirlineCode("UA")
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
        valid_from=leg.record.effective_date,
        valid_to=leg.record.discontinue_date,
        valid_days=leg.record.frequency,
        num_valid_dates=Int16(0),
    )
end

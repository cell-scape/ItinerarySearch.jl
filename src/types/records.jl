# src/types/records.jl — Immutable, isbits record types for DuckDB↔Julia bridge

"""
    `pack_date(d::Date)::UInt32`

Pack a Date into YYYYMMDD integer format for isbits storage.
"""
pack_date(d::Date)::UInt32 = UInt32(year(d) * 10000 + month(d) * 100 + day(d))

"""
    `unpack_date(packed::UInt32)::Date`

Unpack a YYYYMMDD integer back to a Date.
"""
function unpack_date(packed::UInt32)::Date
    y = Int(packed ÷ 10000)
    m = Int((packed ÷ 100) % 100)
    d = Int(packed % 100)
    Date(y, m, d)
end

"""
    struct LegRecord

Immutable flight leg record. Captures everything needed to identify,
display, and cross-reference a flight leg from SSIM data.

The **Flight Identifier** `(carrier, flight_number, operational_suffix,
itinerary_var_id, itinerary_var_overflow, leg_sequence_number, service_type)`
uniquely identifies a record in the SSIM schedule. Dropping
`leg_sequence_number` gives the **Segment Identifier**.
"""
@kwdef struct LegRecord
    # ── Flight Identifier (SSIM Type 3 bytes 2-14) ──
    carrier::AirlineCode
    flight_number::FlightNumber
    operational_suffix::Char
    itinerary_var_id::UInt8
    itinerary_var_overflow::Char
    leg_sequence_number::UInt8
    service_type::Char

    # ── Stations & Times ──
    departure_station::StationCode
    arrival_station::StationCode
    passenger_departure_time::Minutes   # Passenger STD: HHMM → minutes since midnight
    passenger_arrival_time::Minutes     # Passenger STA
    aircraft_departure_time::Minutes    # Aircraft STD (connection building uses this)
    aircraft_arrival_time::Minutes      # Aircraft STA
    departure_utc_offset::Int16         # minutes from UTC
    arrival_utc_offset::Int16
    departure_date_variation::Int8      # 0, 1, 2, or -1 for 'A'
    arrival_date_variation::Int8

    # ── Equipment & Terminals ──
    aircraft_type::InlineString7
    body_type::Char                 # 'W'ide or 'N'arrow
    departure_terminal::InlineString3
    arrival_terminal::InlineString3
    aircraft_owner::AirlineCode

    # ── Operating Date (from EDF expansion) ──
    operating_date::UInt32          # packed YYYYMMDD
    day_of_week::UInt8              # 1=Mon .. 7=Sun (ISO)

    # ── Schedule Dates & Frequency ──
    effective_date::UInt32          # packed YYYYMMDD
    discontinue_date::UInt32        # packed YYYYMMDD
    frequency::UInt8                # 7-bit DOW bitmask (Mon=bit0 .. Sun=bit6)

    # ── MCT & Restrictions ──
    dep_intl_dom::Char              # 'D' or 'I'
    arr_intl_dom::Char
    traffic_restriction_for_leg::InlineString15  # bytes 150-160, indexed by leg_sequence_number
    traffic_restriction_overflow::Char

    # ── Identity & Cross-reference ──
    record_serial::UInt32           # SSIM bytes 195-200
    row_number::UInt64              # monotonic counter assigned during ingest
    segment_hash::UInt64            # hash of segment identity fields
    distance::Distance

    # ── Administrating Carrier (from DEI supplements) ──
    administrating_carrier::AirlineCode     # DEI 50
    administrating_carrier_flight_number::FlightNumber  # DEI 50
    dei_10::String                  # DEI 10: commercial duplicate list (variable length)
    wet_lease::Bool                 # byte 149: 'Z' or 'S'
    dei_127::String                 # DEI 127: operating airline disclosure (variable length)

    # ── Booking ──
    prbd::InlineString31            # bytes 76-95
end

"""
    `flight_id(r::LegRecord)::String`

Human-readable flight identifier (e.g., "UA 354").
"""
flight_id(r::LegRecord) = "$(r.carrier)$(lpad(r.flight_number, 4))"

# ── LegKey ───────────────────────────────────────────────────────────────────

"""
    struct LegKey

Compact reference to a leg in the schedule. Contains the SSIM Type 3 flight
identifier fields plus the database row ID and record serial for cross-referencing.
Administrating carrier fields default to self when the leg is operating.
"""
@kwdef struct LegKey
    # ── Cross-reference IDs ──
    row_number::UInt64 = UInt64(0)
    record_serial::UInt32 = UInt32(0)

    # ── Flight Identifier (SSIM Type 3 bytes 2-14) ──
    carrier::AirlineCode = AirlineCode("")
    flight_number::FlightNumber = FlightNumber(0)
    operational_suffix::Char = ' '
    itinerary_var_id::UInt8 = UInt8(1)
    itinerary_var_overflow::Char = ' '
    leg_sequence_number::UInt8 = UInt8(1)
    service_type::Char = 'J'

    # ── Administrating Carrier (from DEI 50) ──
    administrating_carrier::AirlineCode = AirlineCode("")
    administrating_carrier_flight_number::FlightNumber = FlightNumber(0)

    # ── Station Pair ──
    departure_station::StationCode = StationCode("")
    arrival_station::StationCode = StationCode("")

    # ── Schedule Context ──
    operating_date::UInt32 = UInt32(0)  # packed YYYYMMDD — which day this leg operates
    departure_time::Minutes = Minutes(0)      # scheduled departure (minutes since midnight, local)
end

"""
    `LegKey(r::LegRecord)::LegKey`

Construct a `LegKey` from a full `LegRecord`, copying identity fields.
Administrating carrier fields default to self when the leg is operating.
"""
function LegKey(r::LegRecord)
    cs_al = strip(String(r.administrating_carrier))
    carrier_s = strip(String(r.carrier))
    LegKey(
        row_number                          = r.row_number,
        record_serial                       = r.record_serial,
        carrier                             = r.carrier,
        flight_number                       = r.flight_number,
        operational_suffix                  = r.operational_suffix,
        itinerary_var_id                    = r.itinerary_var_id,
        itinerary_var_overflow              = r.itinerary_var_overflow,
        leg_sequence_number                 = r.leg_sequence_number,
        service_type                        = r.service_type,
        administrating_carrier              = (cs_al == "" || cs_al == carrier_s) ? r.carrier : r.administrating_carrier,
        administrating_carrier_flight_number = (cs_al == "" || cs_al == carrier_s) ? r.flight_number : r.administrating_carrier_flight_number,
        departure_station                   = r.departure_station,
        arrival_station                     = r.arrival_station,
        operating_date                      = r.operating_date,
        departure_time                      = r.passenger_departure_time,
    )
end

flight_id(k::LegKey) = "$(k.carrier)$(lpad(k.flight_number, 4))"

# ── ItineraryRef ─────────────────────────────────────────────────────────────

"""
    struct ItineraryRef

Lightweight itinerary reference containing a sequence of `LegKey` references
and numeric summary fields. Decoupled from the graph — suitable for serialization,
cross-system handoff, and reaccommodation candidate lists.

Display strings (flights, route) are computed on demand via `Base.show` and
helper accessors, not stored — keeps allocations minimal.

# Fields
- `legs::Vector{LegKey}` — ordered leg references
- `num_stops::Int` — number of intermediate stops (0 = nonstop)
- `elapsed_minutes::Int32` — total elapsed time (minutes, UTC)
- `flight_minutes::Int32` — total in-flight block time (minutes, UTC)
- `layover_minutes::Int32` — total ground/connection time (minutes)
- `distance_miles::Float32` — total flown distance (statute miles)
- `circuity::Float32` — ratio of flown distance to great-circle distance
"""
@kwdef struct ItineraryRef
    legs::Vector{LegKey} = LegKey[]
    num_stops::Int = 0
    elapsed_minutes::Int32 = Int32(0)
    flight_minutes::Int32 = Int32(0)
    layover_minutes::Int32 = Int32(0)
    distance_miles::Float32 = Float32(0)
    circuity::Float32 = Float32(0)
end

# ── Derived accessors (computed on demand, not stored) ───────────────────────

"""Origin station code of the itinerary (first leg's departure_station)."""
origin(ref::ItineraryRef) = isempty(ref.legs) ? StationCode("") : ref.legs[1].departure_station

"""Destination station code (last leg's arrival_station)."""
destination(ref::ItineraryRef) = isempty(ref.legs) ? StationCode("") : ref.legs[end].arrival_station

"""Station codes visited in order (origin + intermediates + destination)."""
function stops(ref::ItineraryRef)::Vector{StationCode}
    isempty(ref.legs) && return StationCode[]
    result = StationCode[]
    for k in ref.legs
        (isempty(result) || result[end] != k.departure_station) && push!(result, k.departure_station)
    end
    push!(result, ref.legs[end].arrival_station)
    return result
end

"""Unique consecutive flight IDs as a vector."""
function flights(ref::ItineraryRef)::Vector{String}
    isempty(ref.legs) && return String[]
    result = String[]
    prev = ""
    for k in ref.legs
        fid = flight_id(k)
        if fid != prev
            push!(result, fid)
            prev = fid
        end
    end
    return result
end

"""Flight chain as display string: `"UA4247 -> UA 284 -> UA3612"`"""
flights_str(ref::ItineraryRef) = join(flights(ref), " -> ")

"""Route as display string: `"LFT -> IAH -> ORD -> YYZ"`"""
route_str(ref::ItineraryRef) = join(String.(stops(ref)), " -> ")

"""
    `segment_id(r::LegRecord)::String`

Segment identifier: flight_id + itinerary variation + service type.
"""
segment_id(r::LegRecord) = "$(flight_id(r))/$(r.itinerary_var_id)/$(r.service_type)"

"""
    `full_id(r::LegRecord)::String`

Full leg identifier: segment_id + leg sequence number.
"""
full_id(r::LegRecord) = "$(segment_id(r))/L$(lpad(r.leg_sequence_number, 2, '0'))"


"""
    struct StationRecord

Immutable record for station/airport reference data.
"""
@kwdef struct StationRecord
    code::StationCode = ""
    country::InlineString3 = ""          # 2-char ISO
    state::InlineString3 = ""           # 2-char, may be empty
    city::InlineString3 = ""
    region::InlineString3 = ""          # 3-char IATA region
    latitude::Float64 = 0.
    longitude::Float64 = 0.
    utc_offset::Int16 = 0              # minutes from UTC
end


"""
    struct MCTResult

Result of an MCT lookup query. Contains the matched time, the status that was
queried vs the status that matched (may differ during fallback), suppression
flag, match source, SSIM8 specificity score, and the primary key of the matched
`mct` table row for audit traceability.
"""
@kwdef struct MCTResult
    time::Minutes                   # MCT in minutes (0 if suppressed)
    queried_status::MCTStatus       # the status that was queried
    matched_status::MCTStatus       # the status of the matched record
    suppressed::Bool
    source::MCTSource               # SOURCE_EXCEPTION, SOURCE_STATION_STANDARD, SOURCE_GLOBAL_DEFAULT
    specificity::UInt32             # higher = more specific match
    mct_id::Int32 = Int32(0)        # PK from mct table (0 = global default)
    matched_fields::UInt32 = UInt32(0)  # MCTRecord.specified bitmask of the matched record
end


"""
    struct SegmentRecord

Precomputed segment-level aggregates from the `segments` DuckDB table.
A segment is all legs sharing the same flight identity minus
leg_sequence_number on the same operating date.
"""
@kwdef struct SegmentRecord
    segment_hash::UInt64
    # Identity
    carrier::AirlineCode
    flight_number::FlightNumber
    operational_suffix::Char
    itinerary_var_id::UInt8
    itinerary_var_overflow::Char
    service_type::Char
    operating_date::UInt32          # packed YYYYMMDD
    # Structure
    num_legs::UInt8
    first_leg_seq::UInt8
    last_leg_seq::UInt8
    # Endpoints
    segment_departure_station::StationCode
    segment_arrival_station::StationCode
    # Distances & Circuity
    flown_distance::Distance        # sum of leg distances
    market_distance::Distance       # great-circle org→dst
    segment_circuity::Float32       # flown / market (1.0 = direct)
    # Timing
    segment_passenger_departure_time::Minutes
    segment_passenger_arrival_time::Minutes
    segment_aircraft_departure_time::Minutes
    segment_aircraft_arrival_time::Minutes
end

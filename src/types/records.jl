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

The **Flight Identifier** `(airline, flt_no, operational_suffix, itin_var,
itin_var_overflow, leg_seq, svc_type)` uniquely identifies a record in the
SSIM schedule. Dropping `leg_seq` gives the **Segment Identifier**.
"""
@kwdef struct LegRecord
    # ── Flight Identifier (SSIM Type 3 bytes 2-14) ──
    airline::AirlineCode
    flt_no::FlightNumber
    operational_suffix::Char
    itin_var::UInt8
    itin_var_overflow::Char
    leg_seq::UInt8
    svc_type::Char

    # ── Stations & Times ──
    org::StationCode
    dst::StationCode
    pax_dep::Minutes                # Passenger STD: HHMM → minutes since midnight
    pax_arr::Minutes                # Passenger STA
    ac_dep::Minutes                 # Aircraft STD (connection building uses this)
    ac_arr::Minutes                 # Aircraft STA
    dep_utc_offset::Int16           # minutes from UTC
    arr_utc_offset::Int16
    dep_date_var::Int8              # 0, 1, 2, or -1 for 'A'
    arr_date_var::Int8

    # ── Equipment & Terminals ──
    eqp::InlineString7
    body_type::Char                 # 'W'ide or 'N'arrow
    dep_term::InlineString3
    arr_term::InlineString3
    aircraft_owner::AirlineCode

    # ── Operating Date (from EDF expansion) ──
    operating_date::UInt32          # packed YYYYMMDD
    day_of_week::UInt8              # 1=Mon .. 7=Sun (ISO)

    # ── Schedule Dates & Frequency ──
    eff_date::UInt32                # packed YYYYMMDD
    disc_date::UInt32               # packed YYYYMMDD
    frequency::UInt8                # 7-bit DOW bitmask (Mon=bit0 .. Sun=bit6)

    # ── MCT & Restrictions ──
    mct_status_dep::Char            # 'D' or 'I'
    mct_status_arr::Char
    trc::InlineString15             # bytes 150-160, indexed by leg_seq
    trc_overflow::Char

    # ── Identity & Cross-reference ──
    record_serial::UInt32           # SSIM bytes 195-200
    row_number::UInt64              # monotonic counter assigned during ingest
    segment_hash::UInt64            # hash of segment identity fields
    distance::Distance

    # ── Codeshare / Operating Carrier (from DEI supplements) ──
    codeshare_airline::AirlineCode  # DEI 50
    codeshare_flt_no::FlightNumber  # DEI 50
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
flight_id(r::LegRecord) = "$(r.airline)$(lpad(r.flt_no, 4))"

# ── LegKey ───────────────────────────────────────────────────────────────────

"""
    struct LegKey

Compact reference to a leg in the schedule. Contains the SSIM Type 3 flight
identifier fields plus the database row ID and record serial for cross-referencing.
Codeshare fields default to self when the leg is operating.
"""
@kwdef struct LegKey
    # ── Cross-reference IDs ──
    row_number::UInt64 = UInt64(0)
    record_serial::UInt32 = UInt32(0)

    # ── Flight Identifier (SSIM Type 3 bytes 2-14) ──
    airline::AirlineCode = AirlineCode("")
    flt_no::FlightNumber = FlightNumber(0)
    operational_suffix::Char = ' '
    itin_var::UInt8 = UInt8(1)
    itin_var_overflow::Char = ' '
    leg_seq::UInt8 = UInt8(1)
    svc_type::Char = 'J'

    # ── Codeshare / Operating Carrier (from DEI 50) ──
    codeshare_airline::AirlineCode = AirlineCode("")
    codeshare_flt_no::FlightNumber = FlightNumber(0)

    # ── Station Pair ──
    org::StationCode = StationCode("")
    dst::StationCode = StationCode("")
end

"""
    `LegKey(r::LegRecord)::LegKey`

Construct a `LegKey` from a full `LegRecord`, copying identity fields.
Codeshare fields default to self when the leg is operating.
"""
function LegKey(r::LegRecord)
    cs_al = strip(String(r.codeshare_airline))
    airline_s = strip(String(r.airline))
    LegKey(
        row_number          = r.row_number,
        record_serial       = r.record_serial,
        airline             = r.airline,
        flt_no              = r.flt_no,
        operational_suffix  = r.operational_suffix,
        itin_var            = r.itin_var,
        itin_var_overflow   = r.itin_var_overflow,
        leg_seq             = r.leg_seq,
        svc_type            = r.svc_type,
        codeshare_airline   = (cs_al == "" || cs_al == airline_s) ? r.airline : r.codeshare_airline,
        codeshare_flt_no    = (cs_al == "" || cs_al == airline_s) ? r.flt_no : r.codeshare_flt_no,
        org                 = r.org,
        dst                 = r.dst,
    )
end

flight_id(k::LegKey) = "$(k.airline)$(lpad(k.flt_no, 4))"

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

"""Origin station code of the itinerary (first leg's org)."""
origin(ref::ItineraryRef) = isempty(ref.legs) ? StationCode("") : ref.legs[1].org

"""Destination station code (last leg's dst)."""
destination(ref::ItineraryRef) = isempty(ref.legs) ? StationCode("") : ref.legs[end].dst

"""Station codes visited in order (origin + intermediates + destination)."""
function stops(ref::ItineraryRef)::Vector{StationCode}
    isempty(ref.legs) && return StationCode[]
    result = StationCode[]
    for k in ref.legs
        (isempty(result) || result[end] != k.org) && push!(result, k.org)
    end
    push!(result, ref.legs[end].dst)
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
segment_id(r::LegRecord) = "$(flight_id(r))/$(r.itin_var)/$(r.svc_type)"

"""
    `full_id(r::LegRecord)::String`

Full leg identifier: segment_id + leg sequence number.
"""
full_id(r::LegRecord) = "$(segment_id(r))/L$(lpad(r.leg_seq, 2, '0'))"


"""
    struct StationRecord

Immutable record for station/airport reference data.
"""
@kwdef struct StationRecord
    code::StationCode = ""
    country::InlineString3 = ""          # 2-char ISO
    state::InlineString3 = ""           # 2-char, may be empty
    metro_area::InlineString3 = ""
    region::InlineString3 = ""          # 3-char IATA region
    lat::Float64 = 0.
    lng::Float64 = 0.
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
A segment is all legs sharing the same flight identity minus leg_seq
on the same operating date.
"""
@kwdef struct SegmentRecord
    segment_hash::UInt64
    # Identity
    airline::AirlineCode
    flt_no::FlightNumber
    op_suffix::Char
    itin_var::UInt8
    itin_var_overflow::Char
    svc_type::Char
    operating_date::UInt32          # packed YYYYMMDD
    # Structure
    num_legs::UInt8
    first_leg_seq::UInt8
    last_leg_seq::UInt8
    # Endpoints
    segment_org::StationCode
    segment_dst::StationCode
    # Distances & Circuity
    flown_distance::Distance        # sum of leg distances
    market_distance::Distance       # great-circle org→dst
    segment_circuity::Float32       # flown / market (1.0 = direct)
    # Timing
    segment_pax_dep::Minutes
    segment_pax_arr::Minutes
    segment_ac_dep::Minutes
    segment_ac_arr::Minutes
end

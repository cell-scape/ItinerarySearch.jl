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

Immutable, isbits flight leg record. Captures everything needed to identify,
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
    dei_10::InlineString31          # DEI 10: commercial duplicate list
    wet_lease::Bool                 # byte 149: 'Z' or 'S'
    dei_127::InlineString31         # DEI 127: operating airline disclosure

    # ── Booking ──
    prbd::InlineString31            # bytes 76-95
end

"""
    `flight_id(r::LegRecord)::String`

Human-readable flight identifier (e.g., "UA 354").
"""
flight_id(r::LegRecord) = "$(r.airline)$(lpad(r.flt_no, 4))"

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
    code::StationCode
    country::InlineString3          # 2-char ISO
    state::InlineString3            # 2-char, may be empty
    city::InlineString31
    region::InlineString3           # 3-char IATA region
    lat::Float64
    lng::Float64
    utc_offset::Int16               # minutes from UTC
end


"""
    struct MCTResult

Result of an MCT lookup query. Contains the matched time, the status that was
queried vs the status that matched (may differ during fallback), suppression
flag, match source, and SSIM8 specificity score.
"""
@kwdef struct MCTResult
    time::Minutes                   # MCT in minutes (0 if suppressed)
    queried_status::MCTStatus       # the status that was queried
    matched_status::MCTStatus       # the status of the matched record
    suppressed::Bool
    source::MCTSource               # SOURCE_EXCEPTION, SOURCE_STATION_STANDARD, SOURCE_GLOBAL_DEFAULT
    specificity::UInt32             # higher = more specific match
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

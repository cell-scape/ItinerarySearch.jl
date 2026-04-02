# src/ingest/newssim_materialize.jl — Materialize graph structs from newssim DuckDB table

"""
    `function parse_dms(s::AbstractString)::Float64`
---

# Description
- Parse a DMS (degrees-minutes-seconds) coordinate string into decimal degrees
- Format: "DD.MM.SSX" where X is N/S/E/W
- South and West are negative
- Returns 0.0 for empty or missing values

# Arguments
1. `s::AbstractString`: DMS string like "37.37.08N" or "122.23.30W"

# Returns
- `::Float64`: decimal degrees (negative for S and W)

# Examples
```julia
julia> parse_dms("37.37.08N")
37.61888888888889
julia> parse_dms("122.23.30W")
-122.39166666666668
julia> parse_dms("")
0.0
```
"""
function parse_dms(s::AbstractString)::Float64
    str = strip(String(s))
    isempty(str) && return 0.0

    # Last character is the direction
    dir = uppercase(str[end])
    coord = str[1:end-1]

    # Split on '.' — format is DD.MM.SS (degrees, minutes, seconds)
    parts = split(coord, '.')
    length(parts) < 3 && return 0.0

    degrees = tryparse(Float64, parts[1])
    minutes = tryparse(Float64, parts[2])
    seconds = tryparse(Float64, parts[3])

    degrees === nothing && return 0.0
    minutes === nothing && return 0.0
    seconds === nothing && return 0.0

    decimal = degrees + minutes / 60.0 + seconds / 3600.0

    (dir == 'S' || dir == 'W') && (decimal = -decimal)

    return decimal
end

"""
    `_parse_time_to_minutes(val)::Int16`

Parse a time value into minutes since midnight.
Handles DuckDB `Time` objects, strings like "HH:MM:SS.0", `missing`, and `nothing`.
"""
function _parse_time_to_minutes(val)::Int16
    (val === nothing || val === missing) && return Int16(0)

    # DuckDB may return a Dates.Time object
    if val isa Dates.Time
        return Int16(Dates.hour(val) * 60 + Dates.minute(val))
    end

    s = strip(String(val))
    isempty(s) && return Int16(0)

    parts = split(s, ':')
    length(parts) < 2 && return Int16(0)

    h = tryparse(Int, parts[1])
    m = tryparse(Int, parts[2])
    h === nothing && return Int16(0)
    m === nothing && return Int16(0)

    return Int16(h * 60 + m)
end

"""
    `_compute_utc_offset(local_dt_val, utc_dt_val)::Int16`

Compute the UTC offset in minutes by comparing local and UTC datetime values.
Handles DuckDB `DateTime` objects, strings, `missing`, and `nothing`.
Returns 0 if either value is missing or unparseable.
"""
function _compute_utc_offset(local_dt_val, utc_dt_val)::Int16
    (local_dt_val === nothing || local_dt_val === missing) && return Int16(0)
    (utc_dt_val === nothing || utc_dt_val === missing) && return Int16(0)

    local_dt = _parse_datetime(local_dt_val)
    utc_dt = _parse_datetime(utc_dt_val)
    (local_dt === nothing || utc_dt === nothing) && return Int16(0)

    diff_minutes = round(Int, Dates.value(local_dt - utc_dt) / 60_000)
    return Int16(diff_minutes)
end

"""
    `_parse_datetime(val)::Union{DateTime, Nothing}`

Parse a datetime value that may be a DateTime, Date, or string in ISO format.
"""
function _parse_datetime(val)::Union{DateTime,Nothing}
    val === nothing && return nothing
    val === missing && return nothing
    val isa DateTime && return val
    val isa Date && return DateTime(val)

    s = strip(String(val))
    isempty(s) && return nothing

    # Handle "YYYY-MM-DDTHH:MM:SS.0" or "YYYY-MM-DDTHH:MM:SS"
    # Strip trailing fractional seconds
    s = replace(s, r"\.\d+$" => "")

    try
        return DateTime(s, dateformat"yyyy-mm-ddTHH:MM:SS")
    catch
        try
            return DateTime(s, dateformat"yyyy-mm-dd")
        catch
            return nothing
        end
    end
end

"""
    `_compute_date_variation(dt_val, base_date::Date)::Int8`

Compute the date variation (0, 1, 2, or -1) by comparing the date part of a
datetime value to the base operating date.
"""
function _compute_date_variation(dt_val, base_date::Date)::Int8
    (dt_val === nothing || dt_val === missing) && return Int8(0)
    dt = _parse_datetime(dt_val)
    dt === nothing && return Int8(0)
    diff = Dates.value(Date(dt) - base_date)
    diff < -1 && return Int8(0)
    diff > 2 && return Int8(2)
    return Int8(diff)
end

"""
    `_correct_arrival_datetime(dep_utc_val, arr_utc_val)::Any`

Correct arrival UTC datetime for data producer errors where the arrival date
is not properly adjusted for day crossings. Some upstream SSIM parsers anchor
the arrival datetime to the operating date without applying the date variation
(+1/+2 days for overnight or transpacific flights).

Detection: if `arr_utc <= dep_utc`, the arrival datetime is clearly wrong
(a flight cannot arrive before it departs in UTC). Correction: add days to
the arrival until `arr_utc > dep_utc` and blocktime is <= 24 hours.

Returns the corrected arrival value, or the original if no correction needed.
"""
function _correct_arrival_datetime(dep_utc_val, arr_utc_val)
    (dep_utc_val === nothing || dep_utc_val === missing) && return arr_utc_val
    (arr_utc_val === nothing || arr_utc_val === missing) && return arr_utc_val

    dep_dt = _parse_datetime(dep_utc_val)
    arr_dt = _parse_datetime(arr_utc_val)
    (dep_dt === nothing || arr_dt === nothing) && return arr_utc_val

    # If arrival is already after departure, no correction needed
    arr_dt > dep_dt && return arr_utc_val

    # Add days until arrival > departure with reasonable blocktime (< 24h)
    for days in 1:3
        corrected = arr_dt + Day(days)
        diff_minutes = Dates.value(corrected - dep_dt) ÷ 60_000
        if diff_minutes > 0 && diff_minutes <= 1440
            return corrected
        end
    end

    # Fallback: +1 day even if blocktime > 24h (ultra-long-haul)
    return arr_dt + Day(1)
end

"""
    `function query_newssim_legs(store::DuckDBStore, window_start::Date, window_end::Date)::Vector{LegRecord}`
---

# Description
- Query the `newssim` table for leg records within a date window
- Filters strictly to leg records: `leg_or_seg = 'L'` only (segment records excluded)
- Excludes rows with `num_of_legs_in_seg > 0` as an additional safety check
- Constructs `LegRecord` instances with fields derived from CSV columns
- This is the newssim equivalent of `query_schedule_legs`

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store (must have `newssim` table)
2. `window_start::Date`: start of the search window (inclusive)
3. `window_end::Date`: end of the search window (inclusive)

# Returns
- `::Vector{LegRecord}`: one record per leg row in the date window

# Examples
```julia
julia> legs = query_newssim_legs(store, Date(2026, 2, 25), Date(2026, 2, 27));
```
"""
function query_newssim_legs(
    store::DuckDBStore,
    window_start::Date,
    window_end::Date,
)::Vector{LegRecord}
    result = DBInterface.execute(
        store.db,
        """
        SELECT *
        FROM newssim
        WHERE leg_or_seg = 'L'
          AND (CAST(num_of_legs_in_seg AS INT) = 0 OR num_of_legs_in_seg IS NULL)
          AND CAST(date AS DATE) >= CAST(? AS DATE)
          AND CAST(date AS DATE) <= CAST(? AS DATE)
        ORDER BY row_number
        """,
        [window_start, window_end],
    )

    records = LegRecord[]
    for r in result
        # Parse operating date
        date_val = _safe_missing(r.date, Date(1900, 1, 1))
        op_date = date_val isa DateTime ? Date(date_val) : (date_val isa Date ? date_val : Date(String(date_val)))

        packed_date = pack_date(op_date)
        dow = UInt8(Dates.dayofweek(op_date))

        # Parse times
        pax_dep = _parse_time_to_minutes(r.passenger_departure_time)
        pax_arr = _parse_time_to_minutes(r.passenger_arrival_time)

        # Correct arrival datetimes for day-crossing errors in source data.
        # Some upstream SSIM parsers don't apply date variation to the arrival
        # datetime, causing arr_utc to be before dep_utc for overnight flights.
        arr_dt_corrected = _correct_arrival_datetime(r.departure_datetime_utc, r.arrival_datetime_utc)
        arr_local_corrected = _correct_arrival_datetime(r.departure_datetime, r.arrival_datetime)

        # UTC offsets from datetime comparisons
        dep_utc = _compute_utc_offset(r.departure_datetime, r.departure_datetime_utc)
        arr_utc = _compute_utc_offset(arr_local_corrected, arr_dt_corrected)

        # Date variations
        dep_date_var = _compute_date_variation(r.departure_datetime, op_date)
        arr_date_var = _compute_date_variation(arr_local_corrected, op_date)

        # Carrier and flight number
        carrier_str = _safe_string(r.carrier)
        flt_num = Int16(_safe_missing(r.flight_number, 0))

        # Administrating carrier (default to marketing carrier)
        admin_carrier_str = _safe_string(hasproperty(r, :administrating_carrier) ? r.administrating_carrier : nothing)
        admin_carrier = isempty(admin_carrier_str) ? AirlineCode(carrier_str) : AirlineCode(admin_carrier_str)
        admin_flt_raw = hasproperty(r, :administrating_carrier_flight_number) ?
            _safe_missing(r.administrating_carrier_flight_number, nothing) : nothing
        admin_flt = if admin_flt_raw === nothing || admin_flt_raw === missing
            flt_num
        else
            v = tryparse(Int16, string(admin_flt_raw))
            v === nothing ? flt_num : v
        end

        # Itinerary var ID — the CSV value may encode both the variation (0-99)
        # and the overflow (hundreds place). E.g., 102 → itin_var=2, overflow='1'.
        itin_var_raw = hasproperty(r, :itinerary_var_id) ? _safe_missing(r.itinerary_var_id, 1) : 1
        itin_var_full = itin_var_raw isa AbstractString ? (tryparse(Int, itin_var_raw) !== nothing ? parse(Int, itin_var_raw) : 1) : Int(itin_var_raw)
        itin_var = UInt8(itin_var_full % 100)
        itin_var_overflow = itin_var_full >= 100 ? Char('0' + (itin_var_full ÷ 100)) : ' '

        # Leg sequence number
        leg_seq_raw = hasproperty(r, :leg_sequence_number) ? _safe_missing(r.leg_sequence_number, 1) : 1
        leg_seq = UInt8(leg_seq_raw isa AbstractString ? (tryparse(Int, leg_seq_raw) !== nothing ? parse(Int, leg_seq_raw) : 1) : Int(leg_seq_raw))

        # DEI 127
        dei_127_str = hasproperty(r, :DEI_127) ? _safe_string(r.DEI_127) : ""

        # Traffic restriction
        trl = hasproperty(r, :traffic_restriction_for_leg) ? _safe_string(r.traffic_restriction_for_leg) : ""
        # Clean up the "." placeholder used in some CSV exports
        trl == "." && (trl = "")

        # PRBD
        prbd_str = hasproperty(r, :prbd) ? _safe_string(r.prbd) : ""

        # Row number
        row_num = UInt64(_safe_missing(r.row_number, 0))

        # Build segment hash from identity fields (must include overflow to match SSIM model)
        seg_hash = hash(carrier_str, hash(flt_num, hash(itin_var, hash(itin_var_overflow, hash(packed_date)))))

        rec = LegRecord(
            carrier                          = AirlineCode(carrier_str),
            flight_number                    = flt_num,
            operational_suffix               = ' ',
            itinerary_var_id                 = itin_var,
            itinerary_var_overflow           = itin_var_overflow,
            leg_sequence_number              = leg_seq,
            service_type                     = _first_char(r.service_type, 'J'),
            departure_station                = StationCode(_safe_string(r.departure_station)),
            arrival_station                  = StationCode(_safe_string(r.arrival_station)),
            passenger_departure_time         = pax_dep,
            passenger_arrival_time           = pax_arr,
            aircraft_departure_time          = pax_dep,   # CSV doesn't distinguish
            aircraft_arrival_time            = pax_arr,
            departure_utc_offset             = dep_utc,
            arrival_utc_offset               = arr_utc,
            departure_date_variation         = dep_date_var,
            arrival_date_variation           = arr_date_var,
            aircraft_type                    = InlineString7(_safe_string(r.aircraft_type)),
            body_type                        = _first_char(r.body_type, ' '),
            departure_terminal               = InlineString3(_safe_string(r.departure_terminal)),
            arrival_terminal                 = InlineString3(_safe_string(r.arrival_terminal)),
            aircraft_owner                   = AirlineCode(_safe_string(hasproperty(r, :aircraft_owner) ? r.aircraft_owner : nothing)),
            operating_date                   = packed_date,
            day_of_week                      = dow,
            effective_date                   = packed_date,  # date-expanded
            discontinue_date                 = packed_date,
            frequency                        = UInt8(0x7f),  # all days
            dep_intl_dom                     = _first_char(hasproperty(r, :departure_international_domestic_status) ? r.departure_international_domestic_status : nothing, ' '),
            arr_intl_dom                     = _first_char(hasproperty(r, :arrival_international_domestic_status) ? r.arrival_international_domestic_status : nothing, ' '),
            traffic_restriction_for_leg      = InlineString15(trl),
            traffic_restriction_overflow     = ' ',
            record_serial                    = UInt32(0),
            row_number                       = row_num,
            segment_hash                     = UInt64(seg_hash),
            distance                         = Float32(0),   # gap-filled from geodesic
            administrating_carrier           = admin_carrier,
            administrating_carrier_flight_number = admin_flt,
            dei_10                           = "",
            wet_lease                        = false,
            dei_127                          = dei_127_str,
            prbd                             = InlineString31(prbd_str),
        )
        push!(records, rec)
    end

    @info "Queried newssim legs" count = length(records) window_start window_end
    return records
end

"""
    `function query_newssim_station(store::DuckDBStore, code::StationCode)::Union{StationRecord, Nothing}`
---

# Description
- Query the `newssim` table for station metadata by IATA code
- The CSV has geo data on each row, so this queries for rows where the station
  appears as either departure or arrival station
- Parses DMS lat/lng coordinates into decimal degrees

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store (must have `newssim` table)
2. `code::StationCode`: the IATA station code to look up

# Returns
- `::Union{StationRecord, Nothing}`: station record, or `nothing` if not found

# Examples
```julia
julia> stn = query_newssim_station(store, StationCode("SFO"));
julia> stn.country
"US"
```
"""
function query_newssim_station(
    store::DuckDBStore,
    code::StationCode,
)::Union{StationRecord,Nothing}
    code_str = strip(String(code))

    # Try departure side first — prefer rows with non-empty geo data
    result = DBInterface.execute(
        store.db,
        """
        SELECT
            departure_country AS country,
            departure_state AS state,
            departure_latitude AS latitude,
            departure_longitude AS longitude,
            departure_region AS region,
            departure_city AS city,
            departure_datetime AS local_dt,
            departure_datetime_utc AS utc_dt
        FROM newssim
        WHERE departure_station = ?
          AND departure_latitude IS NOT NULL
          AND departure_latitude != ''
        LIMIT 1
        """,
        [code_str],
    )
    rows = collect(result)

    if isempty(rows)
        # Try arrival side
        result = DBInterface.execute(
            store.db,
            """
            SELECT
                arrival_country AS country,
                arrival_state AS state,
                arrival_latitude AS latitude,
                arrival_longitude AS longitude,
                arrival_region AS region,
                arrival_city AS city,
                arrival_datetime AS local_dt,
                arrival_datetime_utc AS utc_dt
            FROM newssim
            WHERE arrival_station = ?
              AND arrival_latitude IS NOT NULL
              AND arrival_latitude != ''
            LIMIT 1
            """,
            [code_str],
        )
        rows = collect(result)
    end

    isempty(rows) && return nothing

    r = rows[1]
    country_str = _safe_string(r.country)
    state_str = _safe_string(r.state)
    lat_str = _safe_string(r.latitude)
    lng_str = _safe_string(r.longitude)
    region_raw = _safe_string(r.region)
    city_str = _safe_string(r.city)

    # Region may contain comma-separated values like "EUR, SCH". The more
    # specific region takes precedence for MCT matching — Schengen (SCH)
    # has different MCT rules than general European (EUR). Use the LAST
    # element which is the most specific; fall back to the first if only one.
    region_parts = strip.(split(region_raw, ','))
    region_str = length(region_parts) > 1 ? region_parts[end] : region_parts[1]

    lat = parse_dms(lat_str)
    lng = parse_dms(lng_str)

    # Derive UTC offset from the datetime pair if available
    utc_off = _compute_utc_offset(
        hasproperty(r, :local_dt) ? r.local_dt : nothing,
        hasproperty(r, :utc_dt) ? r.utc_dt : nothing,
    )

    return StationRecord(
        code       = StationCode(code_str),
        country    = InlineString3(length(country_str) > 3 ? country_str[1:3] : country_str),
        state      = InlineString3(length(state_str) > 3 ? state_str[1:3] : state_str),
        city       = InlineString3(length(city_str) > 3 ? city_str[1:3] : city_str),
        region     = InlineString3(length(region_str) > 3 ? region_str[1:3] : region_str),
        latitude   = lat,
        longitude  = lng,
        utc_offset = utc_off,
    )
end

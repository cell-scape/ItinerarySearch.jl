# src/ingest/newssim_materialize.jl — Materialize graph structs from newssim DuckDB table
#
# Uses station UTC offsets from the airports reference file when available,
# rather than deriving from the CSV's datetime columns (which may have
# day-crossing errors from upstream SSIM parsers).

# ── Timezone offset loader ──────────────────────────────────────────────────────

"""
    `load_timezone_offsets(path::AbstractString)::Dict{StationCode, Int16}`
---

# Description
- Load station UTC offsets from an airports reference file
- Supports two formats (auto-detected):
  - Tab-delimited: `CODE\\tNAME\\tMETRO\\tCOUNTRY\\tSTATE\\tLAT\\tLNG\\tOFFSET_MIN`
  - Fixed-width (OAG mdstua.txt): station code at cols 7-9, UTC offset ±HHMM at cols 11-15
- Returns a dictionary mapping 3-letter station codes to UTC offset in minutes
- These are standard (non-DST) offsets; DST awareness is a future enhancement
- Handles duplicate entries by keeping the first (airport-level, not city-level)

# Arguments
1. `path::AbstractString`: path to airports reference file (may be compressed)

# Returns
- `::Dict{StationCode, Int16}`: station code → UTC offset in minutes

# Examples
```julia
julia> tz = load_timezone_offsets("data/input/airports_tab.txt");
julia> tz[StationCode("ORD")]
-360
```
"""
function load_timezone_offsets(path::AbstractString)::Dict{StationCode,Int16}
    offsets = Dict{StationCode,Int16}()
    io = open_maybe_compressed(path)
    try
        first_line = readline(io)

        if occursin('\t', first_line)
            # Tab-delimited format: CODE\tNAME\t...\tOFFSET_MIN
            _parse_tz_tab_line!(offsets, first_line)
            for line in eachline(io)
                _parse_tz_tab_line!(offsets, line)
            end
        else
            # Fixed-width format: station code at 7-9, UTC offset ±HHMM at 11-15
            _parse_tz_fwf_line!(offsets, first_line)
            for line in eachline(io)
                _parse_tz_fwf_line!(offsets, line)
            end
        end
    finally
        close(io)
    end
    @info "Loaded timezone offsets" count = length(offsets) path
    return offsets
end

function _parse_tz_tab_line!(offsets::Dict{StationCode,Int16}, line::AbstractString)
    parts = split(line, '\t')
    length(parts) >= 8 || return
    code = strip(String(parts[1]))
    (length(code) != 3 || any(!isletter, code)) && return
    haskey(offsets, StationCode(code)) && return
    v = tryparse(Int16, strip(String(parts[8])))
    v === nothing && return
    offsets[StationCode(code)] = v
end

function _parse_tz_fwf_line!(offsets::Dict{StationCode,Int16}, line::AbstractString)
    length(line) >= 15 || return
    code = String(line[7:9])
    (length(code) != 3 || any(!isletter, code)) && return
    haskey(offsets, StationCode(code)) && return
    offset_str = String(line[11:15])
    # Parse ±HHMM format
    s = strip(offset_str)
    length(s) < 5 && return
    sign = s[1] == '-' ? Int16(-1) : Int16(1)
    hhmm = tryparse(Int, s[2:end])
    hhmm === nothing && return
    offsets[StationCode(code)] = sign * Int16(div(hhmm, 100) * 60 + rem(hhmm, 100))
end

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
    window_end::Date;
    tz_offsets::Dict{StationCode,Int16}=Dict{StationCode,Int16}(),
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

        # Station codes for timezone lookup
        dep_stn = StationCode(_safe_string(r.departure_station))
        arr_stn = StationCode(_safe_string(r.arrival_station))

        # UTC offsets: prefer reference timezone data over CSV datetime columns.
        # The CSV's UTC datetimes have systematic day-crossing errors from
        # upstream SSIM parsers that don't apply date variation properly.
        dep_utc_off = get(tz_offsets, dep_stn, nothing)
        arr_utc_off = get(tz_offsets, arr_stn, nothing)

        if dep_utc_off !== nothing && arr_utc_off !== nothing
            # Use reference timezone offsets — reliable, no day-crossing bugs
            dep_utc = dep_utc_off
            arr_utc = arr_utc_off

            # Compute arrival date variation from local times and known UTC offsets.
            # Convert both times to UTC-minutes-since-midnight-of-operating-date:
            dep_utc_mins = Int(pax_dep) - Int(dep_utc)   # can be negative (dep before UTC midnight)
            arr_utc_mins = Int(pax_arr) - Int(arr_utc)   # can be > 1440 or < 0

            # The flight time in UTC must be positive and <= 24h for a single leg.
            # Find the integer number of days N such that:
            #   0 < (arr_utc_mins + N*1440) - dep_utc_mins <= 1440
            # This gives us the day offset in UTC. The arrival local date variation
            # is then derived by converting back to local time.
            raw_diff = arr_utc_mins - dep_utc_mins
            # Normalize raw_diff to 1..1440 range by adding/subtracting days
            utc_day_offset = if raw_diff > 0 && raw_diff <= 1440
                0
            elseif raw_diff <= 0
                # Need to add days
                d = 1
                while raw_diff + d * 1440 <= 0 && d < 4; d += 1; end
                d
            else
                # raw_diff > 1440 — need to subtract days
                d = -1
                while raw_diff + d * 1440 > 1440 && d > -4; d -= 1; end
                d
            end

            # Arrival UTC minute = dep_utc_mins + flight_time
            # Arrival local minute = arr_utc_mins + utc_day_offset * 1440
            # The local arrival day relative to operating date:
            arr_local_total_mins = Int(pax_arr) + utc_day_offset * 1440
            arr_date_var = if arr_local_total_mins < 0
                Int8(-1)
            elseif arr_local_total_mins < 1440
                Int8(0)
            elseif arr_local_total_mins < 2880
                Int8(1)
            else
                Int8(2)
            end

            dep_date_var = _compute_date_variation(r.departure_datetime, op_date)
        else
            # Fallback: derive from CSV datetime columns with day-crossing correction
            arr_dt_corrected = _correct_arrival_datetime(r.departure_datetime_utc, r.arrival_datetime_utc)
            arr_local_corrected = _correct_arrival_datetime(r.departure_datetime, r.arrival_datetime)

            dep_utc = _compute_utc_offset(r.departure_datetime, r.departure_datetime_utc)
            arr_utc = _compute_utc_offset(arr_local_corrected, arr_dt_corrected)

            dep_date_var = _compute_date_variation(r.departure_datetime, op_date)
            arr_date_var = _compute_date_variation(arr_local_corrected, op_date)
        end

        # Carrier and flight number
        carrier_str = _safe_string(r.carrier)
        flt_num = Int16(_safe_missing(r.flight_number, 0))

        # Operating carrier (default to marketing carrier; CSV column still called administrating_carrier)
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
            operating_carrier                = admin_carrier,
            operating_flight_number          = admin_flt,
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
    code::StationCode;
    tz_offsets::Dict{StationCode,Int16}=Dict{StationCode,Int16}(),
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

    # Use reference timezone offset if available; fall back to datetime derivation
    utc_off = get(tz_offsets, StationCode(code_str), nothing)
    if utc_off === nothing
        utc_off = _compute_utc_offset(
            hasproperty(r, :local_dt) ? r.local_dt : nothing,
            hasproperty(r, :utc_dt) ? r.utc_dt : nothing,
        )
    end

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

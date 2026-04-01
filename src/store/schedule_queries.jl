# src/store/schedule_queries.jl — Schedule-level (unexpanded) leg and segment queries

"""
    `_row_to_schedule_leg(r)::LegRecord`

Convert a DuckDB row from the raw `legs` table to a `LegRecord`.
Unlike `_row_to_leg` (which reads `legs_with_operating`), this converter
handles the columns present only in `legs` (`aircraft_departure_time`, `aircraft_arrival_time`,
`record_serial`, `traffic_restriction_overflow`) and synthesizes the EDF-only fields:

- `operating_date` is set to `effective_date` (representative schedule date)
- `day_of_week` is set to 0 (unused at schedule level; `frequency` is authoritative)

Administrating carrier fields (`administrating_carrier`, `administrating_carrier_flight_number`,
`dei_10`, `dei_127`) are left as empty/zero because DEI join is omitted at schedule level.
"""
function _row_to_schedule_leg(r)::LegRecord
    eff_val = _safe_missing(r.effective_date, Date(1900, 1, 1))
    disc_val = _safe_missing(r.discontinue_date, Date(2099, 12, 31))
    eff_d = eff_val isa DateTime ? Date(eff_val) : Date(eff_val)
    disc_d = disc_val isa DateTime ? Date(disc_val) : Date(disc_val)

    LegRecord(
        carrier              = AirlineCode(_safe_string(r.carrier)),
        flight_number        = Int16(_safe_missing(r.flight_number, 0)),
        operational_suffix   = _first_char(r.op_suffix, ' '),
        itinerary_var_id     = UInt8(_safe_missing(r.itinerary_var_id, 0)),
        itinerary_var_overflow = _first_char(r.itinerary_var_overflow, ' '),
        leg_sequence_number  = UInt8(_safe_missing(r.leg_sequence_number, 0)),
        service_type         = _first_char(r.service_type, ' '),
        departure_station    = StationCode(_safe_string(r.departure_station)),
        arrival_station      = StationCode(_safe_string(r.arrival_station)),
        passenger_departure_time = Int16(_safe_missing(r.passenger_departure_time, 0)),
        passenger_arrival_time   = Int16(_safe_missing(r.passenger_arrival_time, 0)),
        aircraft_departure_time  = Int16(_safe_missing(r.aircraft_departure_time, 0)),
        aircraft_arrival_time    = Int16(_safe_missing(r.aircraft_arrival_time, 0)),
        departure_utc_offset = Int16(_safe_missing(r.departure_utc_offset, 0)),
        arrival_utc_offset   = Int16(_safe_missing(r.arrival_utc_offset, 0)),
        departure_date_variation = Int8(_safe_missing(r.departure_date_variation, 0)),
        arrival_date_variation   = Int8(_safe_missing(r.arrival_date_variation, 0)),
        aircraft_type        = InlineString7(_safe_string(r.aircraft_type)),
        body_type            = _first_char(r.body_type, ' '),
        departure_terminal   = InlineString3(_safe_string(r.departure_terminal)),
        arrival_terminal     = InlineString3(_safe_string(r.arrival_terminal)),
        aircraft_owner       = AirlineCode(_safe_string(r.aircraft_owner)),
        # operating_date = effective_date (representative date); day_of_week unused at schedule level
        operating_date       = pack_date(eff_d),
        day_of_week          = UInt8(0),
        effective_date       = pack_date(eff_d),
        discontinue_date     = pack_date(disc_d),
        frequency            = UInt8(_safe_missing(r.frequency, 0)),
        dep_intl_dom         = _first_char(r.dep_intl_dom, ' '),
        arr_intl_dom         = _first_char(r.arr_intl_dom, ' '),
        traffic_restriction_for_leg = InlineString15(_safe_string(r.traffic_restriction_for_leg)),
        traffic_restriction_overflow = _first_char(r.traffic_restriction_overflow, ' '),
        record_serial        = UInt32(_safe_missing(r.record_serial, 0)),
        row_number           = UInt64(_safe_missing(r.row_id, 0)),
        segment_hash         = UInt64(0),
        distance             = Float32(_safe_missing(r.distance, 0.0)),
        administrating_carrier = AirlineCode(_safe_string(hasproperty(r, :administrating_carrier) ? r.administrating_carrier : nothing)),
        administrating_carrier_flight_number = Int16(_safe_missing(hasproperty(r, :administrating_carrier_flight_number) ? r.administrating_carrier_flight_number : nothing, 0)),
        dei_10               = _safe_string(hasproperty(r, :dei_10) ? r.dei_10 : nothing),
        wet_lease            = Bool(_safe_missing(r.wet_lease, false)),
        dei_127              = _safe_string(hasproperty(r, :dei_127) ? r.dei_127 : nothing),
        prbd                 = InlineString31(_safe_string(r.prbd)),
    )
end

"""
    `function query_schedule_legs(store::DuckDBStore, window_start::Date, window_end::Date)::Vector{LegRecord}`
---

# Description
- Query the unexpanded `legs` table for schedule records whose effective period
  overlaps `[window_start, window_end]`
- Returns one `LegRecord` per schedule entry, not per operating date
- The `operating_date` field is set to `eff_date` (representative date)
- The `day_of_week` field is set to 0 (the `frequency` bitmask is authoritative
  at schedule level)
- Joins DEI 50 (codeshare), DEI 10 (commercial duplicates), DEI 127 (operating disclosure)

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `window_start::Date`: start of the search window (inclusive)
3. `window_end::Date`: end of the search window (inclusive)

# Returns
- `::Vector{LegRecord}`: one record per schedule entry active during the window

# Examples
```julia
julia> legs = query_schedule_legs(store, Date(2026, 6, 1), Date(2026, 6, 30));
```
"""
function query_schedule_legs(
    store::DuckDBStore,
    window_start::Date,
    window_end::Date,
)::Vector{LegRecord}
    result = DBInterface.execute(
        store.db,
        """
        SELECT l.*,
            TRIM(SUBSTRING(dei50.data, 1, 3)) AS administrating_carrier,
            CAST(NULLIF(TRIM(SUBSTRING(dei50.data, 4, 4)), '') AS SMALLINT) AS administrating_carrier_flight_number,
            dei10.data AS dei_10,
            dei127.data AS dei_127
        FROM legs l
        LEFT JOIN dei dei50  ON dei50.row_id  = l.row_id AND dei50.dei_code  = 50
        LEFT JOIN dei dei10  ON dei10.row_id  = l.row_id AND dei10.dei_code  = 10
        LEFT JOIN dei dei127 ON dei127.row_id = l.row_id AND dei127.dei_code = 127
        WHERE l.effective_date <= ?
          AND l.discontinue_date >= ?
        ORDER BY l.row_id
        """,
        [window_end, window_start],
    )
    [_row_to_schedule_leg(r) for r in result]
end

"""
    `function query_schedule_segments(store::DuckDBStore, window_start::Date, window_end::Date)::Vector{SegmentRecord}`
---

# Description
- Query schedule-level segment aggregates from the raw `legs` table
- Groups legs by segment identity `(airline, flt_no, op_suffix, itin_var,
  itin_var_overflow, svc_type)` without expanding by operating date
- Returns one `SegmentRecord` per unique schedule segment active during
  `[window_start, window_end]`
- `operating_date` is set to the `eff_date` of the first leg in each group
- `segment_hash` is computed with the same hash expression used by
  `_build_segments!`, but substituting `eff_date` for `operating_date`
- `market_distance` and `segment_circuity` are left as 0 (no spatial join
  at schedule level)

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `window_start::Date`: start of the search window (inclusive)
3. `window_end::Date`: end of the search window (inclusive)

# Returns
- `::Vector{SegmentRecord}`: one record per unique schedule segment in the window

# Examples
```julia
julia> segs = query_schedule_segments(store, Date(2026, 6, 1), Date(2026, 6, 30));
```
"""
function query_schedule_segments(
    store::DuckDBStore,
    window_start::Date,
    window_end::Date,
)::Vector{SegmentRecord}
    result = DBInterface.execute(
        store.db,
        """
        SELECT
            hash(carrier || CAST(flight_number AS VARCHAR) || op_suffix
                 || CAST(itinerary_var_id AS VARCHAR) || itinerary_var_overflow
                 || service_type || CAST(MIN(effective_date) AS VARCHAR)) AS segment_hash,
            carrier,
            flight_number,
            op_suffix,
            itinerary_var_id,
            itinerary_var_overflow,
            service_type,
            MIN(effective_date)                                          AS effective_date,
            FIRST(leg_sequence_number ORDER BY leg_sequence_number)      AS first_leg_seq,
            LAST(leg_sequence_number ORDER BY leg_sequence_number)       AS last_leg_seq,
            CAST(COUNT(*) AS INTEGER)                                    AS num_legs,
            FIRST(departure_station ORDER BY leg_sequence_number)        AS segment_departure_station,
            LAST(arrival_station ORDER BY leg_sequence_number)           AS segment_arrival_station,
            COALESCE(SUM(distance), 0)                                   AS flown_distance,
            FIRST(passenger_departure_time ORDER BY leg_sequence_number) AS segment_passenger_departure_time,
            LAST(passenger_arrival_time ORDER BY leg_sequence_number)    AS segment_passenger_arrival_time,
            FIRST(aircraft_departure_time ORDER BY leg_sequence_number)  AS segment_aircraft_departure_time,
            LAST(aircraft_arrival_time ORDER BY leg_sequence_number)     AS segment_aircraft_arrival_time
        FROM legs
        WHERE effective_date <= ?
          AND discontinue_date >= ?
        GROUP BY carrier, flight_number, op_suffix, itinerary_var_id, itinerary_var_overflow, service_type
        ORDER BY carrier, flight_number, op_suffix, itinerary_var_id, itinerary_var_overflow, service_type
        """,
        [window_end, window_start],
    )
    records = SegmentRecord[]
    for r in result
        eff_val = _safe_missing(r.effective_date, Date(1900, 1, 1))
        eff_d = eff_val isa DateTime ? Date(eff_val) : Date(eff_val)
        push!(
            records,
            SegmentRecord(
                segment_hash      = UInt64(r.segment_hash),
                carrier           = AirlineCode(_safe_string(r.carrier)),
                flight_number     = Int16(_safe_missing(r.flight_number, 0)),
                operational_suffix = _first_char(r.op_suffix, ' '),
                itinerary_var_id  = UInt8(_safe_missing(r.itinerary_var_id, 0)),
                itinerary_var_overflow = _first_char(r.itinerary_var_overflow, ' '),
                service_type      = _first_char(r.service_type, ' '),
                operating_date    = pack_date(eff_d),
                num_legs          = UInt8(_safe_missing(r.num_legs, 0)),
                first_leg_seq     = UInt8(_safe_missing(r.first_leg_seq, 0)),
                last_leg_seq      = UInt8(_safe_missing(r.last_leg_seq, 0)),
                segment_departure_station = StationCode(_safe_string(r.segment_departure_station)),
                segment_arrival_station   = StationCode(_safe_string(r.segment_arrival_station)),
                flown_distance    = Float32(_safe_missing(r.flown_distance, 0.0)),
                market_distance   = Float32(0),
                segment_circuity  = Float32(0),
                segment_passenger_departure_time = Int16(_safe_missing(r.segment_passenger_departure_time, 0)),
                segment_passenger_arrival_time   = Int16(_safe_missing(r.segment_passenger_arrival_time, 0)),
                segment_aircraft_departure_time  = Int16(_safe_missing(r.segment_aircraft_departure_time, 0)),
                segment_aircraft_arrival_time    = Int16(_safe_missing(r.segment_aircraft_arrival_time, 0)),
            ),
        )
    end
    records
end

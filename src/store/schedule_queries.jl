# src/store/schedule_queries.jl — Schedule-level (unexpanded) leg and segment queries

"""
    `_row_to_schedule_leg(r)::LegRecord`

Convert a DuckDB row from the raw `legs` table to a `LegRecord`.
Unlike `_row_to_leg` (which reads `legs_with_operating`), this converter
handles the columns present only in `legs` (`ac_dep_mins`, `ac_arr_mins`,
`record_serial`, `trc_overflow`) and synthesizes the EDF-only fields:

- `operating_date` is set to `eff_date` (representative schedule date)
- `day_of_week` is set to 0 (unused at schedule level; `frequency` is authoritative)

Codeshare fields (`codeshare_airline`, `codeshare_flt_no`, `dei_10`, `dei_127`)
are left as empty/zero because DEI join is omitted at schedule level.
"""
function _row_to_schedule_leg(r)::LegRecord
    eff_val = _safe_missing(r.eff_date, Date(1900, 1, 1))
    disc_val = _safe_missing(r.disc_date, Date(2099, 12, 31))
    eff_d = eff_val isa DateTime ? Date(eff_val) : Date(eff_val)
    disc_d = disc_val isa DateTime ? Date(disc_val) : Date(disc_val)

    LegRecord(
        airline              = AirlineCode(_safe_string(r.airline)),
        flt_no               = Int16(_safe_missing(r.flt_no, 0)),
        operational_suffix   = _first_char(r.op_suffix, ' '),
        itin_var             = UInt8(_safe_missing(r.itin_var, 0)),
        itin_var_overflow    = _first_char(r.itin_var_overflow, ' '),
        leg_seq              = UInt8(_safe_missing(r.leg_seq, 0)),
        svc_type             = _first_char(r.svc_type, ' '),
        org                  = StationCode(_safe_string(r.org)),
        dst                  = StationCode(_safe_string(r.dst)),
        pax_dep              = Int16(_safe_missing(r.pax_dep_mins, 0)),
        pax_arr              = Int16(_safe_missing(r.pax_arr_mins, 0)),
        ac_dep               = Int16(_safe_missing(r.ac_dep_mins, 0)),
        ac_arr               = Int16(_safe_missing(r.ac_arr_mins, 0)),
        dep_utc_offset       = Int16(_safe_missing(r.dep_utc_offset, 0)),
        arr_utc_offset       = Int16(_safe_missing(r.arr_utc_offset, 0)),
        dep_date_var         = Int8(_safe_missing(r.dep_date_var, 0)),
        arr_date_var         = Int8(_safe_missing(r.arr_date_var, 0)),
        eqp                  = InlineString7(_safe_string(r.eqp)),
        body_type            = _first_char(r.body_type, ' '),
        dep_term             = InlineString3(_safe_string(r.dep_term)),
        arr_term             = InlineString3(_safe_string(r.arr_term)),
        aircraft_owner       = AirlineCode(_safe_string(r.aircraft_owner)),
        # operating_date = eff_date (representative date); day_of_week unused at schedule level
        operating_date       = pack_date(eff_d),
        day_of_week          = UInt8(0),
        eff_date             = pack_date(eff_d),
        disc_date            = pack_date(disc_d),
        frequency            = UInt8(_safe_missing(r.frequency, 0)),
        mct_status_dep       = _first_char(r.mct_dep, ' '),
        mct_status_arr       = _first_char(r.mct_arr, ' '),
        trc                  = InlineString15(_safe_string(r.trc)),
        trc_overflow         = _first_char(r.trc_overflow, ' '),
        record_serial        = UInt32(_safe_missing(r.record_serial, 0)),
        row_number           = UInt64(_safe_missing(r.row_id, 0)),
        segment_hash         = UInt64(0),
        distance             = Float32(_safe_missing(r.distance, 0.0)),
        codeshare_airline    = AirlineCode(_safe_string(hasproperty(r, :codeshare_airline) ? r.codeshare_airline : nothing)),
        codeshare_flt_no     = Int16(_safe_missing(hasproperty(r, :codeshare_flt_no) ? r.codeshare_flt_no : nothing, 0)),
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
            TRIM(SUBSTRING(dei50.data, 1, 3)) AS codeshare_airline,
            CAST(NULLIF(TRIM(SUBSTRING(dei50.data, 4, 4)), '') AS SMALLINT) AS codeshare_flt_no,
            dei10.data AS dei_10,
            dei127.data AS dei_127
        FROM legs l
        LEFT JOIN dei dei50  ON dei50.row_id  = l.row_id AND dei50.dei_code  = 50
        LEFT JOIN dei dei10  ON dei10.row_id  = l.row_id AND dei10.dei_code  = 10
        LEFT JOIN dei dei127 ON dei127.row_id = l.row_id AND dei127.dei_code = 127
        WHERE l.eff_date <= ?
          AND l.disc_date >= ?
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
            hash(airline || CAST(flt_no AS VARCHAR) || op_suffix
                 || CAST(itin_var AS VARCHAR) || itin_var_overflow
                 || svc_type || CAST(MIN(eff_date) AS VARCHAR)) AS segment_hash,
            airline,
            flt_no,
            op_suffix,
            itin_var,
            itin_var_overflow,
            svc_type,
            MIN(eff_date)                               AS eff_date,
            FIRST(leg_seq ORDER BY leg_seq)             AS first_leg_seq,
            LAST(leg_seq ORDER BY leg_seq)              AS last_leg_seq,
            CAST(COUNT(*) AS INTEGER)                   AS num_legs,
            FIRST(org ORDER BY leg_seq)                 AS segment_org,
            LAST(dst ORDER BY leg_seq)                  AS segment_dst,
            COALESCE(SUM(distance), 0)                  AS flown_distance,
            FIRST(pax_dep_mins ORDER BY leg_seq)        AS segment_pax_dep,
            LAST(pax_arr_mins ORDER BY leg_seq)         AS segment_pax_arr,
            FIRST(ac_dep_mins ORDER BY leg_seq)         AS segment_ac_dep,
            LAST(ac_arr_mins ORDER BY leg_seq)          AS segment_ac_arr
        FROM legs
        WHERE eff_date <= ?
          AND disc_date >= ?
        GROUP BY airline, flt_no, op_suffix, itin_var, itin_var_overflow, svc_type
        ORDER BY airline, flt_no, op_suffix, itin_var, itin_var_overflow, svc_type
        """,
        [window_end, window_start],
    )
    records = SegmentRecord[]
    for r in result
        eff_val = _safe_missing(r.eff_date, Date(1900, 1, 1))
        eff_d = eff_val isa DateTime ? Date(eff_val) : Date(eff_val)
        push!(
            records,
            SegmentRecord(
                segment_hash      = UInt64(r.segment_hash),
                airline           = AirlineCode(_safe_string(r.airline)),
                flt_no            = Int16(_safe_missing(r.flt_no, 0)),
                op_suffix         = _first_char(r.op_suffix, ' '),
                itin_var          = UInt8(_safe_missing(r.itin_var, 0)),
                itin_var_overflow = _first_char(r.itin_var_overflow, ' '),
                svc_type          = _first_char(r.svc_type, ' '),
                operating_date    = pack_date(eff_d),
                num_legs          = UInt8(_safe_missing(r.num_legs, 0)),
                first_leg_seq     = UInt8(_safe_missing(r.first_leg_seq, 0)),
                last_leg_seq      = UInt8(_safe_missing(r.last_leg_seq, 0)),
                segment_org       = StationCode(_safe_string(r.segment_org)),
                segment_dst       = StationCode(_safe_string(r.segment_dst)),
                flown_distance    = Float32(_safe_missing(r.flown_distance, 0.0)),
                market_distance   = Float32(0),
                segment_circuity  = Float32(0),
                segment_pax_dep   = Int16(_safe_missing(r.segment_pax_dep, 0)),
                segment_pax_arr   = Int16(_safe_missing(r.segment_pax_arr, 0)),
                segment_ac_dep    = Int16(_safe_missing(r.segment_ac_dep, 0)),
                segment_ac_arr    = Int16(_safe_missing(r.segment_ac_arr, 0)),
            ),
        )
    end
    records
end

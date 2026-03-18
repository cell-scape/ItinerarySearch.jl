# src/store/duckdb_store.jl — DuckDB-backed store

using DuckDB
using DBInterface

"""
    mutable struct DuckDBStore <: AbstractStore

DuckDB-backed implementation of `AbstractStore`. Holds a single in-memory or
file-backed DuckDB database with all schedule, reference, and derived tables
created at construction time.

# Fields
- `db::DuckDB.DB`: the DuckDB database (in-memory or file-backed)
- `is_open::Bool`: whether the store is open; set to `false` after `close`

# Construction
- `DuckDBStore()` — in-memory database (`:memory:`)
- `DuckDBStore(path)` — file-backed database at `path`

# Examples
```julia
julia> store = DuckDBStore();
julia> stats = table_stats(store);
julia> close(store)
```
"""
mutable struct DuckDBStore <: AbstractStore
    db::DuckDB.DB
    is_open::Bool
end

"""
    `DuckDBStore(path::String=":memory:")`
---

# Description
- Create a DuckDB-backed store at `path` (default: in-memory)
- All tables are created via DDL at construction time
- Use `close(store)` when done to release resources

# Arguments
1. `path::String=":memory:"`: filesystem path or `":memory:"` for in-memory DB

# Returns
- `::DuckDBStore`: initialized store with all tables created
"""
function DuckDBStore(path::String=":memory:")
    db = DuckDB.DB(path)
    store = DuckDBStore(db, true)
    _create_tables!(store)
    store
end

"""
    `Base.close(store::DuckDBStore)`
---

# Description
- Close the DuckDB connection and mark the store as closed
- Safe to call multiple times (no-op if already closed)

# Arguments
1. `store::DuckDBStore`: the store to close
"""
function Base.close(store::DuckDBStore)
    if store.is_open
        DBInterface.close!(store.db)
        store.is_open = false
    end
end

# Internal helper: execute a SQL statement against the store's DB
function _exec(store::DuckDBStore, sql::String)
    DBInterface.execute(store.db, sql)
end

# Internal helper: create all tables
function _create_tables!(store::DuckDBStore)
    # ── Core schedule tables ──────────────────────────────────────────────────

    _exec(store, """
        CREATE TABLE IF NOT EXISTS legs (
            row_id              UBIGINT PRIMARY KEY,
            record_serial       UINTEGER,
            airline             VARCHAR(3),
            flt_no              SMALLINT,
            op_suffix           CHAR(1),
            itin_var            UTINYINT,
            itin_var_overflow   CHAR(1),
            leg_seq             UTINYINT,
            svc_type            CHAR(1),
            org                 CHAR(3),
            dst                 CHAR(3),
            pax_dep_mins        SMALLINT,
            pax_arr_mins        SMALLINT,
            ac_dep_mins         SMALLINT,
            ac_arr_mins         SMALLINT,
            dep_utc_offset      SMALLINT,
            arr_utc_offset      SMALLINT,
            dep_date_var        TINYINT,
            arr_date_var        TINYINT,
            dep_term            VARCHAR(2),
            arr_term            VARCHAR(2),
            eqp                 CHAR(3),
            body_type           CHAR(1),
            aircraft_owner      VARCHAR(3),
            eff_date            DATE,
            disc_date           DATE,
            frequency           UTINYINT,
            mct_dep             CHAR(1),
            mct_arr             CHAR(1),
            trc                 VARCHAR(11),
            trc_overflow        CHAR(1),
            prbd                VARCHAR(20),
            distance            FLOAT,
            wet_lease           BOOLEAN DEFAULT FALSE
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS dei (
            row_id              UBIGINT,
            dei_code            SMALLINT,
            board_point         CHAR(3),
            off_point           CHAR(3),
            data                VARCHAR(155),
            record_serial       UINTEGER
        )
    """)

    # ── Reference tables ──────────────────────────────────────────────────────

    _exec(store, """
        CREATE TABLE IF NOT EXISTS stations (
            code        CHAR(3) PRIMARY KEY,
            country     CHAR(2),
            state       VARCHAR(2),
            city        VARCHAR(40),
            region      CHAR(3),
            lat         DOUBLE,
            lng         DOUBLE,
            utc_offset  SMALLINT
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS mct (
            mct_id              INTEGER PRIMARY KEY,
            record_serial       UINTEGER,
            arr_stn             CHAR(3),
            dep_stn             CHAR(3),
            mct_status          CHAR(2),
            time_minutes        SMALLINT,
            arr_carrier         VARCHAR(3),
            arr_cs_ind          CHAR(1),
            arr_cs_op_carrier   VARCHAR(3),
            dep_carrier         VARCHAR(3),
            dep_cs_ind          CHAR(1),
            dep_cs_op_carrier   VARCHAR(3),
            arr_acft_type       CHAR(3),
            arr_acft_body       CHAR(1),
            dep_acft_type       CHAR(3),
            dep_acft_body       CHAR(1),
            arr_term            CHAR(2),
            dep_term            CHAR(2),
            prv_ctry            CHAR(2),
            prv_stn             CHAR(3),
            nxt_ctry            CHAR(2),
            nxt_stn             CHAR(3),
            arr_flt_rng_start   SMALLINT,
            arr_flt_rng_end     SMALLINT,
            dep_flt_rng_start   SMALLINT,
            dep_flt_rng_end     SMALLINT,
            prv_state           CHAR(2),
            nxt_state           CHAR(2),
            prv_rgn             CHAR(3),
            nxt_rgn             CHAR(3),
            eff_date            DATE,
            dis_date            DATE,
            suppress            BOOLEAN,
            supp_rgn            CHAR(3),
            supp_ctry           CHAR(2),
            supp_state          CHAR(2),
            submitting_carrier  CHAR(2),
            station_standard    BOOLEAN,
            specificity         UINTEGER DEFAULT 0
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS oa_control (
            carrier_cd          CHAR(2),
            exception_carrier   CHAR(1),
            irrops_window       SMALLINT,
            joint_venture       CHAR(1),
            carrier_group       VARCHAR(4),
            eligible_wet_leases VARCHAR(4)
        )
    """)

    # ── Derived / pipeline tables ─────────────────────────────────────────────

    _exec(store, """
        CREATE TABLE IF NOT EXISTS expanded_legs (
            row_id              UBIGINT,
            operating_date      DATE,
            airline             VARCHAR(3),
            flt_no              SMALLINT,
            op_suffix           CHAR(1),
            itin_var            UTINYINT,
            itin_var_overflow   CHAR(1),
            leg_seq             UTINYINT,
            svc_type            CHAR(1),
            org                 CHAR(3),
            dst                 CHAR(3),
            pax_dep_mins        SMALLINT,
            pax_arr_mins        SMALLINT,
            dep_utc_offset      SMALLINT,
            arr_utc_offset      SMALLINT,
            dep_date_var        TINYINT,
            arr_date_var        TINYINT,
            dep_term            VARCHAR(2),
            arr_term            VARCHAR(2),
            eqp                 CHAR(3),
            body_type           CHAR(1),
            aircraft_owner      VARCHAR(3),
            mct_dep             CHAR(1),
            mct_arr             CHAR(1),
            trc                 VARCHAR(11),
            prbd                VARCHAR(20),
            distance            FLOAT,
            wet_lease           BOOLEAN DEFAULT FALSE,
            segment_hash        UBIGINT
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS segments (
            segment_hash        UBIGINT PRIMARY KEY,
            airline             VARCHAR(3),
            flt_no              SMALLINT,
            op_suffix           CHAR(1),
            itin_var            UTINYINT,
            itin_var_overflow   CHAR(1),
            svc_type            CHAR(1),
            operating_date      DATE,
            num_legs            UTINYINT,
            first_leg_seq       UTINYINT,
            last_leg_seq        UTINYINT,
            segment_org         CHAR(3),
            segment_dst         CHAR(3),
            flown_distance      FLOAT,
            market_distance     FLOAT,
            segment_circuity    FLOAT,
            segment_pax_dep     SMALLINT,
            segment_pax_arr     SMALLINT,
            segment_ac_dep      SMALLINT,
            segment_ac_arr      SMALLINT
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS markets (
            org                 CHAR(3),
            dst                 CHAR(3),
            distance            FLOAT,
            PRIMARY KEY (org, dst)
        )
    """)

    # ── Simple reference tables ───────────────────────────────────────────────

    _exec(store, """
        CREATE TABLE IF NOT EXISTS classmap (
            cabin       CHAR(2),
            classes     VARCHAR(20)
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS serviceclass (
            sort_order  INT,
            class_code  VARCHAR(2),
            cabin_tier  INT
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS aircrafts (
            code        CHAR(3) PRIMARY KEY,
            body_type   CHAR(1),
            description VARCHAR(50)
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS seats (
            eqp     CHAR(3),
            cabin   CHAR(2),
            seats   INT
        )
    """)

    _exec(store, """
        CREATE TABLE IF NOT EXISTS regions (
            region      CHAR(3),
            airport     CHAR(3),
            metro_area  CHAR(3)
        )
    """)

    return store
end

"""
    `table_stats(store::DuckDBStore)`
---

# Description
- Return row counts for all core tables in the DuckDB store
- Returns a NamedTuple with fields for each table
- Tables that do not yet exist return 0 (safe to call before ingest)

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store

# Returns
- `::NamedTuple`: row counts with keys `legs`, `dei`, `stations`, `mct`,
  `expanded_legs`, `segments`, `markets`

# Examples
```julia
julia> store = DuckDBStore();
julia> stats = table_stats(store);
julia> stats.legs
0
```
"""
function table_stats(store::DuckDBStore)
    function _count(table::String)::Int
        try
            result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM $table")
            row = first(result)
            return Int(row.n)
        catch
            return 0
        end
    end
    (
        legs         = _count("legs"),
        dei          = _count("dei"),
        stations     = _count("stations"),
        mct          = _count("mct"),
        expanded_legs = _count("expanded_legs"),
        segments     = _count("segments"),
        markets      = _count("markets"),
    )
end

"""
    `post_ingest_sql!(store::DuckDBStore)::Nothing`
---

# Description
- Run the full post-ingest SQL pipeline after schedule data has been loaded
- Steps: EDF expansion, codeshare resolution view, segments table,
  spatial extension load, markets table (NDOD distances), segment circuity
  precomputation, and indexes

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store (must have legs and stations populated)

# Returns
- `::Nothing`
"""
function post_ingest_sql!(store::DuckDBStore)::Nothing
    @info "Post-ingest: expanding schedule (EDF)..."
    _expand_schedule!(store)

    @info "Post-ingest: creating codeshare view..."
    _create_codeshare_view!(store)

    @info "Post-ingest: building segments..."
    _build_segments!(store)

    @info "Post-ingest: computing market distances..."
    _build_markets!(store)

    @info "Post-ingest: computing segment circuity..."
    _compute_circuity!(store)

    @info "Post-ingest: injecting leg distances from markets..."
    _inject_leg_distances!(store)

    @info "Post-ingest: creating indexes..."
    _create_indexes!(store)

    @info "Post-ingest complete." stats=table_stats(store)
    nothing
end

function _expand_schedule!(store::DuckDBStore)
    _exec(store, "DROP TABLE IF EXISTS expanded_legs")
    _exec(store, """
    CREATE TABLE expanded_legs AS
    SELECT l.*, d.operating_date,
           EXTRACT(ISODOW FROM d.operating_date)::INT AS day_of_week
    FROM legs l
    CROSS JOIN LATERAL generate_series(l.eff_date, l.disc_date, INTERVAL 1 DAY) AS d(operating_date)
    WHERE l.frequency & (1 << ((EXTRACT(ISODOW FROM d.operating_date) - 1)::INT)) != 0
    """)
end

function _create_codeshare_view!(store::DuckDBStore)
    _exec(store, "DROP VIEW IF EXISTS legs_with_operating")
    _exec(store, """
    CREATE VIEW legs_with_operating AS
    SELECT l.*,
        TRIM(SUBSTRING(dei50.data, 1, 3)) AS codeshare_airline,
        CAST(NULLIF(TRIM(SUBSTRING(dei50.data, 4, 4)), '') AS SMALLINT) AS codeshare_flt_no,
        dei10.data AS dei_10,
        dei127.data AS dei_127
    FROM expanded_legs l
    LEFT JOIN dei dei50  ON dei50.row_id = l.row_id AND dei50.dei_code = 50
    LEFT JOIN dei dei10  ON dei10.row_id = l.row_id AND dei10.dei_code = 10
    LEFT JOIN dei dei127 ON dei127.row_id = l.row_id AND dei127.dei_code = 127
    """)
end

function _build_segments!(store::DuckDBStore)
    _exec(store, "DROP TABLE IF EXISTS segments")
    _exec(store, """
    CREATE TABLE segments AS
    SELECT
        hash(airline || CAST(flt_no AS VARCHAR) || op_suffix
             || CAST(itin_var AS VARCHAR) || itin_var_overflow
             || svc_type || CAST(operating_date AS VARCHAR)) AS segment_hash,
        airline, flt_no, op_suffix, itin_var, itin_var_overflow, svc_type,
        operating_date,
        FIRST(leg_seq ORDER BY leg_seq) AS first_leg_seq,
        LAST(leg_seq ORDER BY leg_seq) AS last_leg_seq,
        CAST(COUNT(*) AS INTEGER) AS num_legs,
        FIRST(org ORDER BY leg_seq) AS segment_org,
        LAST(dst ORDER BY leg_seq) AS segment_dst,
        COALESCE(SUM(distance), 0) AS flown_distance,
        FIRST(pax_dep_mins ORDER BY leg_seq) AS segment_pax_dep,
        LAST(pax_arr_mins ORDER BY leg_seq) AS segment_pax_arr,
        FIRST(ac_dep_mins ORDER BY leg_seq) AS segment_ac_dep,
        LAST(ac_arr_mins ORDER BY leg_seq) AS segment_ac_arr,
        STRING_AGG(trc, '|' ORDER BY leg_seq) AS trc_by_leg,
        LIST(org ORDER BY leg_seq) AS board_points,
        LIST(dst ORDER BY leg_seq) AS off_points
    FROM expanded_legs
    GROUP BY airline, flt_no, op_suffix, itin_var, itin_var_overflow, svc_type, operating_date
    """)
end

function _build_markets!(store::DuckDBStore)
    # Install spatial extension for ST_Distance_Sphere
    try
        _exec(store, "INSTALL spatial")
    catch
        # May already be installed
    end
    _exec(store, "LOAD spatial")

    _exec(store, "DROP TABLE IF EXISTS markets")
    _exec(store, """
    CREATE TABLE markets AS
    WITH active_markets AS (
        SELECT DISTINCT
            LEAST(segment_org, segment_dst) AS stn_a,
            GREATEST(segment_org, segment_dst) AS stn_b
        FROM segments

        UNION

        SELECT DISTINCT
            LEAST(a.org, b.dst) AS stn_a,
            GREATEST(a.org, b.dst) AS stn_b
        FROM expanded_legs a
        JOIN expanded_legs b
            ON  a.airline = b.airline
            AND a.flt_no = b.flt_no
            AND a.op_suffix = b.op_suffix
            AND a.itin_var = b.itin_var
            AND a.itin_var_overflow = b.itin_var_overflow
            AND a.svc_type = b.svc_type
            AND a.operating_date = b.operating_date
            AND a.leg_seq < b.leg_seq
        WHERE a.org != b.dst
    )
    SELECT
        m.stn_a,
        m.stn_b,
        m.stn_a || m.stn_b AS ndod,
        ST_Distance_Sphere(
            ST_Point(sa.lng, sa.lat),
            ST_Point(sb.lng, sb.lat)
        ) / 1609.344 AS distance_miles,
        ST_Distance_Sphere(
            ST_Point(sa.lng, sa.lat),
            ST_Point(sb.lng, sb.lat)
        ) / 1852.0 AS distance_nm
    FROM active_markets m
    JOIN stations sa ON sa.code = m.stn_a
    JOIN stations sb ON sb.code = m.stn_b
    """)
end

function _compute_circuity!(store::DuckDBStore)
    _exec(store, "ALTER TABLE segments ADD COLUMN IF NOT EXISTS market_distance FLOAT")
    _exec(store, "ALTER TABLE segments ADD COLUMN IF NOT EXISTS segment_circuity FLOAT")

    _exec(store, """
    UPDATE segments s
    SET market_distance = m.distance_miles,
        segment_circuity = s.flown_distance / NULLIF(m.distance_miles, 0)
    FROM markets m
    WHERE m.stn_a = LEAST(s.segment_org, s.segment_dst)
      AND m.stn_b = GREATEST(s.segment_org, s.segment_dst)
    """)
end

function _inject_leg_distances!(store::DuckDBStore)
    # Inject great-circle distance from markets table into legs
    _exec(store, """
    UPDATE legs l
    SET distance = m.distance_miles
    FROM markets m
    WHERE m.stn_a = LEAST(l.org, l.dst)
      AND m.stn_b = GREATEST(l.org, l.dst)
      AND l.distance = 0
    """)

    # Also update expanded_legs (derived from legs via EDF expansion)
    _exec(store, """
    UPDATE expanded_legs el
    SET distance = m.distance_miles
    FROM markets m
    WHERE m.stn_a = LEAST(el.org, el.dst)
      AND m.stn_b = GREATEST(el.org, el.dst)
      AND el.distance = 0
    """)

    r = _exec(store, "SELECT COUNT(*) as n FROM legs WHERE distance > 0")
    updated = first(r).n
    @info "Leg distances injected" updated
end

function _create_indexes!(store::DuckDBStore)
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_expanded_org_date ON expanded_legs (org, operating_date)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_expanded_dst_date ON expanded_legs (dst, operating_date)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_segments_hash ON segments (segment_hash)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_segments_org_date ON segments (segment_org, operating_date)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_segments_dst_date ON segments (segment_dst, operating_date)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_markets_ndod ON markets (ndod)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_markets_stn_a ON markets (stn_a)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_markets_stn_b ON markets (stn_b)")
    _exec(store, "CREATE INDEX IF NOT EXISTS idx_dei_row_id ON dei (row_id)")
end

# ── Query implementations ───────────────────────────────────────

"""
    `_safe_missing(val, default)`

Handle both `nothing` and `missing` (DuckDB NULLs) by returning `default`.
Julia's `something()` only handles `nothing`, not `missing`.
"""
_safe_missing(val, default) = (val === nothing || val === missing) ? default : val

"""
    `_first_char(val, default::Char)::Char`

Extract the first non-whitespace character from a DuckDB field value.
Returns `default` if the value is null, missing, or empty after stripping.
"""
function _first_char(val, default::Char)::Char
    (val === nothing || val === missing) && return default
    s = strip(String(val))
    isempty(s) ? default : s[1]
end

"""
    `_safe_string(val, default::String="")::String`

Convert a DuckDB field value to a trimmed String, returning `default` for null/missing.
"""
function _safe_string(val, default::String="")::String
    (val === nothing || val === missing) && return default
    strip(String(val))
end

"""
    `query_station(store::DuckDBStore, code::StationCode)::Union{StationRecord, Nothing}`
---

# Description
- Look up a station by its IATA code in the `stations` reference table

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `code::StationCode`: the IATA airport code

# Returns
- `::Union{StationRecord, Nothing}`: station record, or `nothing` if not found
"""
function query_station(store::DuckDBStore, code::StationCode)::Union{StationRecord, Nothing}
    result = DBInterface.execute(store.db,
        "SELECT * FROM stations WHERE code = ?", [String(code)])
    rows = collect(result)
    isempty(rows) && return nothing
    r = rows[1]
    StationRecord(
        code       = StationCode(_safe_string(r.code)),
        country    = InlineString3(_safe_string(r.country)),
        state      = InlineString3(_safe_string(r.state)),
        city       = InlineString31(_safe_string(r.city)),
        region     = InlineString3(_safe_string(r.region)),
        lat        = Float64(_safe_missing(r.lat, 0.0)),
        lng        = Float64(_safe_missing(r.lng, 0.0)),
        utc_offset = Int16(_safe_missing(r.utc_offset, 0)),
    )
end

"""
    `_row_to_leg(r)::LegRecord`

Convert a DuckDB row from `legs_with_operating` to a `LegRecord`.
Handles `missing` (DuckDB NULL) for all optional fields.

Notes:
- `operating_date` may arrive as `DateTime` from DuckDB; `Date()` conversion handles both.
- `segment_hash` is not in `legs_with_operating` (it comes from legs which has no segment_hash
  column); set to 0 — populated by joining segments table in Subsystem 2.
- `eff_date`/`disc_date` arrive as `Date` objects from DuckDB.
"""
function _row_to_leg(r)::LegRecord
    # operating_date may be a DateTime (DuckDB DATE → Julia DateTime) or Date
    op_date_val = _safe_missing(r.operating_date, Date(1900, 1, 1))
    op_date = op_date_val isa DateTime ? Date(op_date_val) : Date(op_date_val)

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
        operating_date       = pack_date(op_date),
        day_of_week          = UInt8(_safe_missing(r.day_of_week, 0)),
        eff_date             = pack_date(eff_d),
        disc_date            = pack_date(disc_d),
        frequency            = UInt8(_safe_missing(r.frequency, 0)),
        mct_status_dep       = _first_char(r.mct_dep, ' '),
        mct_status_arr       = _first_char(r.mct_arr, ' '),
        trc                  = InlineString15(_safe_string(r.trc)),
        trc_overflow         = _first_char(r.trc_overflow, ' '),
        record_serial        = UInt32(_safe_missing(r.record_serial, 0)),
        row_number           = UInt64(_safe_missing(r.row_id, 0)),
        # segment_hash not in legs_with_operating — populated via segments join in Subsystem 2
        segment_hash         = UInt64(0),
        distance             = Float32(_safe_missing(r.distance, 0.0)),
        codeshare_airline    = AirlineCode(_safe_string(hasproperty(r, :codeshare_airline) ? r.codeshare_airline : nothing)),
        codeshare_flt_no     = Int16(_safe_missing(hasproperty(r, :codeshare_flt_no) ? r.codeshare_flt_no : nothing, 0)),
        dei_10               = InlineString31(_safe_string(hasproperty(r, :dei_10) ? r.dei_10 : nothing)),
        wet_lease            = Bool(_safe_missing(r.wet_lease, false)),
        dei_127              = InlineString31(_safe_string(hasproperty(r, :dei_127) ? r.dei_127 : nothing)),
        prbd                 = InlineString31(_safe_string(r.prbd)),
    )
end

"""
    `get_departures(store::DuckDBStore, station::StationCode, date::Date)::Vector{LegRecord}`
---

# Description
- Return all legs departing from `station` on `date` from the `legs_with_operating` view

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `station::StationCode`: departure station code
3. `date::Date`: operating date

# Returns
- `::Vector{LegRecord}`: all departing legs (may be empty)
"""
function get_departures(store::DuckDBStore, station::StationCode, date::Date)::Vector{LegRecord}
    result = DBInterface.execute(store.db,
        "SELECT * FROM legs_with_operating WHERE org = ? AND operating_date = ?",
        [String(station), date])
    [_row_to_leg(r) for r in result]
end

"""
    `get_arrivals(store::DuckDBStore, station::StationCode, date::Date)::Vector{LegRecord}`
---

# Description
- Return all legs arriving at `station` on `date` from the `legs_with_operating` view

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `station::StationCode`: arrival station code
3. `date::Date`: operating date

# Returns
- `::Vector{LegRecord}`: all arriving legs (may be empty)
"""
function get_arrivals(store::DuckDBStore, station::StationCode, date::Date)::Vector{LegRecord}
    result = DBInterface.execute(store.db,
        "SELECT * FROM legs_with_operating WHERE dst = ? AND operating_date = ?",
        [String(station), date])
    [_row_to_leg(r) for r in result]
end

"""
    `query_legs(store::DuckDBStore, origin::StationCode, destination::StationCode, date::Date)::Vector{LegRecord}`
---

# Description
- Return all legs between an O-D pair on `date` from the `legs_with_operating` view

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `origin::StationCode`: departure station code
3. `destination::StationCode`: arrival station code
4. `date::Date`: operating date

# Returns
- `::Vector{LegRecord}`: matching leg records (may be empty)
"""
function query_legs(store::DuckDBStore, origin::StationCode, destination::StationCode, date::Date)::Vector{LegRecord}
    result = DBInterface.execute(store.db,
        "SELECT * FROM legs_with_operating WHERE org = ? AND dst = ? AND operating_date = ?",
        [String(origin), String(destination), date])
    [_row_to_leg(r) for r in result]
end

"""
    `query_market_distance(store::DuckDBStore, stn_a::StationCode, stn_b::StationCode)::Union{Float64, Nothing}`
---

# Description
- Return the NDOD market distance between two stations in miles
- Uses the normalized NDOD key (alphabetically ordered) so order of arguments does not matter

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `stn_a::StationCode`: first station code
3. `stn_b::StationCode`: second station code

# Returns
- `::Union{Float64, Nothing}`: great-circle distance in miles, or `nothing` if market unknown
"""
function query_market_distance(store::DuckDBStore, stn_a::StationCode, stn_b::StationCode)::Union{Float64, Nothing}
    a = min(String(stn_a), String(stn_b))
    b = max(String(stn_a), String(stn_b))
    ndod = a * b
    result = DBInterface.execute(store.db,
        "SELECT distance_miles FROM markets WHERE ndod = ?", [ndod])
    rows = collect(result)
    isempty(rows) && return nothing
    Float64(rows[1].distance_miles)
end

"""
    `query_segment(store::DuckDBStore, segment_hash::UInt64)::Union{SegmentRecord, Nothing}`
---

# Description
- Return aggregate segment data for a segment identified by its hash key

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `segment_hash::UInt64`: the precomputed segment identity hash

# Returns
- `::Union{SegmentRecord, Nothing}`: segment record, or `nothing` if not found
"""
function query_segment(store::DuckDBStore, segment_hash::UInt64)::Union{SegmentRecord, Nothing}
    result = DBInterface.execute(store.db,
        "SELECT * FROM segments WHERE segment_hash = ?",
        [segment_hash])
    rows = collect(result)
    isempty(rows) && return nothing
    r = rows[1]
    # operating_date may arrive as DateTime from DuckDB DATE column
    op_dt = _safe_missing(r.operating_date, Date(1900, 1, 1))
    op_date = op_dt isa DateTime ? Date(op_dt) : Date(op_dt)
    SegmentRecord(
        segment_hash      = UInt64(r.segment_hash),
        airline           = AirlineCode(_safe_string(r.airline)),
        flt_no            = Int16(_safe_missing(r.flt_no, 0)),
        op_suffix         = _first_char(r.op_suffix, ' '),
        itin_var          = UInt8(_safe_missing(r.itin_var, 0)),
        itin_var_overflow = _first_char(r.itin_var_overflow, ' '),
        svc_type          = _first_char(r.svc_type, ' '),
        operating_date    = pack_date(op_date),
        num_legs          = UInt8(_safe_missing(r.num_legs, 0)),
        first_leg_seq     = UInt8(_safe_missing(r.first_leg_seq, 0)),
        last_leg_seq      = UInt8(_safe_missing(r.last_leg_seq, 0)),
        segment_org       = StationCode(_safe_string(r.segment_org)),
        segment_dst       = StationCode(_safe_string(r.segment_dst)),
        flown_distance    = Float32(_safe_missing(r.flown_distance, 0.0)),
        market_distance   = Float32(_safe_missing(r.market_distance, 0.0)),
        segment_circuity  = Float32(_safe_missing(r.segment_circuity, 0.0)),
        segment_pax_dep   = Int16(_safe_missing(r.segment_pax_dep, 0)),
        segment_pax_arr   = Int16(_safe_missing(r.segment_pax_arr, 0)),
        segment_ac_dep    = Int16(_safe_missing(r.segment_ac_dep, 0)),
        segment_ac_arr    = Int16(_safe_missing(r.segment_ac_arr, 0)),
    )
end

"""
    `query_segment_stops(store::DuckDBStore, segment_hash::UInt64)::Tuple{Vector{StationCode}, Vector{StationCode}}`
---

# Description
- Return the board points and off points arrays for a segment
- For a single-leg segment, each list has one entry

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `segment_hash::UInt64`: the precomputed segment identity hash

# Returns
- `::Tuple{Vector{StationCode}, Vector{StationCode}}`: (board_points, off_points)
"""
function query_segment_stops(store::DuckDBStore, segment_hash::UInt64)::Tuple{Vector{StationCode}, Vector{StationCode}}
    result = DBInterface.execute(store.db,
        "SELECT board_points, off_points FROM segments WHERE segment_hash = ?",
        [segment_hash])
    rows = collect(result)
    isempty(rows) && return (StationCode[], StationCode[])
    r = rows[1]
    # board_points and off_points are DuckDB LIST columns — Julia receives them as
    # Vector{Union{Missing, String}}; filter out any missing entries
    bp = [StationCode(_safe_string(s)) for s in r.board_points if s !== missing]
    op_pts = [StationCode(_safe_string(s)) for s in r.off_points if s !== missing]
    (bp, op_pts)
end

"""
    `query_mct(store::DuckDBStore, arr_carrier, dep_carrier, station, status; kwargs...)::MCTResult`
---

# Description
- Look up MCT for a connection at `station` with the given traffic status
- Tries carrier-specific exception records first, then falls back to global default
- Full SSIM8 hierarchical lookup deferred to Subsystem 2

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `arr_carrier::AirlineCode`: arriving flight carrier
3. `dep_carrier::AirlineCode`: departing flight carrier
4. `station::StationCode`: connecting station
5. `status::MCTStatus`: connection traffic type (MCT_DD, MCT_DI, MCT_ID, MCT_II)

# Returns
- `::MCTResult`: MCT value, source, specificity, and suppression flag
"""
function query_mct(store::DuckDBStore, arr_carrier::AirlineCode, dep_carrier::AirlineCode,
                   station::StationCode, status::MCTStatus; kwargs...)::MCTResult
    status_str = status == MCT_DD ? "DD" :
                 status == MCT_DI ? "DI" :
                 status == MCT_ID ? "ID" : "II"

    # Try carrier-specific exception first, then station standard.
    # NOTE: dep_carrier is accepted for API compatibility but not used in queries
    # until Subsystem 2 implements full SSIM8 hierarchical MCT lookup.
    result = DBInterface.execute(store.db, """
    SELECT time_minutes, suppress, arr_carrier FROM mct
    WHERE arr_stn = ? AND dep_stn = ? AND mct_status = ?
      AND (arr_carrier = ? OR arr_carrier = '')
      AND suppress = false
    ORDER BY CASE WHEN arr_carrier != '' THEN 0 ELSE 1 END
    LIMIT 1
    """, [String(station), String(station), status_str, String(arr_carrier)])

    rows = collect(result)
    if !isempty(rows)
        r = rows[1]
        carrier_val = _safe_string(r.arr_carrier)
        source = isempty(strip(carrier_val)) ? SOURCE_STATION_STANDARD : SOURCE_EXCEPTION
        return MCTResult(
            time            = Int16(_safe_missing(r.time_minutes, 0)),
            queried_status  = status,
            matched_status  = status,
            suppressed      = Bool(_safe_missing(r.suppress, false)),
            source          = source,
            specificity     = UInt32(isempty(strip(carrier_val)) ? 50 : 100),
        )
    end

    # Fall back to global default
    MCTResult(
        time           = MCT_DEFAULTS[status],
        queried_status = status,
        matched_status = status,
        suppressed     = false,
        source         = SOURCE_GLOBAL_DEFAULT,
        specificity    = UInt32(0),
    )
end

# ── load_schedule! ───────────────────────────────────────────────

"""
    `load_schedule!(store::DuckDBStore, config::SearchConfig)::Nothing`
---

# Description
- Load all schedule and reference data from files described in `config` into the store
- Ingests SSIM, MCT, and reference files, then runs the full post-ingest SQL pipeline
- MCT is pre-filtered using the station and carrier sets from the ingested legs

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store (tables must already exist)
2. `config::SearchConfig`: file paths and search parameters

# Returns
- `::Nothing`
"""
function load_schedule!(store::DuckDBStore, config::SearchConfig)::Nothing
    @info "Loading schedule..." ssim=config.ssim_path mct=config.mct_path

    # Ingest SSIM
    ingest_ssim!(store, config.ssim_path)

    # Build schedule filters for MCT pre-filtering
    stn_filter, car_filter = _build_schedule_filters(store)

    # Ingest MCT with schedule-based filtering
    ingest_mct!(store, config.mct_path;
                station_filter=stn_filter, carrier_filter=car_filter)

    # Load reference tables (skip missing files gracefully)
    isfile(config.airports_path)   && load_airports!(store, config.airports_path)
    isfile(config.regions_path)    && load_regions!(store, config.regions_path)
    isfile(config.aircrafts_path)  && load_aircrafts!(store, config.aircrafts_path)
    isfile(config.oa_control_path) && load_oa_control!(store, config.oa_control_path)

    # Post-ingest processing: EDF expansion, codeshare view, segments, markets, circuity, indexes
    post_ingest_sql!(store)

    @info "Schedule loaded." stats=table_stats(store)
    nothing
end

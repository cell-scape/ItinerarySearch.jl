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

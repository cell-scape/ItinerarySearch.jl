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

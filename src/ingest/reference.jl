# src/ingest/reference.jl — Reference table loaders using FixedWidthParsers native formats

using DuckDB
using DBInterface
using FixedWidthParsers

# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

# Load schemas directly from CSV files in FixedWidthParsers, bypassing the
# precompiled constants. This ensures we always get the latest schema definitions.
const _FWP_EXAMPLES = joinpath(dirname(dirname(pathof(FixedWidthParsers))), "examples")
const _AIRPORT_SCHEMA  = load_schema(joinpath(_FWP_EXAMPLES, "airport.csv"))
const _AIRCRAFT_SCHEMA = load_schema(joinpath(_FWP_EXAMPLES, "aircraft.csv"))
const _REGIONAL_SCHEMA = load_schema(joinpath(_FWP_EXAMPLES, "regional.csv"))
const _SEATS_SCHEMA    = load_schema(joinpath(_FWP_EXAMPLES, "seats.csv"))

# ---------------------------------------------------------------------------
# UTC offset parser
# ---------------------------------------------------------------------------

# Parse "+HHMM" / "-HHMM" UTC offset string into integer minutes.
# Returns 0 for any unrecognisable value.
function _parse_utc_minutes(s::AbstractString)::Int16
    s = strip(s)
    length(s) < 5 && return Int16(0)
    sign = s[1] == '-' ? -1 : 1
    v = tryparse(Int, @view s[2:end])
    v === nothing && return Int16(0)
    Int16(sign * (div(v, 100) * 60 + rem(v, 100)))
end

# ---------------------------------------------------------------------------
# DMS coordinate parser
# ---------------------------------------------------------------------------

# Parse DMS strings returned by the rebuilt schema, e.g. "42", "31", "00", "N".
# Returns a signed decimal degree value.
function _dms_to_decimal(deg_s, min_s, sec_s, hem_s)::Float64
    d = something(tryparse(Int, strip(string(deg_s))), 0)
    m = something(tryparse(Int, strip(string(min_s))), 0)
    s = something(tryparse(Int, strip(string(sec_s))), 0)
    dec = Float64(d) + Float64(m) / 60.0 + Float64(s) / 3600.0
    hem = strip(string(hem_s))
    (hem == "S" || hem == "W") && (dec = -dec)
    dec
end

# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

"""
    `load_airports!(store::DuckDBStore, path::String)::Nothing`
---

# Description
- Load airport/station reference data from OAG mdstua.txt (fixed-width) into `stations` table
- Parses via a corrected AIRPORT_SCHEMA with all Int fields converted to String
  so blank DST-window fields do not raise parse errors
- UTC offset converted from "+HHMM"/"-HHMM" string to integer minutes
- Latitude/longitude converted from DMS fields to decimal degrees
- Records with zero lat/lng (no coordinate data) are skipped
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (stations table must already exist)
2. `path::String`: path to OAG mdstua.txt fixed-width file (may be compressed)

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_airports!(store, "data/input/mdstua.txt");
julia> table_stats(store).stations
3000
```
"""
function load_airports!(store::DuckDBStore, path::String)::Nothing
    stmt = DBInterface.prepare(
        store.db,
        """
    INSERT OR REPLACE INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?)
"""
    )
    loaded = 0
    iter = eachrecord(open_maybe_compressed(path), _AIRPORT_SCHEMA)
    try
        for rec in iter
            code = strip(string(rec.airport))
            # Require exactly 3 ASCII letters for a valid IATA airport code
            (length(code) != 3 || any(!isletter, code)) && continue

            country = strip(string(rec.country))
            state = strip(string(rec.state))
            city = strip(string(rec.metro_area))
            region = strip(string(rec.metro_area))

            utc_minutes = _parse_utc_minutes(string(rec.utc_var))

            lat = _dms_to_decimal(rec.latitude_degrees, rec.latitude_minutes,
                rec.latitude_seconds, rec.latitude_hemisphere)
            lng = _dms_to_decimal(rec.longitude_degrees, rec.longitude_minutes,
                rec.longitude_seconds, rec.longitude_hemisphere)

            # Skip records with no coordinate data
            (lat == 0.0 && lng == 0.0) && continue

            # Column order matches stations DDL: code, country, state, city, region, lat, lng, utc_offset
            DBInterface.execute(stmt, [code, country, state, city, region, lat, lng, utc_minutes])
            loaded += 1
        end
    finally
        close(iter)
        DBInterface.close!(stmt)
    end
    @info "Loaded airports" count = loaded
    nothing
end

"""
    `load_regions!(store::DuckDBStore, path::String)::Nothing`
---

# Description
- Load region-to-airport mapping from REGIMFILUA.DAT (fixed-width) into `regions` table
- Uses REGIONAL_SCHEMA via `eachrecord` (9-byte records match the file exactly)
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (regions table must already exist)
2. `path::String`: path to REGIMFILUA.DAT fixed-width file (may be compressed)

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_regions!(store, "data/input/REGIMFILUA.DAT");
julia> first(DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM regions")).n
13107
```
"""
function load_regions!(store::DuckDBStore, path::String)::Nothing
    stmt = DBInterface.prepare(store.db, "INSERT INTO regions VALUES (?, ?, ?)")
    loaded = 0
    iter = eachrecord(open_maybe_compressed(path), _REGIONAL_SCHEMA)
    try
        for rec in iter
            region = strip(string(rec.region))
            airport = strip(string(rec.airport))
            city = strip(string(rec.city))
            (isempty(region) || isempty(airport)) && continue
            DBInterface.execute(stmt, [region, airport, city])
            loaded += 1
        end
    finally
        close(iter)
        DBInterface.close!(stmt)
    end
    @info "Loaded regions" count = loaded
    nothing
end

"""
    `load_oa_control!(store::DuckDBStore, path::String)::Nothing`
---

# Description
- Load OA (Other Airline) control table from CSV file into `oa_control` table
- First line is header row (skipped)
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (oa_control table must already exist)
2. `path::String`: path to CSV file (may be compressed)

# Format
CSV with header: carrier_cd, exception_carrier, irrops_window, joint_venture, carrier_group, eligible_wet_leases

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_oa_control!(store, "data/input/oa_control_table.csv");
julia> first(DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM oa_control")).n
50
```
"""
function load_oa_control!(store::DuckDBStore, path::String)::Nothing
    io = open_maybe_compressed(path)
    stmt = DBInterface.prepare(store.db, "INSERT INTO oa_control VALUES (?, ?, ?, ?, ?, ?)")
    header = true
    try
        for line in eachline(io)
            if header
                header = false
                continue
            end
            isempty(strip(line)) && continue
            parts = split(line, ',')
            length(parts) < 6 && continue

            carrier = strip(String(parts[1]))
            exc = strip(String(parts[2]))
            irrops = something(tryparse(Int16, strip(String(parts[3]))), Int16(0))
            jv = strip(String(parts[4]))
            group = strip(String(parts[5]))
            wet = strip(String(parts[6]))

            DBInterface.execute(stmt, [carrier, exc, irrops, jv, group, wet])
        end
    finally
        DBInterface.close!(stmt)
        close(io)
    end
    nothing
end

"""
    `load_aircrafts!(store::DuckDBStore, path::String)::Nothing`
---

# Description
- Load aircraft reference data from aircraft.txt (fixed-width) into `aircrafts` table
- Parses via a corrected AIRCRAFT_SCHEMA with all Int fields converted to String
  so blank range/speed fields do not raise parse errors
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (aircrafts table must already exist)
2. `path::String`: path to aircraft.txt fixed-width file (may be compressed)

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_aircrafts!(store, "data/input/aircraft.txt");
julia> first(DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM aircrafts")).n
514
```
"""
function load_aircrafts!(store::DuckDBStore, path::String)::Nothing
    stmt = DBInterface.prepare(store.db, "INSERT OR REPLACE INTO aircrafts VALUES (?, ?, ?)")
    loaded = 0
    iter = eachrecord(open_maybe_compressed(path), _AIRCRAFT_SCHEMA)
    try
        for rec in iter
            code = strip(string(rec.equip))
            isempty(code) && continue
            body = strip(string(rec.bodytype))
            desc = strip(string(rec.description))
            DBInterface.execute(stmt, [code, body, desc])
            loaded += 1
        end
    finally
        close(iter)
        DBInterface.close!(stmt)
    end
    @info "Loaded aircrafts" count = loaded
    nothing
end

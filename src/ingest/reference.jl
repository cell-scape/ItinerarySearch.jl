# src/ingest/reference.jl — Reference table loaders (CSV/delimited, not fixed-width)

using DuckDB
using DBInterface

"""
    `load_airports!(store::DuckDBStore, path::String)::Nothing`
---

# Description
- Load airport/station reference data from tab-delimited file into `stations` table
- Uses prepared statements to safely handle city names with apostrophes (e.g. "O'Hare")
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (stations table must already exist)
2. `path::String`: path to tab-delimited airports file (may be compressed)

# Format
Tab-delimited columns: code, name, city, country, state, lat, lng, utc_offset, region

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_airports!(store, "data/mdstua.txt");
julia> table_stats(store).stations
3000
```
"""
function load_airports!(store::DuckDBStore, path::String)::Nothing
    io = open_maybe_compressed(path)
    stmt = DBInterface.prepare(store.db, """
        INSERT OR REPLACE INTO stations VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """)
    try
        for line in eachline(io)
            isempty(strip(line)) && continue
            parts = split(line, '\t')
            length(parts) < 8 && continue

            code    = strip(String(parts[1]))
            city    = length(parts) >= 3 ? strip(String(parts[3])) : ""
            country = length(parts) >= 4 ? strip(String(parts[4])) : ""
            state   = length(parts) >= 5 ? strip(String(parts[5])) : ""
            lat     = something(tryparse(Float64, strip(String(parts[6]))), 0.0)
            lng     = something(tryparse(Float64, strip(String(parts[7]))), 0.0)
            utc     = something(tryparse(Int16, strip(String(parts[8]))), Int16(0))
            region  = length(parts) >= 9 ? strip(String(parts[9])) : ""

            # Column order: code, country, state, city, region, lat, lng, utc_offset
            DBInterface.execute(stmt, [code, country, state, city, region, lat, lng, utc])
        end
    finally
        DBInterface.close!(stmt)
        close(io)
    end
    nothing
end

"""
    `load_regions!(store::DuckDBStore, path::String)::Nothing`
---

# Description
- Load region-to-airport mapping from space-delimited file into `regions` table
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (regions table must already exist)
2. `path::String`: path to space-delimited regions file (may be compressed)

# Format
Space-delimited columns: region airport metro_area

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_regions!(store, "data/REGIMFILUA.DAT");
julia> first(DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM regions")).n
500
```
"""
function load_regions!(store::DuckDBStore, path::String)::Nothing
    io = open_maybe_compressed(path)
    stmt = DBInterface.prepare(store.db, "INSERT INTO regions VALUES (?, ?, ?)")
    try
        for line in eachline(io)
            isempty(strip(line)) && continue
            parts = split(strip(line))
            length(parts) < 3 && continue

            DBInterface.execute(stmt, [String(parts[1]), String(parts[2]), String(parts[3])])
        end
    finally
        DBInterface.close!(stmt)
        close(io)
    end
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
julia> load_oa_control!(store, "data/oa_control.csv");
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
            exc     = strip(String(parts[2]))
            irrops  = something(tryparse(Int16, strip(String(parts[3]))), Int16(0))
            jv      = strip(String(parts[4]))
            group   = strip(String(parts[5]))
            wet     = strip(String(parts[6]))

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
- Load aircraft reference data from tab-delimited file into `aircrafts` table
- Transparent decompression via `open_maybe_compressed`

# Arguments
1. `store::DuckDBStore`: initialized store (aircrafts table must already exist)
2. `path::String`: path to tab-delimited aircraft file (may be compressed)

# Format
Tab-delimited columns: code, body_type, description

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> load_aircrafts!(store, "data/aircraft.txt");
julia> first(DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM aircrafts")).n
100
```
"""
function load_aircrafts!(store::DuckDBStore, path::String)::Nothing
    io = open_maybe_compressed(path)
    stmt = DBInterface.prepare(store.db, "INSERT OR REPLACE INTO aircrafts VALUES (?, ?, ?)")
    try
        for line in eachline(io)
            isempty(strip(line)) && continue
            parts = split(line, '\t')
            length(parts) < 3 && continue

            code = strip(String(parts[1]))
            body = strip(String(parts[2]))
            desc = strip(String(parts[3]))

            DBInterface.execute(stmt, [code, body, desc])
        end
    finally
        DBInterface.close!(stmt)
        close(io)
    end
    nothing
end

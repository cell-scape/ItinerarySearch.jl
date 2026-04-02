# src/ingest/newssim.jl — CSV ingest for denormalized newssim schedule files

"""
    `function detect_delimiter(path::AbstractString)::Char`
---

# Description
- Auto-detect the field delimiter of a CSV file by reading the first line
- Uses `open_maybe_compressed` to handle .gz, .zst, .bz2, .xz files
- Checks for `|` first, then `\\t`, then defaults to `,`

# Arguments
1. `path::AbstractString`: path to the CSV file (may be compressed)

# Returns
- `::Char`: the detected delimiter character

# Examples
```julia
julia> detect_delimiter("data/demo/sample_newssim.csv.gz")
','
```
"""
function detect_delimiter(path::AbstractString)::Char
    io = open_maybe_compressed(String(path))
    try
        header = readline(io)
        occursin('|', header) && return '|'
        occursin('\t', header) && return '\t'
        return ','
    finally
        close(io)
    end
end

"""
    `function ingest_newssim!(store::DuckDBStore, path::AbstractString; delimiter::Union{Char,Nothing}=nothing)::Int`
---

# Description
- Load a denormalized newssim CSV file into DuckDB as the `newssim` table
- Drops existing `newssim` table if present
- Auto-detects delimiter if not provided
- Uses DuckDB's `read_csv_auto` which handles .gz files natively

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store
2. `path::AbstractString`: path to the CSV file (may be .gz compressed)

# Keyword Arguments
- `delimiter::Union{Char,Nothing}=nothing`: field delimiter; auto-detected if `nothing`

# Returns
- `::Int`: number of rows loaded

# Examples
```julia
julia> store = DuckDBStore();
julia> n = ingest_newssim!(store, "data/demo/sample_newssim.csv.gz");
julia> n > 100_000
true
```
"""
function ingest_newssim!(
    store::DuckDBStore,
    path::AbstractString;
    delimiter::Union{Char,Nothing} = nothing,
)::Int
    abspath_str = abspath(String(path))
    isfile(abspath_str) || error("File not found: $abspath_str")

    # Auto-detect delimiter if not provided
    delim = delimiter === nothing ? detect_delimiter(abspath_str) : delimiter

    # Drop existing table
    _exec(store, "DROP TABLE IF EXISTS newssim")

    # Escape the path for SQL (replace single quotes)
    escaped_path = replace(abspath_str, "'" => "''")

    # Escape the delimiter for SQL
    delim_str = delim == '\'' ? "''''" : string(delim)

    sql = """
        CREATE TABLE newssim AS
        SELECT * FROM read_csv_auto(
            '$(escaped_path)',
            delim = '$(delim_str)',
            header = true,
            ignore_errors = true
        )
    """
    _exec(store, sql)

    # Get row count
    result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM newssim")
    row = first(result)
    n = Int(row.n)

    @info "Ingested newssim CSV" path = abspath_str rows = n delimiter = delim
    return n
end

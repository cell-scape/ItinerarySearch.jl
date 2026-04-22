# src/ingest/circuity.jl — CSV loaders for the production circuity files:
#   cirOvrdDflt.dat  — HIGH,CIRCUITY             (distance-tiered defaults)
#   cirOvrd.dat      — ORG,DEST,ENTNM,CRTY       (market-level overrides)

"""
    `load_circuity_tiers(path::AbstractString)::Vector{CircuityTier}`
---

# Description
- Parse a CSV with columns `HIGH,CIRCUITY` into a `Vector{CircuityTier}`
- Rows are consumed in file order and must have strictly increasing `HIGH`
- An empty final `HIGH` value is interpreted as `Inf` (catchall tier); any
  other positive value is accepted as-is (e.g. `99999` is a common
  "effectively unlimited" upper bound)
- Validates via `_validate_circuity_tiers` before returning

# Arguments
1. `path::AbstractString`: path to the CSV file

# Returns
- `::Vector{CircuityTier}`: validated tier list in file order

# Throws
- `ArgumentError` if the file is missing required columns, has non-ascending
  thresholds, or contains non-positive factors
"""
function load_circuity_tiers(path::AbstractString)::Vector{CircuityTier}
    df = CSV.read(
        path,
        DataFrames.DataFrame;
        types=Dict(:HIGH => Union{Missing,Float64}, :CIRCUITY => Float64),
    )
    :HIGH in propertynames(df) ||
        throw(ArgumentError("$path: missing column `HIGH`"))
    :CIRCUITY in propertynames(df) ||
        throw(ArgumentError("$path: missing column `CIRCUITY`"))
    tiers = CircuityTier[]
    for row in eachrow(df)
        d = ismissing(row.HIGH) ? Inf : Float64(row.HIGH)
        push!(tiers, CircuityTier(d, Float64(row.CIRCUITY)))
    end
    _validate_circuity_tiers(tiers)
    return tiers
end

"""
    `load_circuity_overrides(path::AbstractString; specificity::UInt32 = UInt32(1000))::Vector{MarketOverride}`
---

# Description
- Parse a CSV with columns `ORG,DEST,ENTNM,CRTY` into market-level circuity overrides
- Each row produces one `MarketOverride`:
  - `origin = StationCode(ORG)`, `destination = StationCode(DEST)`
  - `carrier = WILDCARD_AIRLINE` (circuity ignores carrier)
  - `params = ParameterSet(circuity_tiers = [CircuityTier(Inf, CRTY)])`
  - `specificity` as supplied
- `ENTNM` is accepted (reserved for future entity grouping) but presence only —
  missing values are rejected. Production data currently uses `*`.

# Arguments
1. `path::AbstractString`: path to the CSV file

# Keyword Arguments
- `specificity::UInt32=1000`: specificity to stamp on each override; callers can
  lower this for layered override files

# Returns
- `::Vector{MarketOverride}`: overrides in file order (caller merges into
  `SearchConstraints.overrides` and sorts by descending specificity)

# Throws
- `ArgumentError` if columns are missing, ORG/DEST are empty, ENTNM is missing,
  or CRTY is non-positive
"""
function load_circuity_overrides(
    path::AbstractString;
    specificity::UInt32 = UInt32(1000),
)::Vector{MarketOverride}
    df = CSV.read(
        path,
        DataFrames.DataFrame;
        types = Dict(:ORG => String, :DEST => String,
                     :ENTNM => Union{Missing,String}, :CRTY => Float64),
    )
    for col in (:ORG, :DEST, :ENTNM, :CRTY)
        col in propertynames(df) ||
            throw(ArgumentError("$path: missing column `$col`"))
    end
    overrides = MarketOverride[]
    for row in eachrow(df)
        org = strip(String(row.ORG))
        dst = strip(String(row.DEST))
        isempty(org)         && throw(ArgumentError("$path: blank ORG"))
        isempty(dst)         && throw(ArgumentError("$path: blank DEST"))
        ismissing(row.ENTNM) && throw(ArgumentError("$path: missing ENTNM"))
        row.CRTY > 0 ||
            throw(ArgumentError("$path: CRTY must be positive; got $(row.CRTY)"))
        tiers = [CircuityTier(Inf, Float64(row.CRTY))]
        push!(
            overrides,
            MarketOverride(
                origin      = StationCode(org),
                destination = StationCode(dst),
                carrier     = WILDCARD_AIRLINE,
                params      = ParameterSet(circuity_tiers = tiers),
                specificity = specificity,
            ),
        )
    end
    return overrides
end

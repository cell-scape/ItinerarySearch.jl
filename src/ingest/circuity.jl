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

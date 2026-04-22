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

"""
    `apply_circuity_files!(constraints::SearchConstraints; defaults_path=nothing, overrides_path=nothing)::SearchConstraints`
---

# Description
- Compose CSV-loaded tiers and overrides into a `SearchConstraints`
- Returns a new `SearchConstraints` with:
  - `defaults.circuity_tiers` replaced by the parsed tiers (if `defaults_path`
    is supplied and the file exists)
  - `overrides` appended with the parsed market overrides (if `overrides_path`
    is supplied and the file exists)
  - `overrides` re-sorted by descending specificity to preserve the invariant
    required by `resolve_params` and `_resolve_circuity_params`
- The `!` in the name is aspirational — the function returns a new constraints
  object rather than mutating; Julia's `@kwdef` immutable `ParameterSet` forces
  reconstruction. The name preserves the conventional "this modifies
  constraints conceptually" signal.

# Arguments
1. `constraints::SearchConstraints`: starting constraints object

# Keyword Arguments
- `defaults_path::Union{String,Nothing}=nothing`: path to a tier CSV
  (cirOvrdDflt.dat format); if nothing or file missing, defaults stay unchanged
- `overrides_path::Union{String,Nothing}=nothing`: path to a market override CSV
  (cirOvrd.dat format); if nothing or file missing, overrides stay unchanged

# Returns
- `::SearchConstraints`: new constraints with CSV-loaded circuity config folded in
"""
function apply_circuity_files!(
    constraints::SearchConstraints;
    defaults_path::Union{String,Nothing} = nothing,
    overrides_path::Union{String,Nothing} = nothing,
)::SearchConstraints
    new_defaults = constraints.defaults
    if defaults_path !== nothing && isfile(defaults_path)
        tiers = load_circuity_tiers(defaults_path)
        # Reconstruct ParameterSet with new tiers, preserving all other fields.
        # @kwdef immutable structs cannot be mutated; we splat a Dict of existing
        # field values and override only circuity_tiers.
        kws = Dict(k => getfield(constraints.defaults, k)
                   for k in fieldnames(ParameterSet) if k != :circuity_tiers)
        new_defaults = ParameterSet(; kws..., circuity_tiers = tiers)
    end

    new_overrides = copy(constraints.overrides)
    if overrides_path !== nothing && isfile(overrides_path)
        append!(new_overrides, load_circuity_overrides(overrides_path))
        sort!(new_overrides, by = o -> -Int64(o.specificity))
    end

    return SearchConstraints(
        defaults         = new_defaults,
        overrides        = new_overrides,
        closed_stations  = constraints.closed_stations,
        closed_markets   = constraints.closed_markets,
        delays           = constraints.delays,
        flight_delays    = constraints.flight_delays,
    )
end

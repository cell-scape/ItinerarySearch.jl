# src/types/constraints.jl — Constraint and parameter types for connection building and search

"""
    struct CircuityTier

Distance-tiered circuity factor. A `Vector{CircuityTier}` is evaluated in order;
the first tier whose `max_distance` is ≥ the query distance supplies the factor.
The final tier's `factor` is the fallback when the distance exceeds every
threshold.

# Fields
- `max_distance::Float64` — inclusive upper bound in miles (`Inf` = catchall)
- `factor::Float64` — maximum allowed `route_distance / great_circle_distance`
"""
struct CircuityTier
    max_distance::Float64
    factor::Float64
end

"""
    const DEFAULT_CIRCUITY_TIERS::Vector{CircuityTier}

Industry-standard tier values used when no tiered defaults are supplied via
JSON or CSV. Stable over time.
"""
const DEFAULT_CIRCUITY_TIERS = CircuityTier[
    CircuityTier(250.0, 2.4),
    CircuityTier(800.0, 1.9),
    CircuityTier(2000.0, 1.5),
    CircuityTier(Inf, 1.3),
]

"""
    `function _validate_circuity_tiers(tiers::Vector{CircuityTier})`
---

# Description
- Validates a `Vector{CircuityTier}` for use in connection/itinerary rules
- Called once at load time (from JSON and CSV loaders); runtime lookup assumes
  invariants already hold

# Arguments
1. `tiers::Vector{CircuityTier}`: tier list to validate

# Returns
- `nothing` on success

# Throws
- `ArgumentError` if `tiers` is empty, has non-ascending `max_distance`, or
  contains a non-positive `factor`

# Notes
- `NaN` inputs are undefined behaviour; callers must pass finite values or `Inf`.
"""
function _validate_circuity_tiers(tiers::Vector{CircuityTier})
    isempty(tiers) && throw(ArgumentError("circuity_tiers must not be empty"))
    for i in 2:length(tiers)
        tiers[i].max_distance > tiers[i - 1].max_distance ||
            throw(
                ArgumentError(
                    "tier thresholds must be strictly ascending; got $(tiers[i-1].max_distance) then $(tiers[i].max_distance)",
                ),
            )
    end
    for t in tiers
        t.factor > 0 ||
            throw(ArgumentError("tier factors must be positive; got $(t.factor)"))
    end
    return nothing
end

"""
    `_circuity_factor_at(tiers::Vector{CircuityTier}, distance::Float64)::Float64`
---

# Description
- Return the circuity factor for `distance` from `tiers`
- Tiers must be pre-validated (ascending `max_distance`)
- Falls back to the last tier's factor when `distance` exceeds every threshold

# Arguments
1. `tiers::Vector{CircuityTier}`: tier list (pre-validated, ascending)
2. `distance::Float64`: route or leg distance in miles

# Returns
- `::Float64`: the applicable circuity factor
"""
@inline function _circuity_factor_at(tiers::Vector{CircuityTier}, distance::Float64)::Float64
    for t in tiers
        distance <= t.max_distance && return t.factor
    end
    return last(tiers).factor
end

"""
    struct ParameterSet
---

# Description
- All tunable parameters for connection and itinerary validation
- Sentinel values mean "no override": `NO_MINUTES` for times, empty `Set` for
  set-valued filters (empty = allow all, not deny all)
- Used as the default parameter set in `SearchConstraints` and as the payload in
  `MarketOverride`; construct with `@kwdef` keyword syntax

# Fields
## Connection-level
- `min_mct_override::Minutes` — minimum MCT to enforce; `NO_MINUTES` means use SSIM8 MCT table
- `max_mct_override::Minutes` — maximum connection time allowed (minutes)
- `min_connection_time::Minutes` — minimum connection time; `NO_MINUTES` means no minimum enforced
- `max_connection_time::Minutes` — maximum connection time (minutes)
- `circuity_tiers::Vector{CircuityTier}` — distance-tiered circuity factors;
  default is `DEFAULT_CIRCUITY_TIERS`. See `_effective_circuity_factor`.
- `domestic_circuity_extra_miles::Float64` — flat mileage tolerance for domestic legs
- `international_circuity_extra_miles::Float64` — flat mileage tolerance for international legs
- `valid_codeshare_partners::Set{Tuple{AirlineCode,AirlineCode}}` — allowed marketing/operating pairs; empty = allow all
- `valid_jv_groups::Set{InlineString7}` — allowed joint-venture group codes; empty = allow all
- `valid_wet_leases::Set{AirlineCode}` — allowed wet-lease operating carriers; empty = allow all

## Leg-level filters
- `min_leg_distance::Distance` — minimum leg distance (miles)
- `max_leg_distance::Distance` — maximum leg distance (miles); `Inf32` = unlimited
- `allow_service_types::Set{Char}` — permitted SSIM service-type codes; empty = allow all
- `deny_service_types::Set{Char}` — rejected SSIM service-type codes; empty = deny none
- `allow_aircraft_types::Set{InlineString7}` — permitted IATA aircraft type codes; empty = allow all
- `deny_aircraft_types::Set{InlineString7}` — rejected IATA aircraft type codes; empty = deny none
- `allow_body_types::Set{Char}` — permitted aircraft body-type codes; empty = allow all
- `deny_body_types::Set{Char}` — rejected aircraft body-type codes; empty = deny none

## Connection-level categorical filters
- `allow_stations::Set{StationCode}` — permitted connect-point stations; empty = allow all
- `deny_stations::Set{StationCode}` — rejected connect-point stations; empty = deny none
- `allow_countries::Set{InlineString3}` — permitted connect-point ISO-2 country codes; empty = allow all
- `deny_countries::Set{InlineString3}` — rejected connect-point ISO-2 country codes; empty = deny none
- `allow_regions::Set{InlineString3}` — permitted connect-point IATA region codes; empty = allow all
- `deny_regions::Set{InlineString3}` — rejected connect-point IATA region codes; empty = deny none
- `allow_states::Set{InlineString3}` — permitted connect-point state/province codes; empty = allow all
- `deny_states::Set{InlineString3}` — rejected connect-point state/province codes; empty = deny none

## Itinerary-level
- `min_stops::Int16` — minimum number of intermediate stops (0 = nonstop allowed)
- `max_stops::Int16` — maximum number of intermediate stops
- `min_elapsed::Int32` — minimum total elapsed time for the itinerary (minutes)
- `max_elapsed::Int32` — maximum total elapsed time for the itinerary (minutes)
- `min_flight_time::Int32` — minimum total airborne time across all legs (minutes)
- `max_flight_time::Int32` — maximum total airborne time across all legs (minutes)
- `min_layover_time::Int32` — minimum total layover time across all connections (minutes)
- `max_layover_time::Int32` — maximum total layover time across all connections (minutes)
- `min_total_distance::Distance` — minimum total flown distance (miles)
- `max_total_distance::Distance` — maximum total flown distance (miles); `Inf32` = unlimited
- `min_circuity::Float64` — global floor on actual/market distance ratio (rejects too-direct itineraries when > 0)
- `max_circuity::Float64` — global ceiling on the effective factor; `Inf` = no ceiling
- `max_results::Int32` — stop search after this many results per O-D; `0` = unlimited

## Itinerary-level carrier filters
- `allow_carriers::Set{AirlineCode}` — permitted marketing carriers anywhere in itinerary; empty = allow all
- `deny_carriers::Set{AirlineCode}` — rejected marketing carriers anywhere in itinerary; empty = deny none
- `allow_operating_carriers::Set{AirlineCode}` — permitted operating carriers; empty = allow all
- `deny_operating_carriers::Set{AirlineCode}` — rejected operating carriers; empty = deny none
"""
@kwdef struct ParameterSet
    # ── Connection-level ─────────────────────────────────────────────────
    min_mct_override::Minutes = NO_MINUTES          # NO_MINUTES = use SSIM8 MCT
    max_mct_override::Minutes = Minutes(480)
    min_connection_time::Minutes = NO_MINUTES        # NO_MINUTES = no minimum
    max_connection_time::Minutes = Minutes(480)
    circuity_tiers::Vector{CircuityTier} = DEFAULT_CIRCUITY_TIERS
    domestic_circuity_extra_miles::Float64 = 500.0
    international_circuity_extra_miles::Float64 = 1000.0

    # Forward-declared — not yet consumed by rules
    valid_codeshare_partners::Set{Tuple{AirlineCode,AirlineCode}} = Set{Tuple{AirlineCode,AirlineCode}}()
    valid_jv_groups::Set{InlineString7} = Set{InlineString7}()
    valid_wet_leases::Set{AirlineCode} = Set{AirlineCode}()

    # ── Leg-level filters ────────────────────────────────────────────────
    min_leg_distance::Distance = Distance(0.0)
    max_leg_distance::Distance = Distance(Inf32)
    allow_service_types::Set{Char} = Set{Char}()
    deny_service_types::Set{Char} = Set{Char}()
    allow_aircraft_types::Set{InlineString7} = Set{InlineString7}()
    deny_aircraft_types::Set{InlineString7} = Set{InlineString7}()
    allow_body_types::Set{Char} = Set{Char}()
    deny_body_types::Set{Char} = Set{Char}()

    # ── Connection-level categorical filters ─────────────────────────────
    allow_stations::Set{StationCode} = Set{StationCode}()
    deny_stations::Set{StationCode} = Set{StationCode}()
    allow_countries::Set{InlineString3} = Set{InlineString3}()
    deny_countries::Set{InlineString3} = Set{InlineString3}()
    allow_regions::Set{InlineString3} = Set{InlineString3}()
    deny_regions::Set{InlineString3} = Set{InlineString3}()
    allow_states::Set{InlineString3} = Set{InlineString3}()
    deny_states::Set{InlineString3} = Set{InlineString3}()

    # ── Itinerary-level ──────────────────────────────────────────────────
    min_stops::Int16 = Int16(0)
    max_stops::Int16 = Int16(2)
    min_elapsed::Int32 = Int32(0)
    max_elapsed::Int32 = Int32(1440)
    min_flight_time::Int32 = Int32(0)
    max_flight_time::Int32 = Int32(9999)
    min_layover_time::Int32 = Int32(0)
    max_layover_time::Int32 = Int32(9999)
    min_total_distance::Distance = Distance(0.0)
    max_total_distance::Distance = Distance(Inf32)
    min_circuity::Float64 = 0.0
    max_circuity::Float64 = Inf   # global ceiling on effective factor; Inf = no ceiling
    max_results::Int32 = Int32(0)           # 0 = unlimited

    # ── Itinerary-level carrier filters ──────────────────────────────────
    allow_carriers::Set{AirlineCode} = Set{AirlineCode}()
    deny_carriers::Set{AirlineCode} = Set{AirlineCode}()
    allow_operating_carriers::Set{AirlineCode} = Set{AirlineCode}()
    deny_operating_carriers::Set{AirlineCode} = Set{AirlineCode}()
end

"""
    struct MarketOverride
---

# Description
- Per-market parameter overrides with specificity cascade
- Match criteria use `WILDCARD_*` sentinels for "match anything"; a field set to a
  wildcard constant matches any value in that position
- More specific overrides (higher `specificity` value) should appear first in the
  `SearchConstraints.overrides` vector so they win in `resolve_params`
- Country and region matching (`origin_country`, `dest_country`, `origin_region`,
  `dest_region`) require station metadata lookup; the fields are stored here but
  geographic matching will be fully activated when RuntimeContext provides a station
  lookup callback — see `_override_matches` TODO

# Fields
- `origin::StationCode` — IATA origin airport; `WILDCARD_STATION` matches any
- `destination::StationCode` — IATA destination airport; `WILDCARD_STATION` matches any
- `origin_country::InlineString3` — ISO-2 country of origin; `WILDCARD_COUNTRY` matches any
- `dest_country::InlineString3` — ISO-2 country of destination; `WILDCARD_COUNTRY` matches any
- `origin_region::InlineString3` — IATA region of origin; `WILDCARD_REGION` matches any
- `dest_region::InlineString3` — IATA region of destination; `WILDCARD_REGION` matches any
- `carrier::AirlineCode` — IATA carrier code; `WILDCARD_AIRLINE` matches any
- `params::ParameterSet` — the parameter set to apply when this override matches
- `specificity::UInt32` — sort key; higher value = more specific; first match in descending order wins
"""
@kwdef struct MarketOverride
    origin::StationCode = WILDCARD_STATION
    destination::StationCode = WILDCARD_STATION
    origin_country::InlineString3 = WILDCARD_COUNTRY
    dest_country::InlineString3 = WILDCARD_COUNTRY
    origin_region::InlineString3 = WILDCARD_REGION
    dest_region::InlineString3 = WILDCARD_REGION
    carrier::AirlineCode = WILDCARD_AIRLINE
    params::ParameterSet = ParameterSet()
    specificity::UInt32 = UInt32(0)
end

"""
    struct SearchConstraints
---

# Description
- Global defaults plus market-level overrides plus simulation controls
- `defaults` is the fallback `ParameterSet` used when no override matches a query
- `overrides` must be sorted by descending `specificity` before being passed to
  `resolve_params`; `resolve_params` returns the first matching override's params
- Simulation controls (`closed_stations`, `closed_markets`, `delays`,
  `flight_delays`) allow injecting disruption scenarios without modifying schedules

# Fields
- `defaults::ParameterSet` — global parameter defaults
- `overrides::Vector{MarketOverride}` — market-level overrides, sorted descending specificity
- `closed_stations::Set{StationCode}` — stations excluded from connection building
- `closed_markets::Set{Tuple{StationCode,StationCode}}` — (origin, dest) pairs excluded from search
- `delays::Dict{StationCode,Minutes}` — station-level ground delays (minutes) applied to all departures
- `flight_delays::Dict{UInt64,Minutes}` — per-flight delays keyed by `LegRecord.row_number`
"""
@kwdef struct SearchConstraints
    defaults::ParameterSet = ParameterSet()
    overrides::Vector{MarketOverride} = MarketOverride[]    # sorted by descending specificity
    # Forward-declared — not yet consumed by builder/search (planned for reaccommodation scenario engine)
    closed_stations::Set{StationCode} = Set{StationCode}()
    closed_markets::Set{Tuple{StationCode,StationCode}} = Set{Tuple{StationCode,StationCode}}()
    delays::Dict{StationCode,Minutes} = Dict{StationCode,Minutes}()
    flight_delays::Dict{UInt64,Minutes} = Dict{UInt64,Minutes}()
end

# ── Dict constructors ────────────────────────────────────────────────────────
# Accept AbstractDict inputs with Symbol or String keys.  Nested dict values
# for struct-typed fields (`defaults`, `params`, `overrides` elements) are
# constructed recursively.  Unknown keys throw ArgumentError.
# Key normalization and unknown-key validation live in
# `src/types/dict_ctor_helpers.jl` (loaded earlier).

"""
    `ParameterSet(d::AbstractDict)::ParameterSet`

Construct a `ParameterSet` from an `AbstractDict` with `String` or `Symbol`
keys.  Scalar numeric fields coerce through `@kwdef`; `Set`-typed fields
accept any iterable (e.g. `Vector{String}`) and are wrapped with the
appropriate element type.  Unknown keys throw `ArgumentError`.
"""
function ParameterSet(d::AbstractDict)::ParameterSet
    kw = _normalize_dict_keys(d)
    _validate_known_fields(kw, ParameterSet)
    return ParameterSet(; kw...)
end

"""
    `MarketOverride(d::AbstractDict)::MarketOverride`

Construct a `MarketOverride` from an `AbstractDict`.  Station/carrier fields
accept `String` values (wrapped via `StationCode`/`AirlineCode`); nested
`params` accepts an `AbstractDict` and is constructed recursively.  Unknown
keys throw `ArgumentError`.
"""
function MarketOverride(d::AbstractDict)::MarketOverride
    kw = _normalize_dict_keys(d)
    _validate_known_fields(kw, MarketOverride)
    # String → wrapped inline string types
    if haskey(kw, :origin) && kw[:origin] isa AbstractString
        kw[:origin] = StationCode(kw[:origin])
    end
    if haskey(kw, :destination) && kw[:destination] isa AbstractString
        kw[:destination] = StationCode(kw[:destination])
    end
    if haskey(kw, :carrier) && kw[:carrier] isa AbstractString
        kw[:carrier] = AirlineCode(kw[:carrier])
    end
    for fld in (:origin_country, :dest_country, :origin_region, :dest_region)
        if haskey(kw, fld) && kw[fld] isa AbstractString
            kw[fld] = InlineString3(kw[fld])
        end
    end
    # Nested ParameterSet
    if haskey(kw, :params) && kw[:params] isa AbstractDict
        kw[:params] = ParameterSet(kw[:params])
    end
    return MarketOverride(; kw...)
end

"""
    `SearchConstraints(d::AbstractDict)::SearchConstraints`

Construct a `SearchConstraints` from an `AbstractDict`.  Recursively
constructs `defaults::ParameterSet` from a nested dict and each element of
`overrides::Vector{MarketOverride}` from a nested dict (if the element is
not already a `MarketOverride`).  Unknown keys throw `ArgumentError`.
"""
function SearchConstraints(d::AbstractDict)::SearchConstraints
    kw = _normalize_dict_keys(d)
    _validate_known_fields(kw, SearchConstraints)
    if haskey(kw, :defaults) && kw[:defaults] isa AbstractDict
        kw[:defaults] = ParameterSet(kw[:defaults])
    end
    if haskey(kw, :overrides) && kw[:overrides] isa AbstractVector
        kw[:overrides] = MarketOverride[
            x isa MarketOverride ? x : MarketOverride(x) for x in kw[:overrides]
        ]
    end
    return SearchConstraints(; kw...)
end

# ── Circuity lookup helpers ───────────────────────────────────────────────────

"""
    `_effective_circuity_factor(p::ParameterSet, distance::Float64)::Float64`
---

# Description
- The circuity factor to use in the rule check: `min(tier_factor, p.max_circuity)`
- `min` with `Inf` is a no-op, so the default-case overhead is a single comparison
- Combines the tiered lookup from `p.circuity_tiers` with the scalar ceiling
  from `p.max_circuity`

# Arguments
1. `p::ParameterSet`: the effective parameter set for the market
2. `distance::Float64`: route or leg distance in miles

# Returns
- `::Float64`: the effective circuity factor (tier value capped by scalar ceiling)
"""
@inline function _effective_circuity_factor(p::ParameterSet, distance::Float64)::Float64
    tier_factor = _circuity_factor_at(p.circuity_tiers, distance)
    return min(tier_factor, p.max_circuity)
end

"""
    `function _resolve_circuity_params(constraints::SearchConstraints, origin::StationCode, dest::StationCode)::ParameterSet`
---

# Description
- Market-only override resolver for circuity checks
- Iterates `constraints.overrides` (pre-sorted by descending specificity) and
  returns the first override whose origin and destination wildcards/equality match
- **Carrier is ignored** — circuity is a geographic property and should not vary
  by marketing carrier
- Returns `constraints.defaults` when no override matches

# Arguments
1. `constraints::SearchConstraints`: the global constraints holder
2. `origin::StationCode`: origin airport code of the query
3. `dest::StationCode`: destination airport code of the query

# Returns
- `::ParameterSet`: the effective parameter set for circuity evaluation

# Examples
```julia
julia> sc = SearchConstraints();
julia> p = _resolve_circuity_params(sc, StationCode("ORD"), StationCode("LHR"));
julia> p === sc.defaults
true
```
"""
function _resolve_circuity_params(
    constraints::SearchConstraints,
    origin::StationCode,
    dest::StationCode,
)::ParameterSet
    isempty(constraints.overrides) && return constraints.defaults
    for override in constraints.overrides
        (override.origin == WILDCARD_STATION || override.origin == origin) || continue
        (override.destination == WILDCARD_STATION || override.destination == dest) || continue
        return override.params
    end
    return constraints.defaults
end

# ── Override matching ─────────────────────────────────────────────────────────

"""
    `function _override_matches(o::MarketOverride, origin::StationCode, dest::StationCode, carrier::AirlineCode)::Bool`
---

# Description
- Returns `true` when the given origin/dest/carrier satisfies all non-wildcard
  match criteria in `o`
- A field set to the corresponding `WILDCARD_*` sentinel matches any value
- TODO: Geographic matching (origin_country, dest_country, origin_region,
  dest_region) will be extended when RuntimeContext provides a station lookup
  callback; currently those fields are stored but not evaluated here

# Arguments
1. `o::MarketOverride`: the override to test
2. `origin::StationCode`: origin airport code of the query
3. `dest::StationCode`: destination airport code of the query
4. `carrier::AirlineCode`: itinerary carrier code of the query

# Returns
- `::Bool`: `true` if the override applies to this O-D-carrier triplet
"""
function _override_matches(
    o::MarketOverride, origin::StationCode, dest::StationCode, carrier::AirlineCode
)::Bool
    (o.origin == WILDCARD_STATION || o.origin == origin) || return false
    (o.destination == WILDCARD_STATION || o.destination == dest) || return false
    (o.carrier == WILDCARD_AIRLINE || o.carrier == carrier) || return false
    return true
end

"""
    `function resolve_params(constraints::SearchConstraints, origin::StationCode, dest::StationCode, carrier::AirlineCode)::ParameterSet`
---

# Description
- Resolves the effective `ParameterSet` for a market query
- Iterates `constraints.overrides` (expected to be sorted by descending specificity);
  returns the `params` of the first override that matches the given O-D-carrier triplet
- Falls back to `constraints.defaults` when no override matches

# Arguments
1. `constraints::SearchConstraints`: the global constraints holder
2. `origin::StationCode`: origin airport code
3. `dest::StationCode`: destination airport code
4. `carrier::AirlineCode`: itinerary carrier code

# Returns
- `::ParameterSet`: the effective parameter set for this market

# Examples
```julia
julia> sc = SearchConstraints();
julia> p = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("UA"));
julia> p === sc.defaults
true
```
"""
function resolve_params(
    constraints::SearchConstraints,
    origin::StationCode,
    dest::StationCode,
    carrier::AirlineCode,
)::ParameterSet
    for override in constraints.overrides
        if _override_matches(override, origin, dest, carrier)
            return override.params
        end
    end
    return constraints.defaults
end

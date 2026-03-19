# src/types/constraints.jl — Constraint and parameter types for connection building and search

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
- `min_mct_override::Minutes` — minimum MCT to enforce; `NO_MINUTES` means use SSIM8 MCT table
- `max_mct_override::Minutes` — maximum connection time allowed (minutes)
- `circuity_factor::Float64` — maximum ratio of flown distance to market distance per leg
- `circuity_extra_miles::Float64` — flat mileage tolerance added to circuity threshold
- `valid_codeshare_partners::Set{Tuple{AirlineCode,AirlineCode}}` — allowed marketing/operating pairs; empty = allow all
- `valid_jv_groups::Set{InlineString7}` — allowed joint-venture group codes; empty = allow all
- `valid_wet_leases::Set{AirlineCode}` — allowed wet-lease operating carriers; empty = allow all
- `min_leg_distance::Distance` — minimum leg distance (miles)
- `max_leg_distance::Distance` — maximum leg distance (miles); `Inf32` = unlimited
- `min_stops::Int16` — minimum number of intermediate stops (0 = nonstop allowed)
- `max_stops::Int16` — maximum number of intermediate stops
- `min_elapsed::Int32` — minimum total elapsed time for the itinerary (minutes)
- `max_elapsed::Int32` — maximum total elapsed time for the itinerary (minutes)
- `min_total_distance::Distance` — minimum total flown distance (miles)
- `max_total_distance::Distance` — maximum total flown distance (miles); `Inf32` = unlimited
- `itinerary_circuity::Float64` — maximum ratio of total flown distance to market distance
"""
@kwdef struct ParameterSet
    # Connection-level
    min_mct_override::Minutes = NO_MINUTES          # NO_MINUTES = use SSIM8 MCT
    max_mct_override::Minutes = Minutes(480)
    circuity_factor::Float64 = 2.0
    circuity_extra_miles::Float64 = 500.0
    valid_codeshare_partners::Set{Tuple{AirlineCode,AirlineCode}} = Set{Tuple{AirlineCode,AirlineCode}}()
    valid_jv_groups::Set{InlineString7} = Set{InlineString7}()
    valid_wet_leases::Set{AirlineCode} = Set{AirlineCode}()
    min_leg_distance::Distance = Distance(0.0)
    max_leg_distance::Distance = Distance(Inf32)

    # Itinerary-level
    min_stops::Int16 = Int16(0)
    max_stops::Int16 = Int16(2)
    min_elapsed::Int32 = Int32(0)
    max_elapsed::Int32 = Int32(1440)
    min_total_distance::Distance = Distance(0.0)
    max_total_distance::Distance = Distance(Inf32)
    itinerary_circuity::Float64 = 2.5
    max_results::Int32 = Int32(0)           # 0 = unlimited; stop search after N results per OD
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
    closed_stations::Set{StationCode} = Set{StationCode}()
    closed_markets::Set{Tuple{StationCode,StationCode}} = Set{Tuple{StationCode,StationCode}}()
    delays::Dict{StationCode,Minutes} = Dict{StationCode,Minutes}()
    flight_delays::Dict{UInt64,Minutes} = Dict{UInt64,Minutes}()
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

# src/search/universe.jl — Market-universe enumeration strategies for search_schedule.
#
# Each strategy takes some constraint (carriers, station, flight, zone) and
# returns a MarketUniverse — a concrete list of (origin, destination, date)
# triples to search. Feature B uses :direct and :connected strategies;
# future plans (C/E/F) add new strategies without touching the sweep machinery.

"""
    `MarketUniverse(tuples::Vector{Tuple{String,String,Date}})`
---

# Description
- A concrete list of `(origin, destination, date)` triples to search
- Produced by enumeration strategies (`_universe_from_*`); consumed by `search_schedule`
- Public so advanced callers can precompute a universe once and reuse it

# Fields
- `tuples::Vector{Tuple{String,String,Date}}`: the full enumeration
"""
struct MarketUniverse
    tuples::Vector{Tuple{String,String,Date}}
end

"""
    `_compute_filter_set(store::DuckDBStore, date::Date, carriers::Union{Nothing, AbstractVector{<:AbstractString}}, include_codeshare::Bool)::Union{Nothing, Set{String}}`
---

# Description
- Build the effective carrier-filter set for a universe query
- `carriers === nothing` short-circuits to `nothing` (no filter)
- `include_codeshare=false` returns the carriers as-is
- `include_codeshare=true` unions in codeshare partners from the schedule

# Arguments
1. `store::DuckDBStore`: data store
2. `date::Date`: operating date
3. `carriers::Union{Nothing, AbstractVector{<:AbstractString}}`: source list or nothing
4. `include_codeshare::Bool`: whether to expand to partners

# Returns
- `::Union{Nothing, Set{String}}`: filter set, or `nothing` for no filter
"""
function _compute_filter_set(
    store::DuckDBStore,
    date::Date,
    carriers::Union{Nothing, AbstractVector{<:AbstractString}},
    include_codeshare::Bool,
)::Union{Nothing, Set{String}}
    carriers === nothing && return nothing
    base = Set(String(c) for c in carriers)
    include_codeshare || return base
    partners = query_codeshare_partners(store, date, collect(base))
    return union(base, Set(partners))
end

"""
    `_universe_from_carriers_direct(store::DuckDBStore, date::Date, carriers::Union{Nothing, AbstractVector{<:AbstractString}}, include_codeshare::Bool)::MarketUniverse`
---

# Description
- Enumerate markets with at least one direct flight operated or marketed by a
  filter-set carrier on `date`
- Uses `query_direct_markets_by_carriers` under the hood
- When `carriers === nothing`, returns all direct-flight markets on the date

# Arguments
1. `store::DuckDBStore`: data store
2. `date::Date`: operating date
3. `carriers::Union{Nothing, AbstractVector{<:AbstractString}}`: carrier filter or nothing
4. `include_codeshare::Bool`: expand to codeshare partners before filtering

# Returns
- `::MarketUniverse`: distinct (origin, destination, date) tuples
"""
function _universe_from_carriers_direct(
    store::DuckDBStore,
    date::Date,
    carriers::Union{Nothing, AbstractVector{<:AbstractString}},
    include_codeshare::Bool,
)::MarketUniverse
    filter_set = _compute_filter_set(store, date, carriers, include_codeshare)
    filter_list = filter_set === nothing ? nothing : collect(filter_set)
    pairs = query_direct_markets_by_carriers(store, date, filter_list)
    return MarketUniverse([(o, d, date) for (o, d) in pairs])
end

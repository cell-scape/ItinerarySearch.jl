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

"""
    `_leg_matches_carrier(leg::GraphLeg, filter_set::Set{String})::Bool`
---

# Description
- Returns `true` when the leg's marketing carrier or non-empty operating carrier
  appears in `filter_set`
- `operating_carrier` is an `AirlineCode` (`InlineString3`) — the empty string
  signals "no distinct operating carrier" (host flight)

# Arguments
1. `leg::GraphLeg`: graph leg to test
2. `filter_set::Set{String}`: carrier codes to match against

# Returns
- `::Bool`: whether the leg belongs to a filter carrier
"""
function _leg_matches_carrier(leg::GraphLeg, filter_set::Set{String})::Bool
    carrier_s = String(leg.record.carrier)
    carrier_s in filter_set && return true
    op_s = String(leg.record.operating_carrier)
    !isempty(op_s) && op_s in filter_set && return true
    return false
end

"""
    `_bfs_legs(anchor::GraphLeg, direction::Symbol, max_depth::Int)::Vector{Tuple{GraphLeg,Int}}`
---

# Description
- Breadth-first traversal along `.connect_from` (`direction=:back`) or
  `.connect_to` (`direction=:forward`) starting at `anchor`
- Returns `(leg, depth)` pairs where `depth=0` is the anchor itself and the
  traversal is bounded by `max_depth` hops
- Each leg is visited once (`Set{GraphLeg}` dedup) — the depth recorded is the
  depth of the first arrival, which is the minimum by BFS ordering

# Arguments
1. `anchor::GraphLeg`: starting leg
2. `direction::Symbol`: `:back` (walk `connect_from`, follow `from_leg`) or
   `:forward` (walk `connect_to`, follow `to_leg`)
3. `max_depth::Int`: maximum number of connection hops

# Returns
- `::Vector{Tuple{GraphLeg,Int}}`: visited legs with their (minimum) depth
"""
function _bfs_legs(anchor::GraphLeg, direction::Symbol, max_depth::Int)::Vector{Tuple{GraphLeg,Int}}
    result = Tuple{GraphLeg,Int}[(anchor, 0)]
    max_depth <= 0 && return result

    visited = Set{GraphLeg}()
    push!(visited, anchor)
    frontier = Tuple{GraphLeg,Int}[(anchor, 0)]

    while !isempty(frontier)
        (leg, depth) = popfirst!(frontier)
        depth >= max_depth && continue

        next_cps = direction === :back ? leg.connect_from : leg.connect_to
        for cp in next_cps
            next_leg = (direction === :back ? cp.from_leg : cp.to_leg)::GraphLeg
            next_leg in visited && continue
            push!(visited, next_leg)
            push!(result, (next_leg, depth + 1))
            push!(frontier, (next_leg, depth + 1))
        end
    end
    return result
end

"""
    `_universe_from_carriers_connected(graph::FlightGraph, date::Date, carriers::Union{Nothing, AbstractVector{<:AbstractString}}, include_codeshare::Bool)::MarketUniverse`
---

# Description
- Enumerate markets where a filter-set carrier's leg appears in at least one
  valid itinerary up to `graph.config.max_stops` deep
- Uses the already-built connection graph (`.connect_from` / `.connect_to`
  lists on `GraphLeg`)
- For each filtered "anchor" leg `L`, BFS backward via `.connect_from` and
  forward via `.connect_to`; enumerate `(first_leg, L, last_leg)` combinations
  where `back_depth + forward_depth <= max_stops`, recording the market as
  `(first_leg.record.departure_station, last_leg.record.arrival_station, date)`
- Deduplicates via `Set` before returning

# Arguments
1. `graph::FlightGraph`: already-built graph (supplies `legs` and `config.max_stops`)
2. `date::Date`: operating date (matches graph's target)
3. `carriers::Union{Nothing, AbstractVector{<:AbstractString}}`: filter, or
   `nothing` for no filter (every leg anchors)
4. `include_codeshare::Bool`: **must be `false`** when `carriers !== nothing`
   — this function has no store access, so it cannot expand partners. Callers
   should pre-expand via `_compute_filter_set(store, date, carriers, true)`
   and pass the result with `include_codeshare=false`

# Returns
- `::MarketUniverse`: distinct `(origin, destination, date)` tuples

# Throws
- `ArgumentError`: when `include_codeshare=true` is paired with non-nothing
  `carriers`
"""
function _universe_from_carriers_connected(
    graph,  # ::FlightGraph — not annotated: type is defined in graph/builder.jl, included later
    date::Date,
    carriers::Union{Nothing, AbstractVector{<:AbstractString}},
    include_codeshare::Bool,
)::MarketUniverse
    filter_set = if carriers === nothing
        nothing
    elseif include_codeshare
        throw(ArgumentError(
            "_universe_from_carriers_connected requires pre-expanded carriers " *
            "when include_codeshare=true; compute the filter set via " *
            "_compute_filter_set(store, date, carriers, true) and pass the result " *
            "as carriers with include_codeshare=false",
        ))
    else
        Set(String(c) for c in carriers)
    end

    max_stops = Int(graph.config.max_stops)

    # 1. Anchor legs — those matching the filter (or all legs if no filter).
    anchors = if filter_set === nothing
        graph.legs
    else
        filter(graph.legs) do leg
            _leg_matches_carrier(leg, filter_set)
        end
    end

    # 2. BFS backward and forward from each anchor, enumerate (first, last) pairs.
    markets = Set{Tuple{String,String,Date}}()
    for anchor in anchors
        back_legs = _bfs_legs(anchor, :back, max_stops)
        forward_legs = _bfs_legs(anchor, :forward, max_stops)

        for (first_leg, back_depth) in back_legs
            for (last_leg, forward_depth) in forward_legs
                # Total connections used = back_depth + forward_depth.
                # Bounded by graph.config.max_stops so the pair corresponds to
                # a reachable itinerary within the configured search depth.
                back_depth + forward_depth <= max_stops || continue
                origin_stn = String(first_leg.record.departure_station)
                dest_stn = String(last_leg.record.arrival_station)
                origin_stn == dest_stn && continue  # skip degenerate self-markets
                push!(markets, (origin_stn, dest_stn, date))
            end
        end
    end

    return MarketUniverse(collect(markets))
end

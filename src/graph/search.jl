# src/graph/search.jl — DFS itinerary search with push/pop pattern
#
# RuntimeContext holds all per-search mutable state: rule chains, caches,
# accumulators, and the working result vector.  search_itineraries drives a
# depth-first traversal of the pre-built connection graph.  The working
# Itinerary is mutated in place; deep copies are committed only when a
# complete path reaches the destination and passes all itinerary rules.

# ── RuntimeContext ─────────────────────────────────────────────────────────────

"""
    mutable struct RuntimeContext
---

# Description
- Per-thread mutable search state
- Holds shared immutable references (config, constraints, rule chains) plus
  per-thread mutable state (caches, accumulators, results)
- Construct with keyword arguments; all fields have zero/empty defaults

# Fields
- `config::SearchConfig` — search configuration (scope, interline, stop limits)
- `constraints::SearchConstraints` — market-level parameter overrides
- `cnx_rules::Vector{Any}` — connection rule chain (built by `build_cnx_rules`)
- `itn_rules::Vector{Any}` — itinerary rule chain (built by `build_itn_rules`)
- `gc_cache::Dict{UInt64, Float64}` — great-circle distance cache keyed by
  `hash(origin_code, hash(dest_code))`
- `target_date::UInt32` — packed YYYYMMDD target search date
- `target_dow::StatusBits` — single-bit DOW mask for the target date
- `results::Vector{Itinerary}` — committed itineraries from the current search
- `build_stats::BuildStats` — connection-build instrumentation accumulator
- `search_stats::SearchStats` — search instrumentation accumulator
- `mct_selections::Vector{MCTSelectionRow}` — MCT cascade audit log (Tier 1)
- `event_log::Vector{Any}` — structured event log (Tier 3; empty when disabled)
"""
@kwdef mutable struct RuntimeContext
    # Shared immutable references
    config::SearchConfig = SearchConfig()
    constraints::SearchConstraints = SearchConstraints()

    # Rule chains
    cnx_rules::Vector{Any} = Any[]
    itn_rules::Vector{Any} = Any[]

    # Caches
    gc_cache::Dict{UInt64,Float64} = Dict{UInt64,Float64}()

    # Search state
    target_date::UInt32 = UInt32(0)         # packed YYYYMMDD
    target_dow::StatusBits = StatusBits(0)  # DOW bit for target date

    results::Vector{Itinerary} = Itinerary[]

    # Accumulators
    build_stats::BuildStats = BuildStats()
    search_stats::SearchStats = SearchStats()

    # MCT selection tracking
    mct_selections::Vector{MCTSelectionRow} = MCTSelectionRow[]

    # Event log (tier 3, empty when disabled)
    event_log::Vector{Any} = Any[]
end

# ── Date validity helper ────────────────────────────────────────────────────────

"""
    `@inline function _is_valid_on_date(cp::GraphConnection, target::UInt32, dow::StatusBits)::Bool`
---

# Description
- Returns `true` when `target` falls within the connection's validity window
  AND the connection operates on the DOW indicated by `dow`
- Used in the DFS inner loop; must be branch-predictor friendly

# Arguments
1. `cp::GraphConnection`: connection to test
2. `target::UInt32`: packed YYYYMMDD search date
3. `dow::StatusBits`: single-bit DOW mask (from `dow_bit`)

# Returns
- `::Bool`
"""
@inline function _is_valid_on_date(
    cp::GraphConnection,
    target::UInt32,
    dow::StatusBits,
)::Bool
    cp.valid_from <= target <= cp.valid_to || return false
    (StatusBits(cp.valid_days) & dow) != StatusBits(0)
end

# ── Bearing & direction helpers ────────────────────────────────────────────────

"""
    `function _bearing(lat1::Float64, lng1::Float64, lat2::Float64, lng2::Float64)::Float64`

Compute the initial forward bearing (degrees 0–360) from point 1 to point 2
using the standard haversine-bearing formula.
"""
function _bearing(lat1::Float64, lng1::Float64, lat2::Float64, lng2::Float64)::Float64
    φ1 = deg2rad(lat1)
    φ2 = deg2rad(lat2)
    Δλ = deg2rad(lng2 - lng1)
    y = sin(Δλ) * cos(φ2)
    x = cos(φ1) * sin(φ2) - sin(φ1) * cos(φ2) * cos(Δλ)
    return mod(rad2deg(atan(y, x)), 360.0)
end

"""
    `@inline function _direction_ok(current::GraphStation, next_dst::GraphStation, final_dst::GraphStation; max_divergence_deg::Float64=120.0)::Bool`
---

# Description
- Quick directional-pruning heuristic: rejects DFS branches heading more than
  `max_divergence_deg` away from the destination bearing
- Returns `true` (no prune) when any station has zero coordinates, when
  `next_dst` is the final destination, or when the bearing divergence is within
  the allowed arc
- Used to cut obviously wrong branches before running the rule chain

# Arguments
1. `current::GraphStation`: the station from which `next_dst` departs
2. `next_dst::GraphStation`: the destination of the candidate next leg
3. `final_dst::GraphStation`: the overall search destination

# Keyword Arguments
- `max_divergence_deg::Float64=120.0`: maximum allowed bearing divergence (degrees)

# Returns
- `::Bool`: `true` if the direction is acceptable (or indeterminate)
"""
@inline function _direction_ok(
    current::GraphStation,
    next_dst::GraphStation,
    final_dst::GraphStation;
    max_divergence_deg::Float64 = 120.0,
)::Bool
    # Skip check when coordinates are unpopulated
    (current.record.lat == 0.0 && current.record.lng == 0.0) && return true
    (next_dst.record.lat == 0.0 && next_dst.record.lng == 0.0) && return true
    (final_dst.record.lat == 0.0 && final_dst.record.lng == 0.0) && return true

    # Arriving at the destination is always acceptable
    next_dst.code == final_dst.code && return true

    # Bearing from current to final destination
    b1 = _bearing(
        current.record.lat,
        current.record.lng,
        final_dst.record.lat,
        final_dst.record.lng,
    )
    # Bearing from current to next station
    b2 = _bearing(
        current.record.lat,
        current.record.lng,
        next_dst.record.lat,
        next_dst.record.lng,
    )

    # Smallest angular difference (wrap at 180°)
    diff = abs(b1 - b2)
    if diff > 180.0
        diff = 360.0 - diff
    end
    return diff <= max_divergence_deg
end

# ── Elapsed-time computation ────────────────────────────────────────────────────

"""
    `function _compute_elapsed(itn::Itinerary)::Int32`
---

# Description
- Computes the definitive elapsed time (minutes) from the first passenger
  departure to the last passenger arrival
- Accounts for overnight arrivals via `arr_date_var` on the final leg
  (each +1 adds 1440 minutes)
- Connection ground times for intermediate stops are added on top of the
  dep-to-arr block times
- Returns `Int32(0)` for an empty itinerary

# Arguments
1. `itn::Itinerary`: the completed itinerary to measure

# Returns
- `::Int32`: total elapsed time in minutes
"""
function _compute_elapsed(itn::Itinerary)::Int32
    isempty(itn.connections) && return Int32(0)

    first_cp = itn.connections[1]
    last_cp = itn.connections[end]

    first_leg = first_cp.from_leg::GraphLeg
    # When to_leg === from_leg the connection is a nonstop self-connection;
    # the "last" leg is still to_leg.
    last_leg = last_cp.to_leg::GraphLeg

    dep = Int32(first_leg.record.pax_dep)
    arr =
        Int32(last_leg.record.pax_arr) +
        Int32(last_leg.record.arr_date_var) * Int32(1440)

    elapsed = arr - dep

    # Add connection ground times for multi-leg itineraries (skip first nonstop)
    for i in 2:length(itn.connections)
        cp = itn.connections[i]::GraphConnection
        elapsed += Int32(cp.cnx_time)
    end

    return max(Int32(0), elapsed)
end

# ── Geographic diversity helpers ────────────────────────────────────────────────

"""
    `function _add_station_geo!(metros, states, countries, regions, stn::GraphStation)::Nothing`

Push the geographic attributes of `stn` into the four running sets.
No-ops when `stn.code == NO_STATION` or a field is empty.
"""
function _add_station_geo!(metros, states, countries, regions, stn::GraphStation)
    stn.code == NO_STATION && return
    rec = stn.record
    rec.city != InlineString31("") && push!(metros, rec.city)
    rec.state != InlineString3("") && push!(states, rec.state)
    rec.country != InlineString3("") && push!(countries, rec.country)
    rec.region != InlineString3("") && push!(regions, rec.region)
    return nothing
end

"""
    `function _count_geo_diversity(itn::Itinerary)::NTuple{4, Int16}`
---

# Description
- Counts distinct metros, states, countries, and IATA regions visited by the
  itinerary (including origin and all intermediate + final stations)
- Uses `Set` membership; short-circuits on the `NO_STATION` sentinel

# Arguments
1. `itn::Itinerary`: completed itinerary

# Returns
- `::NTuple{4, Int16}`: `(num_metros, num_states, num_countries, num_regions)`
"""
function _count_geo_diversity(itn::Itinerary)::NTuple{4,Int16}
    metros = Set{InlineString31}()
    states = Set{InlineString3}()
    countries = Set{InlineString3}()
    regions = Set{InlineString3}()

    for cp in itn.connections
        leg = cp.from_leg::GraphLeg
        _add_station_geo!(metros, states, countries, regions, leg.org)
        _add_station_geo!(metros, states, countries, regions, leg.dst)
    end
    # Ensure final destination of last connection is captured
    if !isempty(itn.connections)
        last_leg = (itn.connections[end]::GraphConnection).to_leg::GraphLeg
        _add_station_geo!(metros, states, countries, regions, last_leg.dst)
    end

    return (
        Int16(length(metros)),
        Int16(length(states)),
        Int16(length(countries)),
        Int16(length(regions)),
    )
end

# ── Validate and commit ─────────────────────────────────────────────────────────

"""
    `function _validate_and_commit!(itn::Itinerary, ctx::RuntimeContext)::Nothing`
---

# Description
- Runs the itinerary rule chain on `itn`; if all rules pass, deep-copies `itn`
  into a committed `Itinerary` and appends it to `ctx.results`
- Computes `elapsed_time`, geographic diversity, and `circuity` on the committed
  copy (these are derived fields not maintained during DFS push/pop)
- Increments `ctx.search_stats.paths_rejected` on any rule failure;
  increments `ctx.search_stats.paths_found` and `paths_by_stops` on success

# Arguments
1. `itn::Itinerary`: working itinerary (read-only; deep-copied on success)
2. `ctx::RuntimeContext`: search context (mutated: `results`, `search_stats`)
"""
function _validate_and_commit!(itn::Itinerary, ctx::RuntimeContext)
    # Run itinerary rule chain
    for rule in ctx.itn_rules
        rc = rule(itn, ctx)
        if rc <= 0
            ctx.search_stats.paths_rejected += Int32(1)
            return
        end
    end

    # Compute geographic diversity
    metros, states, countries, regions = _count_geo_diversity(itn)

    # Deep copy and commit
    committed = Itinerary(
        connections=copy(itn.connections),
        status=itn.status,
        elapsed_time=_compute_elapsed(itn),
        num_stops=itn.num_stops,
        num_eqp_changes=itn.num_eqp_changes,
        total_distance=itn.total_distance,
        market_distance=itn.market_distance,
        circuity=itn.total_distance / max(itn.market_distance, Distance(1.0f0)),
        num_metros=metros,
        num_states=states,
        num_countries=countries,
        num_regions=regions,
    )
    push!(ctx.results, committed)

    # Update search stats
    ctx.search_stats.paths_found += Int32(1)
    bucket = min(Int(itn.num_stops) + 1, 4)
    if bucket <= length(ctx.search_stats.paths_by_stops)
        ctx.search_stats.paths_by_stops[bucket] += Int32(1)
    end

    return nothing
end

# ── DFS core ────────────────────────────────────────────────────────────────────

"""
    `function _dfs!(dest::GraphStation, itn::Itinerary, current_leg::GraphLeg, ctx::RuntimeContext, depth::Int)::Nothing`
---

# Description
- Depth-first search from `current_leg` toward `dest`
- For each outbound `GraphConnection` in `current_leg.connect_to`:
  1. Validates the connection against the target date / DOW
  2. Skips nonstop self-connections (they are the starting leg, not a real hop)
  3. Applies direction pruning
  4. Pushes accumulated state onto `itn`, recurses, then pops (push/pop pattern)
- Commits when `current_leg.dst === dest` (via `_validate_and_commit!`)
- Short-circuits when `depth >= max_stops` to bound recursion

# Arguments
1. `dest::GraphStation`: the search destination station
2. `itn::Itinerary`: working itinerary (mutated during DFS, restored on return)
3. `current_leg::GraphLeg`: the most recently traversed leg
4. `ctx::RuntimeContext`: search context (accumulators, config, rule chains)
5. `depth::Int`: current recursion depth (0-based stop count)
"""
function _dfs!(
    dest::GraphStation,
    itn::Itinerary,
    current_leg::GraphLeg,
    ctx::RuntimeContext,
    depth::Int,
)
    # Reached destination
    if current_leg.dst === dest
        _validate_and_commit!(itn, ctx)
        return
    end

    # Max-depth guard
    max_stops = Int(ctx.constraints.defaults.max_stops)
    depth >= max_stops && return

    connect_to = current_leg.connect_to
    @inbounds for i in 1:length(connect_to)
        cp = connect_to[i]::GraphConnection

        # Date / DOW filter
        _is_valid_on_date(cp, ctx.target_date, ctx.target_dow) || continue

        # Skip nonstop self-connections in recursive calls
        cp.from_leg === cp.to_leg && continue

        next_leg = cp.to_leg::GraphLeg

        # Direction pruning
        _direction_ok(cp.station, next_leg.dst, dest) || continue

        # ── Push state ────────────────────────────────────────────────────────
        push!(itn.connections, cp)
        old_status = itn.status
        old_stops = itn.num_stops
        old_eqp = itn.num_eqp_changes
        old_elapsed = itn.elapsed_time
        old_distance = itn.total_distance

        # DOW intersection
        itn.status |= cp.status
        itn.num_stops += Int16(1)

        # Equipment-change detection (skip through-service legs)
        if !cp.is_through && cp.from_leg.record.eqp != cp.to_leg.record.eqp
            itn.num_eqp_changes += Int16(1)
        end

        itn.elapsed_time += Int32(cp.cnx_time)
        itn.total_distance += cp.to_leg.record.distance

        # ── Recurse ───────────────────────────────────────────────────────────
        _dfs!(dest, itn, next_leg, ctx, depth + 1)

        # ── Pop state ─────────────────────────────────────────────────────────
        pop!(itn.connections)
        itn.status = old_status
        itn.num_stops = old_stops
        itn.num_eqp_changes = old_eqp
        itn.elapsed_time = old_elapsed
        itn.total_distance = old_distance
    end
end

# ── Nonstop-connection finder ───────────────────────────────────────────────────

"""
    `function _find_nonstop_connection(station::GraphStation, leg::GraphLeg)::Union{GraphConnection, Nothing}`
---

# Description
- Scans `station.connections` for the nonstop self-connection whose `from_leg`
  and `to_leg` are both `leg` (pointer identity)
- Returns `nothing` when no matching self-connection is found

# Arguments
1. `station::GraphStation`: the departure station
2. `leg::GraphLeg`: the leg whose nonstop self-connection is sought

# Returns
- `::Union{GraphConnection, Nothing}`
"""
function _find_nonstop_connection(
    station::GraphStation,
    leg::GraphLeg,
)::Union{GraphConnection,Nothing}
    for cp_any in station.connections
        cp = cp_any::GraphConnection
        if cp.from_leg === leg && cp.to_leg === leg
            return cp
        end
    end
    return nothing
end

# ── Public API ─────────────────────────────────────────────────────────────────

"""
    `function search_itineraries(stations::Dict{StationCode, GraphStation}, origin::StationCode, dest::StationCode, target_date::Date, ctx::RuntimeContext)::Vector{Itinerary}`
---

# Description
- Searches all valid itineraries from `origin` to `dest` on `target_date`
- Iterates every departure leg at the origin that is valid on `target_date`;
  for each one finds or synthesises its nonstop self-connection, then:
  - Commits a nonstop itinerary when `dep_leg.dst === dst_stn`
  - Drives `_dfs!` to find 1-stop, 2-stop, … itineraries within `max_stops`
- Computes the great-circle market distance once per O-D pair and caches it in
  `ctx.gc_cache` for subsequent calls with the same O-D
- Increments `ctx.search_stats.queries` on entry; clears `ctx.results` at start
- Returns a reference to `ctx.results` (no allocation; caller should copy if
  the reference must outlive the next search call)

# Arguments
1. `stations::Dict{StationCode, GraphStation}`: the full station graph
2. `origin::StationCode`: IATA code of the departure airport
3. `dest::StationCode`: IATA code of the arrival airport
4. `target_date::Date`: the target travel date
5. `ctx::RuntimeContext`: per-thread search context (mutated)

# Returns
- `::Vector{Itinerary}`: all valid itineraries found (reference to `ctx.results`)

# Examples
```julia
julia> ctx = RuntimeContext(itn_rules=build_itn_rules(SearchConfig()));
julia> itns = search_itineraries(stations, StationCode("JFK"), StationCode("LHR"),
                                  Date(2026,6,15), ctx);
julia> length(itns) >= 0
true
```
"""
function search_itineraries(
    stations::Dict{StationCode,GraphStation},
    origin::StationCode,
    dest::StationCode,
    target_date::Date,
    ctx::RuntimeContext,
)::Vector{Itinerary}
    # Look up stations
    org_stn = get(stations, origin, nothing)
    dst_stn = get(stations, dest, nothing)
    (org_stn === nothing || dst_stn === nothing) && return Itinerary[]

    # Update search state
    ctx.target_date = pack_date(target_date)
    ctx.target_dow = dow_bit(Dates.dayofweek(target_date))
    empty!(ctx.results)
    ctx.search_stats.queries += Int32(1)

    # Great-circle market distance (cached)
    gc_key = hash(origin, hash(dest))
    market_dist = get(ctx.gc_cache, gc_key, -1.0)
    if market_dist < 0.0
        market_dist = _haversine_distance(
            org_stn.record.lat,
            org_stn.record.lng,
            dst_stn.record.lat,
            dst_stn.record.lng,
        )
        ctx.gc_cache[gc_key] = market_dist
    end

    # Working itinerary — mutated during DFS, never committed directly
    working = Itinerary(market_distance=Distance(market_dist))

    packed = ctx.target_date
    dow = ctx.target_dow

    # Iterate every departure at origin
    for i in 1:length(org_stn.departures)
        dep_leg = org_stn.departures[i]::GraphLeg

        # Validity check on the leg itself
        freq = StatusBits(dep_leg.record.frequency)
        valid_date = dep_leg.record.eff_date <= packed <= dep_leg.record.disc_date
        valid_dow = (freq & dow) != StatusBits(0)
        (!valid_date || !valid_dow) && continue

        # Retrieve the nonstop self-connection created during build_connections!
        ns_cp = _find_nonstop_connection(org_stn, dep_leg)
        ns_cp === nothing && continue

        # Reset working itinerary for this departure
        empty!(working.connections)
        working.status = StatusBits(dep_leg.record.frequency) & DOW_MASK
        working.num_stops = Int16(0)
        working.num_eqp_changes = Int16(0)
        working.elapsed_time = Int32(0)
        working.total_distance = dep_leg.record.distance
        working.market_distance = Distance(market_dist)

        # Push the nonstop self-connection as the first edge
        push!(working.connections, ns_cp)

        # Nonstop: departure reaches destination directly
        if dep_leg.dst === dst_stn
            _validate_and_commit!(working, ctx)
        end

        # DFS for connecting itineraries (depth 0 = first stop)
        _dfs!(dst_stn, working, dep_leg, ctx, 0)

        # Pop initial connection (pair with push above)
        pop!(working.connections)
    end

    return ctx.results
end

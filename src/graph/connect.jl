# src/graph/connect.jl — O(n²) connection builder for the flight graph
#
# For each station, every arrival leg is paired with every departure leg.
# The rule chain is run on each candidate pair; pairs that pass are stored
# as GraphConnection edges on the station, from_leg.connect_to, and
# to_leg.connect_from.  Nonstop self-connections (from_leg === to_leg) are
# created first and bypass the rule chain.

# ── Internal helpers ──────────────────────────────────────────────────────────

# Per-leg codeshare predicate used by `_set_connection_status!`.  Returns
# true when this leg's marketing carrier+flight differs from its operating
# carrier+flight on the same record (after host-flight resolution: an empty
# operating_carrier or 0 operating_flight_number means "operating IS
# marketing", per the SSIM convention).
@inline function _leg_is_codeshare(rec)::Bool
    op_c = rec.operating_carrier == NO_AIRLINE ? rec.carrier : rec.operating_carrier
    op_f = rec.operating_flight_number == NO_FLIGHTNO ? rec.flight_number : rec.operating_flight_number
    return rec.carrier != op_c || rec.flight_number != op_f
end

"""
    `function _set_connection_status!(cp::GraphConnection, arr_leg::GraphLeg, dep_leg::GraphLeg)::Nothing`
---

# Description
- Sets `STATUS_*` classification bits and the DOW intersection on `cp.status`
  in-place before the rule chain is run
- Sets `STATUS_INTERNATIONAL` when arrival and departure endpoints are in
  different non-empty countries
- Sets `STATUS_CODESHARE` when at least one of the connection's legs has
  marketing carrier+flight ≠ operating carrier+flight on its own record
  (per-leg codeshare property, rolled up).  Independent of `STATUS_INTERLINE`.
- Sets `STATUS_INTERLINE` when marketing carriers differ across the
  connection's two legs (the connection-level online↔interline distinction).
- Sets `STATUS_CNX_OP_THROUGH` when `STATUS_INTERLINE` is set AND the
  operating carriers match across the two legs (codeshare-mediated
  interline connection — same operator carries through).  Used by the rule
  chain to distinguish `INTERLINE_CODESHARE` from `INTERLINE_ALL`.
- Sets `STATUS_THROUGH` (and `cp.is_through = true`) when both legs share the
  same parent `GraphSegment` (non-zero segment hash)
- Sets `STATUS_WETLEASE` when either leg is a wet-lease operation
- ORs the 7-bit DOW intersection (arr & dep frequency bitmasks) into `cp.status`

# Arguments
1. `cp::GraphConnection`: the candidate connection (mutated)
2. `arr_leg::GraphLeg`: arriving leg of the connection
3. `dep_leg::GraphLeg`: departing leg of the connection
"""
function _set_connection_status!(
    cp::GraphConnection,
    arr_leg::GraphLeg,
    dep_leg::GraphLeg,
)::Nothing
    # International connection = MCT status is anything but DD.  Equivalent
    # to "either the arriving leg's arrival is international or the
    # departing leg's departure is international" — the per-leg intl/dom
    # flags written by the SSIM ingest carry exactly this distinction.
    # Previously this checked country equality of the OD endpoints (arr.org
    # vs dep.dst), which incorrectly classified itineraries like
    # DFW → CDG → IAH (US→FR→US, both intl legs) as domestic because the
    # outermost endpoints were in the same country.
    if arr_leg.record.arr_intl_dom == 'I' || dep_leg.record.dep_intl_dom == 'I'
        cp.status |= STATUS_INTERNATIONAL
    end

    # Per-leg codeshare (rolled up onto the connection): set STATUS_CODESHARE
    # when either of the connection's two legs has marketing carrier+flight
    # differing from operating carrier+flight on its own record.  Independent
    # of the interline check below — a leg can be a codeshare inside an
    # online itinerary.
    if _leg_is_codeshare(arr_leg.record) || _leg_is_codeshare(dep_leg.record)
        cp.status |= STATUS_CODESHARE
    end

    # Connection-level interline: marketing carriers differ across the two
    # legs.  This is the online-vs-interline distinction at the cnx level.
    # When it's set, also check the operating carriers — if they're the same
    # (codeshare-mediated transition), set STATUS_CNX_OP_THROUGH so the
    # rule chain can distinguish `INTERLINE_CODESHARE` (allow op-through)
    # from `INTERLINE_ALL` (allow op-different too).
    if arr_leg.record.carrier != dep_leg.record.carrier
        cp.status |= STATUS_INTERLINE
        arr_op = arr_leg.record.operating_carrier != NO_AIRLINE ?
            arr_leg.record.operating_carrier : arr_leg.record.carrier
        dep_op = dep_leg.record.operating_carrier != NO_AIRLINE ?
            dep_leg.record.operating_carrier : dep_leg.record.carrier
        if arr_op == dep_op
            cp.status |= STATUS_CNX_OP_THROUGH
        end
    end

    # Through-flight: same GraphSegment object (pointer identity) and non-zero hash
    if arr_leg.segment === dep_leg.segment &&
       arr_leg.segment.record.segment_hash != UInt64(0)
        cp.status |= STATUS_THROUGH
        cp.is_through = true
    end

    # Wet lease: either leg is wet-leased
    if arr_leg.record.wet_lease || dep_leg.record.wet_lease
        cp.status |= STATUS_WETLEASE
    end

    # DOW intersection: AND of the two 7-bit frequency bitmasks
    # Stored in cp.valid_days (canonical source) and ORed into cp.status for fast checks
    dow_intersection = arr_leg.record.frequency & dep_leg.record.frequency
    cp.valid_days = dow_intersection
    cp.status |= StatusBits(dow_intersection) & DOW_MASK

    return nothing
end

"""
    `function _set_validity_window!(cp::GraphConnection, arr_leg::GraphLeg, dep_leg::GraphLeg)::Nothing`
---

# Description
- Computes the schedule-level validity intersection of two legs and stores the
  result on `cp`
- `valid_from` = max of the two effective dates; `valid_to` = min of the two
  discontinue dates (both packed YYYYMMDD)
- `valid_days` = bitwise AND of the two 7-bit frequency bitmasks
- `num_valid_dates` is set to the number of set bits in `valid_days` as a
  rough per-week estimate (0 when the window is empty)

# Arguments
1. `cp::GraphConnection`: the candidate connection (mutated)
2. `arr_leg::GraphLeg`: arriving leg of the connection
3. `dep_leg::GraphLeg`: departing leg of the connection
"""
function _set_validity_window!(
    cp::GraphConnection,
    arr_leg::GraphLeg,
    dep_leg::GraphLeg,
)::Nothing
    cp.valid_from = max(arr_leg.record.effective_date, dep_leg.record.effective_date)
    cp.valid_to   = min(arr_leg.record.discontinue_date, dep_leg.record.discontinue_date)
    # cp.valid_days is already set by _set_connection_status! (single source of truth)
    if cp.valid_from <= cp.valid_to
        cp.num_valid_dates = Int16(max(1, count_ones(cp.valid_days)))
    else
        cp.num_valid_dates = Int16(0)
    end
    return nothing
end

# ── Station-level builder ─────────────────────────────────────────────────────

"""
    `function build_connections_at_station!(station::GraphStation, rules::Vector{Any}, ctx)::Nothing`
---

# Description
- Builds all valid connections at a single station
- Step 1: creates nonstop self-connections for every departure leg (bypasses
  the rule chain); these represent nonstop flights for DFS traversal
- Step 2: O(n²) pairs every arrival with every departure; skips self-references
  where `arr_leg === dep_leg`
- For each candidate pair: sets status bits and validity window, skips pairs
  with an empty validity window, runs the rule chain (short-circuits on first
  failure), and stores accepted connections on the station,
  `arr_leg.connect_to`, and `dep_leg.connect_from`
- Accumulates `StationStats` in-place on `station.stats`
- The rule chain must be built with `build_cnx_rules` or an equivalent
  compatible callable vector; `ctx` must expose at minimum
  `ctx.config::SearchConfig`, `ctx.constraints::SearchConstraints`, and
  `ctx.gc_cache::Dict{UInt64, Float64}`

# Arguments
1. `station::GraphStation`: airport node to process (mutated)
2. `rules::Vector{Any}`: ordered rule chain; each element is callable as
   `rule(cp::GraphConnection, ctx) -> Int`
3. `ctx`: runtime context passed through to each rule
"""
function build_connections_at_station!(
    station::GraphStation,
    rules,
    ctx,
)::Nothing
    arrivals   = station.arrivals
    departures = station.departures
    n_arr = length(arrivals)
    n_dep = length(departures)

    # Reset station counts (distances and avg are accumulated below)
    stats = station.stats
    stats.num_departures = Int32(n_dep)
    stats.num_arrivals   = Int32(n_arr)
    @debug "Station pairing" station=String(station.code) arrivals=n_arr departures=n_dep

    # ── Step 1: nonstop self-connections for each departure ───────────────────
    for i in 1:n_dep
        dep_leg = departures[i]
        ns_cp = nonstop_connection(dep_leg, station)
        dep_leg.nonstop_cp = ns_cp  # direct field access avoids O(n) scan in search
        push!(station.connections, ns_cp)
        stats.num_nonstops += Int32(1)
        # Track distance and equipment for departures
        stats.total_dep_distance += Float64(dep_leg.distance)
        push!(stats.unique_equipment, dep_leg.record.aircraft_type)
    end

    # Track arrival distances
    for i in 1:n_arr
        arr_leg = arrivals[i]
        stats.total_arr_distance += Float64(arr_leg.distance)
    end

    # Early exit: nothing to pair
    (n_arr == 0 || n_dep == 0) && return nothing

    # ── Step 2: O(n^2) pairing ────────────────────────────────────────────────
    for i in 1:n_arr
        arr_leg = arrivals[i]

        for j in 1:n_dep
            dep_leg = departures[j]

            # Skip self-connection (nonstops already created above)
            arr_leg === dep_leg && continue

            stats.num_pairs_evaluated += Int32(1)

            # Build candidate connection
            cp = GraphConnection(
                from_leg=arr_leg,
                to_leg=dep_leg,
                station=station,
            )

            # Set status bits BEFORE running rules so rules can read them
            _set_connection_status!(cp, arr_leg, dep_leg)

            # Compute validity window
            _set_validity_window!(cp, arr_leg, dep_leg)

            # Skip if validity window is empty
            cp.valid_from > cp.valid_to && continue
            cp.valid_days == UInt8(0) && continue

            # Run rule chain — short-circuit on first failure
            passed = true
            for k in 1:length(rules)
                rc = rules[k](cp, ctx)
                if rc <= 0
                    if length(ctx.build_stats.rule_fail) >= k
                        ctx.build_stats.rule_fail[k] += 1
                    end
                    passed = false
                    break
                else
                    if length(ctx.build_stats.rule_pass) >= k
                        ctx.build_stats.rule_pass[k] += 1
                    end
                end
            end

            passed || continue

            # Store accepted connection
            push!(station.connections, cp)
            push!(arr_leg.connect_to, cp)
            push!(dep_leg.connect_from, cp)

            # Accumulate connection count first (used in running average below)
            stats.num_connections += Int32(1)

            # Classification counters
            if is_international(cp.status)
                stats.num_international += Int32(1)
            else
                stats.num_domestic += Int32(1)
            end

            if is_interline(cp.status)
                stats.num_interline += Int32(1)
            elseif is_codeshare(cp.status)
                stats.num_codeshare += Int32(1)
            else
                stats.num_online += Int32(1)
            end

            if cp.is_through
                stats.num_through += Int32(1)
            end

            # Carrier and equipment tracking
            push!(stats.unique_carriers, arr_leg.record.carrier)
            push!(stats.unique_carriers, dep_leg.record.carrier)
            push!(stats.unique_equipment, arr_leg.record.aircraft_type)
            push!(stats.unique_equipment, dep_leg.record.aircraft_type)

            # Running weighted average ground time (cnx_time set by MCTRule)
            n = Int(stats.num_connections)
            stats.avg_ground_time =
                stats.avg_ground_time * (n - 1) / n + Float64(cp.cnx_time) / n
            @debug "Connection accepted" from_org=String(arr_leg.record.departure_station) to_dst=String(dep_leg.record.arrival_station) cnx_time=cp.cnx_time mct=cp.mct
        end
    end

    return nothing
end

# ── Graph-level builder ───────────────────────────────────────────────────────

"""
    `function build_connections!(stations::Dict{StationCode, GraphStation}, rules::Vector{Any}, ctx)::Nothing`
---

# Description
- Iterates every station in the graph and calls
  `build_connections_at_station!` for each one
- Single-threaded implementation; threading will be added in a later task once
  the `RuntimeContext` type is introduced and thread-safety requirements are
  established
- `stations` is mutated in-place; `rules` and `ctx` are read-only from the
  perspective of this function (individual rules may mutate `ctx` fields such
  as `gc_cache`)

# Arguments
1. `stations::Dict{StationCode, GraphStation}`: map of all station nodes in
   the graph (mutated via `build_connections_at_station!`)
2. `rules::Vector{Any}`: ordered connection rule chain produced by
   `build_cnx_rules`
3. `ctx`: runtime context passed through to each rule; must expose at minimum
   `ctx.config::SearchConfig`, `ctx.constraints::SearchConstraints`, and
   `ctx.gc_cache::Dict{UInt64, Float64}`

# Examples
```julia
julia> rules = build_cnx_rules(SearchConfig(), SearchConstraints(), MCTLookup());
julia> ctx = (config=SearchConfig(), constraints=SearchConstraints(),
              build_stats=BuildStats(rule_pass=zeros(Int64,9), rule_fail=zeros(Int64,9)),
              mct_cache=Dict{UInt64,MCTResult}(), gc_cache=Dict{UInt64,Float64}());
julia> build_connections!(stations, rules, ctx);
```
"""
function build_connections!(
    stations::Dict{StationCode, GraphStation},
    rules,
    ctx,
)::Nothing
    for (_, station) in stations
        build_connections_at_station!(station, rules, ctx)
    end
    return nothing
end

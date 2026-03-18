# src/graph/builder.jl — FlightGraph container and build_graph! pipeline orchestrator
#
# FlightGraph is the top-level in-memory representation of the flight network.
# build_graph! queries DuckDB for schedule-level leg data, materialises all
# graph structs, resolves codeshare identities, builds the MCT lookup, runs
# the O(n²) connection builder, and returns a ready-to-search FlightGraph.
#
# Dependency order:
#   FlightGraph   — embeds MCTLookup, BuildStats, SearchConfig
#   build_graph!  — queries store, calls materialize_mct_lookup,
#                   build_cnx_rules, build_connections!, RuntimeContext
#   search        — convenience wrapper: build_graph! + search_itineraries

using UUIDs

using UUIDs

# ── FlightGraph ───────────────────────────────────────────────────────────────

"""
    mutable struct FlightGraph
---

# Description
- Top-level container for the materialised flight network
- Built by `build_graph!`; holds all stations, legs, segments, connections,
  MCT lookup, and build metadata
- Immutable in practice once built — safe to share across threads for read-only
  search access; mutate only during the build phase

# Fields
- `stations::Dict{StationCode, GraphStation}` — map of IATA code → airport node
- `segments::Dict{UInt64, GraphSegment}` — map of segment hash → segment node
- `legs::Vector{GraphLeg}` — all flight leg nodes in insertion order
- `window_start::Date` — first date of the schedule window (inclusive)
- `window_end::Date` — last date of the schedule window (inclusive)
- `mct_lookup::MCTLookup` — in-memory SSIM8 MCT cascade structure
- `build_id::UUID` — unique identifier for this build instance
- `build_stats::BuildStats` — per-build instrumentation accumulator
- `config::SearchConfig` — snapshot of the config used to build this graph
"""
@kwdef mutable struct FlightGraph
    stations::Dict{StationCode,GraphStation} = Dict{StationCode,GraphStation}()
    segments::Dict{UInt64,GraphSegment} = Dict{UInt64,GraphSegment}()
    legs::Vector{GraphLeg} = GraphLeg[]

    # Schedule window
    window_start::Date = Date(2000, 1, 1)
    window_end::Date = Date(2000, 1, 1)

    # MCT lookup
    mct_lookup::MCTLookup = MCTLookup()

    # Build metadata
    build_id::UUID = uuid4()
    build_stats::BuildStats = BuildStats()

    # Configuration snapshot
    config::SearchConfig = SearchConfig()
end

# ── Codeshare resolution helper ───────────────────────────────────────────────

"""
    `function _resolve_codeshare!(seg::GraphSegment)::Nothing`

Resolve the operating carrier for `seg` by inspecting its constituent legs.

If any leg has a non-empty `codeshare_airline`, the segment is a codeshare:
`operating_airline` and `operating_flt_no` are set from the first such leg and
`is_codeshare` is set to `true`.

When no codeshare is present the operating carrier equals the marketing carrier
from the first leg.
"""
function _resolve_codeshare!(seg::GraphSegment)::Nothing
    for leg_any in seg.legs
        leg = leg_any::GraphLeg
        if leg.record.codeshare_airline != NO_AIRLINE
            seg.operating_airline = leg.record.codeshare_airline
            seg.operating_flt_no = leg.record.codeshare_flt_no
            seg.is_codeshare = true
            return nothing
        end
    end
    # No codeshare — operating carrier is the marketing carrier
    if !isempty(seg.legs)
        first_leg = seg.legs[1]::GraphLeg
        seg.operating_airline = first_leg.record.airline
        seg.operating_flt_no = first_leg.record.flt_no
    end
    return nothing
end

# ── build_graph! ──────────────────────────────────────────────────────────────

"""
    `function build_graph!(store::DuckDBStore, config::SearchConfig, target_date::Date)::FlightGraph`
---

# Description
- Materialise the flight network from DuckDB into an in-memory `FlightGraph`
- Pipeline:
  1. Compute schedule window from `target_date` ± `config.leading_days` /
     `config.trailing_days`
  2. Query schedule-level legs from DuckDB (`query_schedule_legs`)
  3. Create `GraphStation` nodes (one per unique station code); populate from the
     `stations` reference table when available, otherwise create minimal nodes
  4. Create `GraphLeg` edges; link to origin and destination stations
  5. Group legs into `GraphSegment` nodes by `segment_hash`; link legs to segments
  6. Resolve codeshare / operating carrier per segment (`_resolve_codeshare!`)
  7. Materialise `MCTLookup` from the DuckDB `mct` table
     (`materialize_mct_lookup`)
  8. Build connection graph via O(n²) rule-chain pass (`build_connections!`)
  9. Assemble and return the populated `FlightGraph`

# Arguments
1. `store::DuckDBStore`: populated DuckDB store (must have legs loaded)
2. `config::SearchConfig`: search configuration (window sizes, rule parameters)
3. `target_date::Date`: centre date of the schedule window

# Returns
- `::FlightGraph`: fully built and connected flight network graph

# Examples
```julia
julia> store = DuckDBStore();
julia> load_schedule!(store, SearchConfig());
julia> graph = build_graph!(store, SearchConfig(), Date(2026, 6, 15));
julia> length(graph.stations) > 0
true
```
"""
function build_graph!(
    store::DuckDBStore,
    config::SearchConfig,
    target_date::Date,
)::FlightGraph
    t0 = time_ns()

    # 1. Schedule window
    window_start = target_date - Day(config.leading_days)
    window_end = target_date + Day(config.trailing_days)

    @info "Building graph" window_start window_end target_date

    # 2. Query schedule-level legs
    leg_records = query_schedule_legs(store, window_start, window_end)
    @info "Loaded schedule legs" count = length(leg_records)

    # 3. Create stations — one per unique code
    stations = Dict{StationCode,GraphStation}()
    for rec in leg_records
        for code in (rec.org, rec.dst)
            if !haskey(stations, code)
                stn_rec = query_station(store, code)
                if stn_rec !== nothing
                    stations[code] = GraphStation(stn_rec)
                else
                    stations[code] = GraphStation(code = code)
                end
            end
        end
    end
    @info "Created stations" count = length(stations)

    # 4. Create GraphLegs and link to stations
    legs = GraphLeg[]
    sizehint!(legs, length(leg_records))
    for rec in leg_records
        org_stn = stations[rec.org]
        dst_stn = stations[rec.dst]
        leg = GraphLeg(rec, org_stn, dst_stn)
        push!(legs, leg)
        push!(org_stn.departures, leg)
        push!(dst_stn.arrivals, leg)
    end
    @info "Created legs" count = length(legs)

    # 5. Create segments and link legs
    segments = Dict{UInt64,GraphSegment}()
    for leg in legs
        hash_val = leg.record.segment_hash
        # segment_hash == 0 at schedule level (no EDF join); group by (airline, flt_no)
        # using a stable key derived from the leg identity instead
        seg_key = if hash_val != UInt64(0)
            hash_val
        else
            # Synthesise a key from the flight identity: airline + flt_no + itin_var + svc_type
            hash(
                leg.record.airline,
                hash(
                    leg.record.flt_no,
                    hash(leg.record.itin_var, hash(leg.record.svc_type)),
                ),
            )
        end

        if !haskey(segments, seg_key)
            seg_rec = SegmentRecord(
                segment_hash = seg_key,
                airline = leg.record.airline,
                flt_no = leg.record.flt_no,
                op_suffix = leg.record.operational_suffix,
                itin_var = leg.record.itin_var,
                itin_var_overflow = leg.record.itin_var_overflow,
                svc_type = leg.record.svc_type,
                operating_date = leg.record.eff_date,
                num_legs = UInt8(0),
                first_leg_seq = leg.record.leg_seq,
                last_leg_seq = leg.record.leg_seq,
                segment_org = leg.record.org,
                segment_dst = leg.record.dst,
                flown_distance = leg.record.distance,
                market_distance = Distance(0),
                segment_circuity = Float32(0),
                segment_pax_dep = leg.record.pax_dep,
                segment_pax_arr = leg.record.pax_arr,
                segment_ac_dep = leg.record.ac_dep,
                segment_ac_arr = leg.record.ac_arr,
            )
            seg = GraphSegment(record = seg_rec)
            segments[seg_key] = seg
        end

        seg = segments[seg_key]
        push!(seg.legs, leg)
        leg.segment = seg
    end

    # 6. Resolve codeshare per segment and update num_legs
    for (_, seg) in segments
        seg.record = SegmentRecord(
            segment_hash = seg.record.segment_hash,
            airline = seg.record.airline,
            flt_no = seg.record.flt_no,
            op_suffix = seg.record.op_suffix,
            itin_var = seg.record.itin_var,
            itin_var_overflow = seg.record.itin_var_overflow,
            svc_type = seg.record.svc_type,
            operating_date = seg.record.operating_date,
            num_legs = UInt8(length(seg.legs)),
            first_leg_seq = seg.record.first_leg_seq,
            last_leg_seq = seg.record.last_leg_seq,
            segment_org = seg.record.segment_org,
            segment_dst = seg.record.segment_dst,
            flown_distance = seg.record.flown_distance,
            market_distance = seg.record.market_distance,
            segment_circuity = seg.record.segment_circuity,
            segment_pax_dep = seg.record.segment_pax_dep,
            segment_pax_arr = seg.record.segment_pax_arr,
            segment_ac_dep = seg.record.segment_ac_dep,
            segment_ac_arr = seg.record.segment_ac_arr,
        )
        _resolve_codeshare!(seg)
    end
    @info "Created segments" count = length(segments)

    # 7. Materialise MCT lookup
    active_stations = Set{StationCode}(keys(stations))
    mct_lookup = materialize_mct_lookup(store, active_stations)
    @info "Materialised MCT lookup" stations_with_mct = length(mct_lookup.stations)

    # 8. Build connections
    constraints = SearchConstraints()
    cnx_rules = build_cnx_rules(config, constraints, mct_lookup)
    itn_rules = build_itn_rules(config)

    n_rules = length(cnx_rules)
    ctx = RuntimeContext(
        config = config,
        constraints = constraints,
        cnx_rules = cnx_rules,
        itn_rules = itn_rules,
        build_stats = BuildStats(
            rule_pass = zeros(Int64, n_rules),
            rule_fail = zeros(Int64, n_rules),
        ),
    )

    build_connections!(stations, cnx_rules, ctx)

    total_connections =
        sum(stn.stats.num_connections for (_, stn) in stations; init = Int32(0))
    @info "Built connections" total = total_connections

    # 9. Assemble FlightGraph
    build_time = time_ns() - t0

    ctx.build_stats.total_stations = Int32(length(stations))
    ctx.build_stats.total_legs = Int32(length(legs))
    ctx.build_stats.total_segments = Int32(length(segments))
    ctx.build_stats.total_connections = Int32(total_connections)
    ctx.build_stats.build_time_ns = build_time

    graph = FlightGraph(
        stations = stations,
        segments = segments,
        legs = legs,
        window_start = window_start,
        window_end = window_end,
        mct_lookup = mct_lookup,
        build_stats = ctx.build_stats,
        config = config,
    )

    @info "Graph built" build_time_ms = round(build_time / 1.0e6; digits = 1)

    return graph
end

# ── Convenience search ─────────────────────────────────────────────────────────

"""
    `function search(store::DuckDBStore, origin::StationCode, dest::StationCode, target_date::Date; config::SearchConfig=SearchConfig())::Vector{Itinerary}`
---

# Description
- One-shot convenience wrapper: build graph then search
- Calls `build_graph!` with `target_date`, then `search_itineraries`
- For repeated searches over the same network, build the graph once and call
  `search_itineraries` directly — avoids redundant materialisation

# Arguments
1. `store::DuckDBStore`: populated DuckDB store
2. `origin::StationCode`: IATA code of the departure airport
3. `dest::StationCode`: IATA code of the arrival airport
4. `target_date::Date`: the target travel date

# Keyword Arguments
- `config::SearchConfig=SearchConfig()`: search configuration

# Returns
- `::Vector{Itinerary}`: all valid itineraries found (deep-copied from the
  search context; safe to retain after the function returns)

# Examples
```julia
julia> store = DuckDBStore();
julia> load_schedule!(store, SearchConfig());
julia> itns = search(store, StationCode("ORD"), StationCode("LHR"), Date(2026, 6, 15));
julia> itns isa Vector{Itinerary}
true
```
"""
function search(
    store::DuckDBStore,
    origin::StationCode,
    dest::StationCode,
    target_date::Date;
    config::SearchConfig = SearchConfig(),
)::Vector{Itinerary}
    graph = build_graph!(store, config, target_date)

    ctx = RuntimeContext(
        config = config,
        constraints = SearchConstraints(),
        itn_rules = build_itn_rules(config),
    )

    results = search_itineraries(graph.stations, origin, dest, target_date, ctx)
    # Return a copy — ctx.results is reused on subsequent searches
    return copy(results)
end

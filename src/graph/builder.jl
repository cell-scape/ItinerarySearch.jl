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

# ── GeoStats ──────────────────────────────────────────────────────────────────

"""
    const GeoStats

NamedTuple of four `Dict{InlineString3, StationStats}` grouping station-level
stats by metro area, state, country, and IATA region.
"""
const GeoStats = NamedTuple{
    (:by_metro, :by_state, :by_country, :by_region),
    NTuple{4,Dict{InlineString3,StationStats}},
}

"""
    `function aggregate_geo_stats(stations::Dict{StationCode, GraphStation})::GeoStats`
---

# Description
- Single-pass aggregation of per-station `StationStats` grouped by four
  geographic levels: metro area, state, country, and IATA region
- For each station, its `StationStats` is merged into the corresponding
  group accumulator via `merge_station_stats!`
- Empty geographic fields (`InlineString3("")`) are skipped for that level

# Arguments
1. `stations::Dict{StationCode, GraphStation}`: all stations in the graph

# Returns
- `::GeoStats`: NamedTuple with four `Dict{InlineString3, StationStats}`
"""
function aggregate_geo_stats(stations::Dict{StationCode,GraphStation})::GeoStats
    by_metro = Dict{InlineString3,StationStats}()
    by_state = Dict{InlineString3,StationStats}()
    by_country = Dict{InlineString3,StationStats}()
    by_region = Dict{InlineString3,StationStats}()

    empty_code = InlineString3("")

    for (_, stn) in stations
        rec = stn.record
        if rec.city != empty_code
            acc = get!(by_metro, rec.city) do
                StationStats()
            end
            merge_station_stats!(acc, stn.stats)
        end
        if rec.state != empty_code
            acc = get!(by_state, rec.state) do
                StationStats()
            end
            merge_station_stats!(acc, stn.stats)
        end
        if rec.country != empty_code
            acc = get!(by_country, rec.country) do
                StationStats()
            end
            merge_station_stats!(acc, stn.stats)
        end
        if rec.region != empty_code
            acc = get!(by_region, rec.region) do
                StationStats()
            end
            merge_station_stats!(acc, stn.stats)
        end
    end

    return (
        by_metro = by_metro,
        by_state = by_state,
        by_country = by_country,
        by_region = by_region,
    )
end

# ── FlightGraph ───────────────────────────────────────────────────────────────

"""
    mutable struct FlightGraph
---

# Description
- Top-level container for the materialised flight network
- Built by `build_graph!`; holds all stations, legs, segments, connections,
  MCT lookup, build metadata, and geographic stats aggregations
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
- `geo_stats::GeoStats` — station stats aggregated by metro, state, country, and IATA region
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

    # Geographic stats aggregation
    geo_stats::GeoStats = (
        by_metro = Dict{InlineString3,StationStats}(),
        by_state = Dict{InlineString3,StationStats}(),
        by_country = Dict{InlineString3,StationStats}(),
        by_region = Dict{InlineString3,StationStats}(),
    )
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
    for leg_node in seg.legs
        leg = leg_node::GraphLeg  # legs is Vector{AbstractGraphNode}
        if leg.record.operating_carrier != NO_AIRLINE
            seg.operating_airline = leg.record.operating_carrier
            seg.operating_flt_no = leg.record.operating_flight_number
            seg.is_codeshare = true
            return nothing
        end
    end
    # No codeshare — operating carrier is the marketing carrier
    if !isempty(seg.legs)
        first_leg = seg.legs[1]::GraphLeg  # legs is Vector{AbstractGraphNode}
        seg.operating_airline = first_leg.record.carrier
        seg.operating_flt_no = first_leg.record.flight_number
    end
    return nothing
end

# ── build_graph! ──────────────────────────────────────────────────────────────

"""
    `function build_graph!(store::DuckDBStore, config::SearchConfig, target_date::Date; source::Symbol=:ssim)::FlightGraph`
---

# Description
- Materialise the flight network from DuckDB into an in-memory `FlightGraph`
- Pipeline:
  1. Compute schedule window from `target_date` ± `config.leading_days` /
     `config.trailing_days`
  2. Query schedule-level legs from DuckDB (`query_schedule_legs` for `:ssim`,
     `query_newssim_legs` for `:newssim`)
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

# Keyword Arguments
- `source::Symbol=:ssim`: data source — `:ssim` for SSIM fixed-width pipeline,
  `:newssim` for denormalized CSV pipeline

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
    target_date::Date;
    source::Symbol = :ssim,
    airports_path::Union{String,Nothing} = nothing,
)::FlightGraph
    t0 = time_ns()

    # Set up structured logging (task-local — no global state mutation)
    logger = setup_logger(config)

    # Load timezone offsets for the newssim path (used to compute correct UTC
    # offsets from reference data rather than the CSV's datetime columns)
    tz_offsets = Dict{StationCode,Int16}()
    if source == :newssim
        tz_path = airports_path
        if tz_path === nothing
            # Auto-discover: check standard locations (tab-delimited and fixed-width)
            for candidate in [
                "data/input/airports_tab.txt",
                "data/input/mdstua.txt",
                "data/demo/airports_tab.txt",
                "data/demo/airports.txt",
            ]
                if isfile(candidate)
                    tz_path = candidate
                    break
                end
            end
        end
        if tz_path !== nothing && isfile(tz_path)
            tz_offsets = load_timezone_offsets(tz_path)
        end
    end

    return Logging.with_logger(logger) do

    # Set up event log
    event_log = EventLog(enabled = config.event_log_enabled)
    if event_log.enabled && !isempty(config.event_log_path)
        push!(event_log.sinks, JsonlSink(config.event_log_path))
    end
    checkpoint!(event_log)  # baseline metrics

    # 1. Schedule window
    window_start = target_date - Day(config.leading_days)
    window_end = target_date + Day(config.trailing_days)

    @info "Building graph" window_start window_end target_date
    emit!(event_log, PhaseEvent(phase = :schedule_load, action = :start))

    # 2. Query schedule-level legs
    leg_records = if source == :newssim
        query_newssim_legs(store, window_start, window_end; tz_offsets=tz_offsets)
    else
        query_schedule_legs(store, window_start, window_end)
    end
    @info "Loaded schedule legs" count = length(leg_records) source

    # 3. Create stations — one per unique code
    stations = Dict{StationCode,GraphStation}()
    for rec in leg_records
        for code in (rec.departure_station, rec.arrival_station)
            if !haskey(stations, code)
                stn_rec = if source == :newssim
                    query_newssim_station(store, code; tz_offsets=tz_offsets)
                else
                    query_station(store, code)
                end
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
        org_stn = stations[rec.departure_station]
        dst_stn = stations[rec.arrival_station]
        leg = GraphLeg(rec, org_stn, dst_stn)
        push!(legs, leg)
        push!(org_stn.departures, leg)
        push!(dst_stn.arrivals, leg)
    end
    @info "Created legs" count = length(legs)

    # 4b. Gap-fill leg distances from geodesic when record.distance == 0
    n_filled = 0
    for leg in legs
        if leg.distance == Distance(0)
            org_stn = get(stations, leg.record.departure_station, nothing)
            dst_stn = get(stations, leg.record.arrival_station, nothing)
            if org_stn !== nothing &&
               dst_stn !== nothing &&
               (org_stn.record.latitude != 0.0 || org_stn.record.longitude != 0.0) &&
               (dst_stn.record.latitude != 0.0 || dst_stn.record.longitude != 0.0)
                leg.distance = Distance(
                    _geodesic_distance(
                        config,
                        org_stn.record.latitude,
                        org_stn.record.longitude,
                        dst_stn.record.latitude,
                        dst_stn.record.longitude,
                    ),
                )
                n_filled += 1
                @debug "Gap-filled distance" org=String(leg.record.departure_station) dst=String(leg.record.arrival_station) distance=leg.distance
            end
        end
    end
    n_filled > 0 && @info "Gap-filled leg distances" count = n_filled

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
                leg.record.carrier,
                hash(
                    leg.record.flight_number,
                    hash(leg.record.itinerary_var_id, hash(leg.record.service_type)),
                ),
            )
        end

        if !haskey(segments, seg_key)
            seg_rec = SegmentRecord(
                segment_hash = seg_key,
                carrier = leg.record.carrier,
                flight_number = leg.record.flight_number,
                operational_suffix = leg.record.operational_suffix,
                itinerary_var_id = leg.record.itinerary_var_id,
                itinerary_var_overflow = leg.record.itinerary_var_overflow,
                service_type = leg.record.service_type,
                operating_date = leg.record.effective_date,
                num_legs = UInt8(0),
                first_leg_seq = leg.record.leg_sequence_number,
                last_leg_seq = leg.record.leg_sequence_number,
                segment_departure_station = leg.record.departure_station,
                segment_arrival_station = leg.record.arrival_station,
                flown_distance = leg.record.distance,
                market_distance = Distance(0),
                segment_circuity = Float32(0),
                segment_passenger_departure_time = leg.record.passenger_departure_time,
                segment_passenger_arrival_time = leg.record.passenger_arrival_time,
                segment_aircraft_departure_time = leg.record.aircraft_departure_time,
                segment_aircraft_arrival_time = leg.record.aircraft_arrival_time,
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
        r = seg.record
        seg.record = SegmentRecord(
            r.segment_hash, r.carrier, r.flight_number, r.operational_suffix,
            r.itinerary_var_id, r.itinerary_var_overflow, r.service_type, r.operating_date,
            UInt8(length(seg.legs)),  # num_legs — the only changed field
            r.first_leg_seq, r.last_leg_seq, r.segment_departure_station, r.segment_arrival_station,
            r.flown_distance, r.market_distance, r.segment_circuity,
            r.segment_passenger_departure_time, r.segment_passenger_arrival_time,
            r.segment_aircraft_departure_time, r.segment_aircraft_arrival_time,
        )
        _resolve_codeshare!(seg)
        @debug "Segment resolved" hash=seg.record.segment_hash legs=length(seg.legs) codeshare=seg.is_codeshare
    end
    @info "Created segments" count = length(segments)
    emit!(event_log, PhaseEvent(phase = :schedule_load, action = :end, elapsed_ns = time_ns() - t0))
    checkpoint!(event_log)

    # 7. Materialise MCT lookup
    emit!(event_log, PhaseEvent(phase = :mct_materialize, action = :start))
    t_mct = time_ns()
    active_stations = Set{StationCode}(keys(stations))
    constraints = SearchConstraints()
    mct_lookup = materialize_mct_lookup(store, active_stations;
                                       constraints = constraints,
                                       mct_serial_ascending = config.mct_serial_ascending)
    @info "Materialised MCT lookup" stations_with_mct = length(mct_lookup.stations)
    emit!(event_log, PhaseEvent(phase = :mct_materialize, action = :end, elapsed_ns = time_ns() - t_mct))
    checkpoint!(event_log)

    # 8. Build connections
    emit!(event_log, PhaseEvent(phase = :connection_build, action = :start))
    t_cnx = time_ns()
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
        event_log = event_log,
    )

    build_connections!(stations, cnx_rules, ctx)

    total_connections =
        sum(stn.stats.num_connections for (_, stn) in stations; init = Int32(0))
    total_pairs = sum(Int64(stn.stats.num_pairs_evaluated)
                      for (_, stn) in stations; init = Int64(0))
    ctx.build_stats.total_pairs_evaluated = total_pairs
    @info "Built connections" total = total_connections

    geo = aggregate_geo_stats(stations)
    @info "Geographic stats" metros = length(geo.by_metro) states = length(geo.by_state) countries = length(geo.by_country) regions = length(geo.by_region)
    emit!(event_log, PhaseEvent(phase = :connection_build, action = :end, elapsed_ns = time_ns() - t_cnx))
    checkpoint!(event_log)
    emit!(
        event_log,
        BuildSnapshotEvent(
            stations_processed = Int32(length(stations)),
            total_stations = Int32(length(stations)),
            stats = ctx.build_stats,
        ),
    )

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
        geo_stats = geo,
    )

    @info "Graph built" build_time_ms = round(build_time / 1.0e6; digits = 1)

    close(event_log)
    _close_logger(logger)

    return graph
    end  # Logging.with_logger
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

# ── Multi-market convenience search ───────────────────────────────────────────

"""
    `function search_markets(newssim_path::AbstractString; markets, dates, mct_path="", kwargs...)::Dict{Tuple{String,String,Date}, Vector{Itinerary}}`
---

# Description
- All-in-one convenience wrapper: ingest → build → search for every market × date
- Creates an in-memory `DuckDBStore`, ingests the NewSSIM CSV and optional MCT
  file, then iterates over every `(origin, dest, date)` combination
- The graph is rebuilt once per date (schedule window shifts with the target day)
- Returns deep-copied itineraries keyed by `(origin, dest, date)` strings, safe
  to retain indefinitely
- The store is closed automatically on return (or on error)

# Arguments
1. `newssim_path::AbstractString`: path to a NewSSIM CSV file (.csv or .csv.gz)

# Keyword Arguments
- `markets`: vector of `(origin, dest)` string pairs, e.g. `[("ORD","LHR")]`
- `dates::Union{Date, AbstractVector{Date}}`: one or more target travel dates
- `mct_path::AbstractString=""`: optional path to MCT file; omit for global defaults
- All remaining keyword arguments are forwarded to `SearchConfig()`

# Returns
- `::Dict{Tuple{String,String,Date}, Vector{Itinerary}}`: results keyed by
  `(origin_string, dest_string, date)` — index with `results["ORD","LHR",date]`

# Examples
```julia
julia> results = search_markets("data/demo/sample_newssim.csv.gz";
           markets=[("ORD","LHR"), ("DEN","LAX")],
           dates=Date(2026,2,26),
           mct_path="data/demo/mct_demo.dat",
           max_stops=2);
julia> results["ORD","LHR",Date(2026,2,26)] isa Vector{Itinerary}
true
```
"""
function search_markets(
    newssim_path::AbstractString;
    markets::AbstractVector{<:Tuple{AbstractString,AbstractString}},
    dates::Union{Date,AbstractVector{Date}},
    mct_path::AbstractString = "",
    kwargs...,
)::Dict{Tuple{String,String,Date},Vector{Itinerary}}
    config = SearchConfig(; kwargs...)
    store = DuckDBStore()

    try
        ingest_newssim!(store, newssim_path)
        if !isempty(mct_path)
            ingest_mct!(store, mct_path)
        end

        date_vec = dates isa Date ? [dates] : collect(dates)

        ctx = RuntimeContext(
            config = config,
            constraints = SearchConstraints(),
            itn_rules = build_itn_rules(config),
        )

        results = Dict{Tuple{String,String,Date},Vector{Itinerary}}()

        for target in date_vec
            graph = build_graph!(store, config, target; source = :newssim)
            for (org, dst) in markets
                origin = StationCode(org)
                dest = StationCode(dst)
                itns = search_itineraries(graph.stations, origin, dest, target, ctx)
                results[(String(org), String(dst), target)] = copy(itns)
            end
        end

        return results
    finally
        close(store)
    end
end

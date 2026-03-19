# src/graph/layer1.jl — Layer 1 one-via pre-computation (multi-threaded)
#
# Layer 1 pairs every inbound connection at a station with every outbound
# connection sharing the same transit leg, producing pre-computed two-stop
# paths (OneStopConnection) indexed by (origin, destination).
#
# Threading model: each station's work is independent, so threads write to
# private chunk vectors and the merge into graph.layer1 is single-threaded.

"""
    `function build_layer1!(graph::FlightGraph)::Nothing`
---

# Description
- Pre-computes all two-stop (one-via) paths in the flight network and stores
  them in `graph.layer1`
- Uses `Threads.@threads` to process stations in parallel; each thread writes
  to a private chunk vector, so no locking is required during the hot path
- The single-threaded merge phase inserts each `OneStopConnection` into the
  `graph.layer1` dict keyed by `(org_code, dst_code)`
- Sets `graph.layer1_built = true` when complete

# Arguments
1. `graph::FlightGraph`: a fully built graph (must have connections populated
   via `build_connections!` before calling this function)

# Returns
- `::Nothing`

# Examples
```julia
julia> build_layer1!(graph);
julia> graph.layer1_built
true
```
"""
function build_layer1!(graph::FlightGraph)::Nothing
    t0 = time_ns()
    stations = collect(values(graph.stations))
    n = length(stations)
    chunks = Vector{Vector{Tuple{StationCode,StationCode,OneStopConnection}}}(undef, n)

    Threads.@threads for i in 1:n
        chunks[i] = _build_layer1_at_station(stations[i], graph)
    end

    # Single-threaded merge — avoids concurrent Dict writes
    total = 0
    for chunk in chunks
        for (org, dst, osc) in chunk
            key = (org, dst)
            vec = get!(graph.layer1, key) do
                OneStopConnection[]
            end
            push!(vec, osc)
            total += 1
        end
    end

    graph.layer1_built = true
    elapsed_ms = round((time_ns() - t0) / 1.0e6; digits = 1)
    @info "Built Layer 1" connections = total stations = n elapsed_ms = elapsed_ms
    return nothing
end

"""
    `function _build_layer1_at_station(stn::GraphStation, graph::FlightGraph)::Vector{Tuple{StationCode, StationCode, OneStopConnection}}`
---

# Description
- Computes all two-stop paths that pass through `stn` as the intermediate
  connect point of the transit leg
- Iterates departing legs at `stn`; each departing leg `L` is the *transit
  leg* joining an arriving connection (`arr_cp` in `L.connect_from`) with a
  departing connection (`dep_cp` in `L.connect_to`)
- Self-connections (where `from_leg === to_leg`) are skipped; they represent
  nonstop legs, not genuine connecting paths
- Validity is the intersection of `arr_cp` and `dep_cp` validity windows and
  DOW bitmasks
- Round-trips (same origin and destination station) are skipped
- Circuity check: `total_distance / great_circle_distance` must not exceed
  `graph.config.circuity_factor` (skipped when `gc == 0.0`, e.g. same coords)

# Arguments
1. `stn::GraphStation`: station node to process (serves as the first connect
   point — the transit leg departs from here)
2. `graph::FlightGraph`: the owning graph (provides `config.circuity_factor`)

# Returns
- `::Vector{Tuple{StationCode, StationCode, OneStopConnection}}`: list of
  `(via_station_code, destination_code, osc)` tuples ready for insertion into
  `graph.layer1`, keyed by the transit (connect-point) station so the DFS can
  look up two-hop completions using `(current_leg.dst.code, dest.code)`
"""
function _build_layer1_at_station(
    stn::GraphStation,
    graph::FlightGraph,
)::Vector{Tuple{StationCode,StationCode,OneStopConnection}}
    results = Tuple{StationCode,StationCode,OneStopConnection}[]
    circ_limit = Float64(graph.config.circuity_factor)

    for transit_leg_any in stn.departures
        transit_leg = transit_leg_any::GraphLeg

        # connect_from on a leg = connections where this leg is to_leg
        # (connections at stn that brought traffic TO transit_leg)
        arr_cps = transit_leg.connect_from
        isempty(arr_cps) && continue

        # connect_to on a leg = connections where this leg is from_leg
        # (connections at transit_leg's destination that depart onward)
        dep_cps = transit_leg.connect_to
        isempty(dep_cps) && continue

        for arr_cp_any in arr_cps
            arr_cp = arr_cp_any::GraphConnection
            # Skip nonstop self-connections
            arr_cp.from_leg === arr_cp.to_leg && continue

            for dep_cp_any in dep_cps
                dep_cp = dep_cp_any::GraphConnection
                dep_cp.from_leg === dep_cp.to_leg && continue

                # Intersect validity windows
                vf = max(arr_cp.valid_from, dep_cp.valid_from)
                vt = min(arr_cp.valid_to, dep_cp.valid_to)
                vf > vt && continue

                # Intersect DOW bitmasks
                vd = arr_cp.valid_days & dep_cp.valid_days
                vd == 0x00 && continue

                # Total flown distance (3 legs: arr, transit, dep)
                dist =
                    arr_cp.from_leg.distance + transit_leg.distance + dep_cp.to_leg.distance

                # Skip round-trips (origin == destination)
                org_stn = arr_cp.from_leg.org
                dst_stn = dep_cp.to_leg.dst
                org_stn.code == dst_stn.code && continue

                # Circuity check against great-circle origin→destination
                gc = _haversine_distance(
                    org_stn.record.lat,
                    org_stn.record.lng,
                    dst_stn.record.lat,
                    dst_stn.record.lng,
                )
                if gc > 0.0 && Float64(dist) / gc > circ_limit
                    continue
                end

                push!(
                    results,
                    (
                        stn.code,
                        dst_stn.code,
                        OneStopConnection(
                            first = arr_cp,
                            second = dep_cp,
                            via_leg = transit_leg,
                            total_distance = dist,
                            valid_from = vf,
                            valid_to = vt,
                            valid_days = vd,
                        ),
                    ),
                )
            end
        end
    end
    return results
end

"""
    `function _is_valid_on_date(osc::OneStopConnection, target::UInt32, dow::StatusBits)::Bool`
---

# Description
- Returns `true` when a `OneStopConnection` is valid on a specific calendar
  date
- Checks that `target` falls within `[osc.valid_from, osc.valid_to]` and
  that the day-of-week bit for `dow` is set in `osc.valid_days`

# Arguments
1. `osc::OneStopConnection`: the pre-computed path to test
2. `target::UInt32`: packed YYYYMMDD date to check
3. `dow::StatusBits`: single-bit DOW mask (e.g. `DOW_MON`) for the target date

# Returns
- `::Bool`: `true` when `osc` operates on `target`
"""
@inline function _is_valid_on_date(
    osc::OneStopConnection,
    target::UInt32,
    dow::StatusBits,
)::Bool
    osc.valid_from <= target <= osc.valid_to || return false
    (StatusBits(osc.valid_days) & dow) != StatusBits(0)
end

"""
    `function _compute_fingerprint(graph::FlightGraph)::Tuple{UInt64, UInt64, UInt64}`
---

# Description
- Produces a 3-component content fingerprint for a `FlightGraph`, covering the
  schedule, MCT data, and search configuration independently
- Intended for cache-key generation and staleness detection (e.g. deciding
  whether a persisted Layer 1 index is still valid for the current graph)
- `hash()` is deterministic within a Julia session; fingerprints should not be
  persisted across process restarts without a version guard

# Arguments
1. `graph::FlightGraph`: the fully-built graph to fingerprint

# Returns
- `::Tuple{UInt64, UInt64, UInt64}`: `(schedule_hash, mct_hash, config_hash)`
  where each component independently reflects changes to its input domain

# Examples
```julia
julia> fp = _compute_fingerprint(graph);
julia> fp isa Tuple{UInt64, UInt64, UInt64}
true
```
"""
function _compute_fingerprint(graph::FlightGraph)::Tuple{UInt64,UInt64,UInt64}
    # Schedule: sorted leg row_numbers
    row_ids = sort!([leg.record.row_number for leg in graph.legs])
    schedule_hash = hash(row_ids)

    # MCT: sorted mct_ids — MCTLookup.stations is Dict{StationCode, NTuple{4, Vector{MCTRecord}}}
    mct_ids = Int32[]
    for vecs in values(graph.mct_lookup.stations)
        for vec in vecs
            for rec in vec
                push!(mct_ids, rec.mct_id)
            end
        end
    end
    sort!(mct_ids)
    mct_hash = hash(mct_ids)

    # Config: hash a tuple of the search-affecting parameters
    c = graph.config
    config_hash = hash((
        c.circuity_factor,
        c.max_stops,
        c.max_connection_minutes,
        c.scope,
        c.interline,
        c.distance_formula,
    ))

    return (schedule_hash, mct_hash, config_hash)
end

"""
    `function export_layer1!(store::DuckDBStore, graph::FlightGraph)::Nothing`
---

# Description
- Flushes the in-memory Layer 1 one-stop index from `graph.layer1` into the
  DuckDB tables `layer1_metadata` and `layer1_connections`
- Clears both tables before writing so the result is always a complete snapshot
  of the current in-memory state
- Writes one row to `layer1_metadata` recording the fingerprint hashes, the
  schedule window, the build timestamp, and the total connection count
- Writes one row per `OneStopConnection` to `layer1_connections` using a
  prepared statement for efficiency
- Logs elapsed time and total connection count via `@info`

# Arguments
1. `store::DuckDBStore`: the DuckDB-backed store (tables must already exist)
2. `graph::FlightGraph`: a fully built graph with `layer1_built == true`

# Returns
- `::Nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> build_layer1!(graph);
julia> export_layer1!(store, graph);
```
"""
function export_layer1!(store::DuckDBStore, graph::FlightGraph)::Nothing
    t0 = time_ns()
    schedule_hash, mct_hash, config_hash = _compute_fingerprint(graph)

    DBInterface.execute(store.db, "DELETE FROM layer1_metadata")
    DBInterface.execute(store.db, "DELETE FROM layer1_connections")

    total = sum(length(oscs) for (_, oscs) in graph.layer1; init = 0)

    DBInterface.execute(
        store.db,
        """
        INSERT INTO layer1_metadata VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
    """,
        [
            reinterpret(Int64, schedule_hash),
            reinterpret(Int64, mct_hash),
            reinterpret(Int64, config_hash),
            graph.window_start,
            graph.window_end,
            total,
        ],
    )

    stmt = DBInterface.prepare(
        store.db,
        """
        INSERT INTO layer1_connections VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """,
    )
    for ((via_stn, dest), oscs) in graph.layer1
        for osc in oscs
            DBInterface.execute(
                stmt,
                [
                    String(via_stn),
                    String(dest),
                    reinterpret(Int64, osc.via_leg.record.row_number),
                    reinterpret(Int64, osc.first.from_leg.record.row_number),
                    reinterpret(Int64, osc.first.to_leg.record.row_number),
                    reinterpret(Int64, osc.second.from_leg.record.row_number),
                    reinterpret(Int64, osc.second.to_leg.record.row_number),
                    Float64(osc.total_distance),
                    unpack_date(osc.valid_from),
                    unpack_date(osc.valid_to),
                    Int8(osc.valid_days),
                ],
            )
        end
    end
    DBInterface.close!(stmt)

    elapsed_ms = round((time_ns() - t0) / 1.0e6; digits = 1)
    @info "Exported Layer 1" connections = total elapsed_ms = elapsed_ms
    return nothing
end

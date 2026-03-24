# src/server.jl — REST API server for ItinerarySearch

module Server

using HTTP
using JSON3
using Dates

import ..SearchConfig, ..SearchConstraints, ..DuckDBStore, ..FlightGraph
import ..ParameterSet, ..RuntimeContext, ..StationCode, ..TripLeg
import ..build_itn_rules, ..build_graph!, ..table_stats
import ..itinerary_legs_json, ..search_trip
import ..Minutes, ..NO_MINUTES
import ..GraphLeg

"""
    mutable struct ServerState

Shared server state passed to all request handlers.
"""
mutable struct ServerState
    config::SearchConfig
    constraints::SearchConstraints
    store::DuckDBStore
    graph::FlightGraph
    graph_lock::ReentrantLock
    target_date::Date
    build_timestamp::Float64
    rebuilding::Threads.Atomic{Bool}
    start_time::Float64
end

# ── Response helpers ─────────────────────────────────────────────────────────

const _JSON_HEADERS = ["Content-Type" => "application/json"]

function _json_response(status_code::Int, data)::HTTP.Response
    body = JSON3.write(Dict("status" => "ok", "data" => data))
    HTTP.Response(status_code, _JSON_HEADERS, body)
end

function _error_response(status_code::Int, message::String)::HTTP.Response
    body = JSON3.write(Dict("status" => "error", "message" => message))
    HTTP.Response(status_code, _JSON_HEADERS, body)
end

# ── Graph snapshot ───────────────────────────────────────────────────────────

function _snapshot_graph(state::ServerState)::FlightGraph
    lock(state.graph_lock) do
        state.graph
    end
end

# ── Request constraint parsing ────────────────────────────────────────────────

"""
    `function _parse_request_constraints(body::JSON3.Object, defaults::SearchConstraints)::SearchConstraints`
---

# Description
- Build a per-request `SearchConstraints` from JSON body overrides merged with server defaults
- Extracts optional fields `max_stops`, `max_elapsed`, `max_connection`, and `circuity_factor`
- Fields absent from the body use values from `defaults.defaults`

# Arguments
1. `body::JSON3.Object`: parsed JSON request body
2. `defaults::SearchConstraints`: server-level constraint defaults

# Returns
- `::SearchConstraints`: constraints with per-request overrides applied
"""
function _parse_request_constraints(
    body::JSON3.Object,
    defaults::SearchConstraints,
)::SearchConstraints
    base = defaults.defaults

    max_stops = if haskey(body, :max_stops)
        v = body[:max_stops]
        v isa Int64 ? Int16(v) : base.max_stops
    else
        base.max_stops
    end

    max_elapsed = if haskey(body, :max_elapsed)
        v = body[:max_elapsed]
        v isa Int64 ? Int32(v) : base.max_elapsed
    else
        base.max_elapsed
    end

    max_mct_override = if haskey(body, :max_connection)
        v = body[:max_connection]
        v isa Int64 ? Minutes(v) : base.max_mct_override
    else
        base.max_mct_override
    end

    circuity_factor = if haskey(body, :circuity_factor)
        v = body[:circuity_factor]
        v isa Float64 ? v : (v isa Int64 ? Float64(v) : base.circuity_factor)
    else
        base.circuity_factor
    end

    new_ps = ParameterSet(
        min_mct_override        = base.min_mct_override,
        max_mct_override        = max_mct_override,
        circuity_factor         = circuity_factor,
        circuity_extra_miles    = base.circuity_extra_miles,
        valid_codeshare_partners = base.valid_codeshare_partners,
        valid_jv_groups         = base.valid_jv_groups,
        valid_wet_leases        = base.valid_wet_leases,
        min_leg_distance        = base.min_leg_distance,
        max_leg_distance        = base.max_leg_distance,
        min_stops               = base.min_stops,
        max_stops               = max_stops,
        min_elapsed             = base.min_elapsed,
        max_elapsed             = max_elapsed,
        min_total_distance      = base.min_total_distance,
        max_total_distance      = base.max_total_distance,
        itinerary_circuity      = base.itinerary_circuity,
        max_results             = base.max_results,
    )

    return SearchConstraints(
        defaults       = new_ps,
        overrides      = defaults.overrides,
        closed_stations = defaults.closed_stations,
        closed_markets  = defaults.closed_markets,
        delays          = defaults.delays,
        flight_delays   = defaults.flight_delays,
    )
end

# ── Trip serialization ────────────────────────────────────────────────────────

"""
    `function _trips_to_json(trips)::Vector{Dict{String,Any}}`
---

# Description
- Serialize a vector of `Trip` objects to a flat JSON-safe vector of dicts
- Avoids circular graph references by extracting only primitive summary fields
- Each trip entry includes `trip_type`, `score`, `total_elapsed`, `total_distance`,
  and a `legs` array of per-itinerary summaries

# Arguments
1. `trips`: `Vector{Trip}` from `search_trip`

# Returns
- `::Vector{Dict{String,Any}}`: JSON-safe trip representations
"""
function _trips_to_json(trips)::Vector{Dict{String,Any}}
    result = Vector{Dict{String,Any}}()
    sizehint!(result, length(trips))
    for trip in trips
        leg_summaries = Vector{Dict{String,Any}}()
        for itn in trip.itineraries
            # Determine origin and destination from connections
            if isempty(itn.connections)
                push!(leg_summaries, Dict{String,Any}(
                    "origin"          => "",
                    "destination"     => "",
                    "num_stops"       => 0,
                    "elapsed_minutes" => 0,
                    "distance_miles"  => 0,
                ))
                continue
            end

            first_cp = itn.connections[1]
            # from_leg is AbstractGraphNode; GraphLeg is always the concrete type at runtime
            first_leg_rec = (first_cp.from_leg::GraphLeg).record

            last_cp = itn.connections[end]
            last_leg_node = (last_cp.to_leg === last_cp.from_leg ?
                last_cp.from_leg : last_cp.to_leg)::GraphLeg
            last_leg_rec = last_leg_node.record

            push!(leg_summaries, Dict{String,Any}(
                "origin"          => strip(String(first_leg_rec.org)),
                "destination"     => strip(String(last_leg_rec.dst)),
                "num_stops"       => Int(itn.num_stops),
                "elapsed_minutes" => Int(itn.elapsed_time),
                "distance_miles"  => round(Float64(itn.total_distance); digits=0),
            ))
        end

        push!(result, Dict{String,Any}(
            "trip_type"     => String(trip.trip_type),
            "score"         => round(trip.score; digits=2),
            "total_elapsed" => Int(trip.total_elapsed),
            "total_distance" => round(Float64(trip.total_distance); digits=0),
            "legs"          => leg_summaries,
        ))
    end
    return result
end

# ── Endpoint handlers ─────────────────────────────────────────────────────────

"""
    `function _handle_search(req::HTTP.Request, state::ServerState)::HTTP.Response`
---

# Description
- Handle `POST /search` — search itineraries for one or more O-D pairs on given dates
- Required body fields: `origins` (array), `destinations` (array), `dates` (array of "YYYY-MM-DD")
- Optional: `max_stops`, `max_elapsed`, `max_connection`, `circuity_factor`, `cross`, `compact`

# Arguments
1. `req::HTTP.Request`: HTTP request
2. `state::ServerState`: shared server state

# Returns
- `::HTTP.Response`: 200 with nested itinerary JSON, or 4xx error
"""
function _handle_search(req::HTTP.Request, state::ServerState)::HTTP.Response
    raw = JSON3.read(String(req.body))
    raw isa JSON3.Object || return _error_response(400, "Request body must be a JSON object")
    body = raw::JSON3.Object

    # Validate required fields
    if !haskey(body, :origins)
        return _error_response(400, "Missing required field: origins")
    end
    if !haskey(body, :destinations)
        return _error_response(400, "Missing required field: destinations")
    end
    if !haskey(body, :dates)
        return _error_response(400, "Missing required field: dates")
    end

    origins_raw = body[:origins]
    dests_raw   = body[:destinations]
    dates_raw   = body[:dates]

    # Parse origins and destinations as String arrays
    origins = [String(s) for s in origins_raw]
    dests   = [String(s) for s in dests_raw]

    # Parse dates
    parsed_dates = try
        [Date(String(d)) for d in dates_raw]
    catch e
        return _error_response(400, "Invalid date format; expected YYYY-MM-DD: $(sprint(showerror, e))")
    end

    isempty(origins)      && return _error_response(400, "origins must be non-empty")
    isempty(dests)        && return _error_response(400, "destinations must be non-empty")
    isempty(parsed_dates) && return _error_response(400, "dates must be non-empty")

    cross   = get(body, :cross, false)
    compact = get(body, :compact, false)
    cross_b   = cross isa Bool ? cross : false
    compact_b = compact isa Bool ? compact : false

    constraints = _parse_request_constraints(body, state.constraints)
    graph = _snapshot_graph(state)

    ctx = RuntimeContext(
        config      = state.config,
        constraints = constraints,
        itn_rules   = build_itn_rules(state.config),
    )

    json_str = itinerary_legs_json(
        graph.stations, ctx;
        origins      = origins,
        destinations = dests,
        dates        = parsed_dates,
        cross        = cross_b,
        compact      = compact_b,
    )

    parsed = JSON3.read(json_str)
    return _json_response(200, parsed)
end

"""
    `function _handle_trip(req::HTTP.Request, state::ServerState)::HTTP.Response`
---

# Description
- Handle `POST /trip` — search multi-leg trips (outbound + return, etc.)
- Required body field: `legs` (array of objects with `origin`, `destination`, `date`)
- Optional per-leg field: `min_stay` (minutes); optional body fields: `max_trips`, `max_per_leg`

# Arguments
1. `req::HTTP.Request`: HTTP request
2. `state::ServerState`: shared server state

# Returns
- `::HTTP.Response`: 200 with trip array, or 4xx error
"""
function _handle_trip(req::HTTP.Request, state::ServerState)::HTTP.Response
    raw = JSON3.read(String(req.body))
    raw isa JSON3.Object || return _error_response(400, "Request body must be a JSON object")
    body = raw::JSON3.Object

    if !haskey(body, :legs)
        return _error_response(400, "Missing required field: legs")
    end

    legs_raw = body[:legs]

    # Validate each leg has required fields
    trip_legs = TripLeg[]
    for (i, leg_obj) in enumerate(legs_raw)
        if !haskey(leg_obj, :origin) || !haskey(leg_obj, :destination) || !haskey(leg_obj, :date)
            return _error_response(
                400,
                "Leg $i missing required fields: origin, destination, date",
            )
        end

        leg_date = try
            Date(String(leg_obj[:date]))
        catch e
            return _error_response(
                400,
                "Leg $i: invalid date format '$(leg_obj[:date])'; expected YYYY-MM-DD",
            )
        end

        min_stay_raw = get(leg_obj, :min_stay, 0)
        min_stay = min_stay_raw isa Int64 ? Int(min_stay_raw) : 0

        push!(trip_legs, TripLeg(
            origin      = StationCode(String(leg_obj[:origin])),
            destination = StationCode(String(leg_obj[:destination])),
            date        = leg_date,
            min_stay    = min_stay,
        ))
    end

    isempty(trip_legs) && return _error_response(400, "legs must be non-empty")

    max_trips_raw   = get(body, :max_trips, 100)
    max_per_leg_raw = get(body, :max_per_leg, 50)
    max_trips   = max_trips_raw isa Int64 ? Int(max_trips_raw) : 100
    max_per_leg = max_per_leg_raw isa Int64 ? Int(max_per_leg_raw) : 50

    graph = _snapshot_graph(state)

    ctx = RuntimeContext(
        config      = state.config,
        constraints = state.constraints,
        itn_rules   = build_itn_rules(state.config),
    )

    trips = search_trip(
        state.store, graph, trip_legs, ctx;
        max_trips   = max_trips,
        max_per_leg = max_per_leg,
    )

    return _json_response(200, _trips_to_json(trips))
end

"""
    `function _handle_station(req::HTTP.Request, state::ServerState, path::AbstractString)::HTTP.Response`
---

# Description
- Handle `GET /station/:code` — return reference record for a single airport
- Returns code, country, state, metro_area, region, lat, lng, utc_offset

# Arguments
1. `req::HTTP.Request`: HTTP request
2. `state::ServerState`: shared server state
3. `path::AbstractString`: request path (e.g. `/station/ORD`)

# Returns
- `::HTTP.Response`: 200 with station record, or 404 if not found
"""
function _handle_station(
    req::HTTP.Request,
    state::ServerState,
    path::AbstractString,
)::HTTP.Response
    parts = split(path, "/")
    length(parts) < 3 && return _error_response(400, "Invalid station path: $path")
    code_str = String(parts[3])
    isempty(code_str) && return _error_response(400, "Station code is empty")

    code = StationCode(code_str)
    graph = _snapshot_graph(state)
    stn = get(graph.stations, code, nothing)

    if stn === nothing
        return _error_response(404, "Station $code_str not found")
    end

    rec = stn.record
    data = Dict{String,Any}(
        "code"       => strip(String(rec.code)),
        "country"    => strip(String(rec.country)),
        "state"      => strip(String(rec.state)),
        "metro_area" => strip(String(rec.metro_area)),
        "region"     => strip(String(rec.region)),
        "lat"        => rec.lat,
        "lng"        => rec.lng,
        "utc_offset" => Int(rec.utc_offset),
    )
    return _json_response(200, data)
end

"""
    `function _handle_health(req::HTTP.Request, state::ServerState)::HTTP.Response`
---

# Description
- Handle `GET /health` — return server and graph health metrics
- Includes uptime, build timestamp, graph stats, memory RSS, schedule table counts,
  and whether a rebuild is in progress

# Arguments
1. `req::HTTP.Request`: HTTP request
2. `state::ServerState`: shared server state

# Returns
- `::HTTP.Response`: 200 with health metrics JSON
"""
function _handle_health(req::HTTP.Request, state::ServerState)::HTTP.Response
    graph = _snapshot_graph(state)
    stats = graph.build_stats

    data = Dict{String,Any}(
        "uptime_seconds"    => round(time() - state.start_time; digits=1),
        "build_timestamp"   => Dates.format(
            Dates.unix2datetime(state.build_timestamp),
            dateformat"yyyy-mm-ddTHH:MM:SS",
        ),
        "target_date"       => Dates.format(state.target_date, "yyyy-mm-dd"),
        "graph_stations"    => Int(stats.total_stations),
        "graph_legs"        => Int(stats.total_legs),
        "graph_connections" => Int(stats.total_connections),
        "build_time_ms"     => round(stats.build_time_ns / 1.0e6; digits=1),
        "memory_rss_mb"     => round(Sys.maxrss() / 1024^2; digits=1),
        "schedule_stats"    => table_stats(state.store),
        "rebuilding"        => state.rebuilding[],
    )
    return _json_response(200, data)
end

"""
    `function _handle_rebuild(req::HTTP.Request, state::ServerState)::HTTP.Response`
---

# Description
- Handle `POST /rebuild` — trigger an asynchronous graph rebuild
- Returns 409 if a rebuild is already running
- Accepts optional `date` field ("YYYY-MM-DD") to set the new target date;
  falls back to `state.target_date` if absent
- Returns immediately after spawning the background task

# Arguments
1. `req::HTTP.Request`: HTTP request
2. `state::ServerState`: shared server state

# Returns
- `::HTTP.Response`: 200 if rebuild started, 409 if already running
"""
function _handle_rebuild(req::HTTP.Request, state::ServerState)::HTTP.Response
    # Atomic compare-and-swap: false → true; returns the old value
    old_val = Threads.atomic_cas!(state.rebuilding, false, true)
    if old_val
        return _error_response(409, "Rebuild already in progress")
    end

    # Parse optional target date from body
    target = state.target_date
    if length(req.body) > 0
        raw_body = try
            JSON3.read(String(req.body))
        catch
            nothing
        end
        if raw_body isa JSON3.Object
            body_obj = raw_body::JSON3.Object
            if haskey(body_obj, :date)
                date_val = body_obj[:date]
                if date_val isa String
                    target = try
                        Date(date_val)
                    catch
                        state.target_date
                    end
                end
            end
        end
    end

    # Capture immutable snapshots for the background task
    _store  = state.store
    _config = state.config

    _target = target  # immutable local capture
    Threads.@spawn begin
        try
            new_graph = build_graph!(_store, _config, _target)
            lock(state.graph_lock) do
                state.graph           = new_graph
                state.target_date     = _target
                state.build_timestamp = time()
            end
            @info "Rebuild complete" target=_target stations=new_graph.build_stats.total_stations
        catch e
            @error "Rebuild failed" exception=(e, catch_backtrace())
        finally
            state.rebuilding[] = false
        end
    end

    return _json_response(200, Dict("message" => "rebuild started"))
end

# ── Router ───────────────────────────────────────────────────────────────────

function _handle_request(req::HTTP.Request, state::ServerState)::HTTP.Response
    try
        length(req.body) > 1_000_000 && return _error_response(413, "Request body too large")
        method = req.method
        path = split(String(req.target), "?")[1]

        if method == "POST" && path == "/search"
            return _handle_search(req, state)
        elseif method == "POST" && path == "/trip"
            return _handle_trip(req, state)
        elseif method == "GET" && startswith(path, "/station/")
            return _handle_station(req, state, path)
        elseif method == "GET" && path == "/health"
            return _handle_health(req, state)
        elseif method == "POST" && path == "/rebuild"
            return _handle_rebuild(req, state)
        else
            return _error_response(404, "Not found: $method $path")
        end
    catch e
        @error "Request handler error" exception=(e, catch_backtrace())
        return _error_response(500, sprint(showerror, e))
    end
end

# ── Server start ─────────────────────────────────────────────────────────────

"""
    `start(state::ServerState; host::String, port::Int) → nothing`
---

# Description
- Start the HTTP server in blocking mode
- Use for production deployment; control returns only when server is stopped

# Arguments
1. `state::ServerState`: shared server state passed to all request handlers

# Keyword Arguments
- `host::String="0.0.0.0"`: interface address to bind
- `port::Int=8080`: TCP port to listen on

# Returns
- `::nothing`

# Examples
```julia
julia> start(state; host="0.0.0.0", port=8080)
```
"""
function start(state::ServerState; host::String="0.0.0.0", port::Int=8080)
    @info "Starting server" host port target_date=state.target_date
    HTTP.serve(host, port) do req
        _handle_request(req, state)
    end
end

"""
    `start!(state::ServerState; host::String, port::Int) → HTTP.Server`
---

# Description
- Start the HTTP server in non-blocking mode
- Returns a server handle; call `close(server)` to stop
- Use for tests and programmatic lifecycle management

# Arguments
1. `state::ServerState`: shared server state passed to all request handlers

# Keyword Arguments
- `host::String="127.0.0.1"`: interface address to bind
- `port::Int=0`: TCP port to listen on (0 = OS-assigned ephemeral port)

# Returns
- `::HTTP.Server`: server handle

# Examples
```julia
julia> server = start!(state);
julia> close(server)
```
"""
function start!(state::ServerState; host::String="127.0.0.1", port::Int=0)
    @info "Starting server (non-blocking)" host port
    HTTP.serve!(host, port) do req
        _handle_request(req, state)
    end
end

end # module Server

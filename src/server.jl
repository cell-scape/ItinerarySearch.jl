# src/server.jl — REST API server for ItinerarySearch

module Server

using HTTP
using JSON3
using Dates

import ..SearchConfig, ..SearchConstraints, ..DuckDBStore, ..FlightGraph

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

# ── Router ───────────────────────────────────────────────────────────────────

function _handle_request(req::HTTP.Request, state::ServerState)::HTTP.Response
    try
        length(req.body) > 1_000_000 && return _error_response(413, "Request body too large")
        method = req.method
        path = split(String(req.target), "?")[1]

        if method == "POST" && path == "/search"
            return _error_response(501, "Not implemented")
        elseif method == "POST" && path == "/trip"
            return _error_response(501, "Not implemented")
        elseif method == "GET" && startswith(path, "/station/")
            return _error_response(501, "Not implemented")
        elseif method == "GET" && path == "/health"
            return _error_response(501, "Not implemented")
        elseif method == "POST" && path == "/rebuild"
            return _error_response(501, "Not implemented")
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

# src/observe/events.jl — Typed event structs for the observability subsystem

"""
    struct SystemMetricsEvent

Julia runtime and system memory snapshot captured at cooperative checkpoints.
This is the only event type that is `isbits`.

# Fields
- `timestamp::UInt64`: nanosecond timestamp from `time_ns()`
- `thread_id::Int`: thread that emitted the event
- `max_rss::UInt64`: maximum resident set size in bytes
- `gc_live_bytes::Int64`: live bytes tracked by the GC
- `gc_total_pause_ns::UInt64`: cumulative GC pause time in nanoseconds
- `gc_pause_count::Int64`: total number of GC pauses
- `total_memory::UInt64`: total system memory in bytes
- `free_memory::UInt64`: free system memory in bytes
- `julia_threads::Int`: number of Julia worker threads
- `cpu_threads::Int`: number of CPU threads available
"""
@kwdef struct SystemMetricsEvent
    timestamp::UInt64 = time_ns()
    thread_id::Int = Threads.threadid()
    max_rss::UInt64 = UInt64(0)
    gc_live_bytes::Int64 = 0
    gc_total_pause_ns::UInt64 = UInt64(0)
    gc_pause_count::Int64 = 0
    total_memory::UInt64 = UInt64(0)
    free_memory::UInt64 = UInt64(0)
    julia_threads::Int = 0
    cpu_threads::Int = 0
end

"""
    struct PhaseEvent

Marks the start or end of a named processing phase.

# Fields
- `timestamp::UInt64`: nanosecond timestamp from `time_ns()`
- `thread_id::Int`: thread that emitted the event
- `phase::Symbol`: name of the processing phase (e.g., `:ingest`, `:build`, `:search`)
- `action::Symbol`: `:start` or `:stop`
- `elapsed_ns::UInt64`: elapsed time in nanoseconds (meaningful only for `:stop` events)
"""
@kwdef struct PhaseEvent
    timestamp::UInt64 = time_ns()
    thread_id::Int = Threads.threadid()
    phase::Symbol = :unknown
    action::Symbol = :start
    elapsed_ns::UInt64 = UInt64(0)
end

"""
    struct BuildSnapshotEvent

Snapshot of connection-build progress, emitted after `build_connections!`.

# Fields
- `timestamp::UInt64`: nanosecond timestamp from `time_ns()`
- `thread_id::Int`: thread that emitted the event
- `stations_processed::Int32`: number of stations processed so far
- `total_stations::Int32`: total number of stations in the graph
- `stats::BuildStats`: accumulated build statistics
"""
@kwdef struct BuildSnapshotEvent
    timestamp::UInt64 = time_ns()
    thread_id::Int = Threads.threadid()
    stations_processed::Int32 = Int32(0)
    total_stations::Int32 = Int32(0)
    stats::BuildStats = BuildStats()
end

"""
    struct SearchSnapshotEvent

Snapshot of search progress, emitted after `search_itineraries` when `metrics_level == :full`.

# Fields
- `timestamp::UInt64`: nanosecond timestamp from `time_ns()`
- `thread_id::Int`: thread that emitted the event
- `origin::StationCode`: origin station for this search
- `destination::StationCode`: destination station for this search
- `stats::SearchStats`: accumulated search statistics
"""
@kwdef struct SearchSnapshotEvent
    timestamp::UInt64 = time_ns()
    thread_id::Int = Threads.threadid()
    origin::StationCode = StationCode("")
    destination::StationCode = StationCode("")
    stats::SearchStats = SearchStats()
end

"""
    struct CustomEvent

Escape hatch for ad-hoc diagnostic events. Not `isbits` (Dict payload).

# Fields
- `timestamp::UInt64`: nanosecond timestamp from `time_ns()`
- `thread_id::Int`: thread that emitted the event
- `name::Symbol`: event name for filtering and routing
- `message::String`: human-readable description
- `metadata::Dict{String,Any}`: arbitrary key-value payload
"""
@kwdef struct CustomEvent
    timestamp::UInt64 = time_ns()
    thread_id::Int = Threads.threadid()
    name::Symbol = :custom
    message::String = ""
    metadata::Dict{String,Any} = Dict{String,Any}()
end

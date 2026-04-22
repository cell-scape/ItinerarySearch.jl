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

"""
    `SpanEvent(; kind, name, trace_id, span_id, parent_span_id, unix_nano, ...)`
---

# Description
- OpenTelemetry-shaped span event emitted at the start and end edges of a span.
- One `:start` event and one `:end` event are emitted per span; a follow-on OTLP
  exporter plan pairs them by `span_id` and emits a single OTLP span per pair.
- `kind` is a documented invariant (`:start` or `:end`), not enforced at the type level.

# Fields
- `kind::Symbol`: `:start` or `:end` (invariant — not type-enforced)
- `name::Symbol`: span name, e.g. `:search_markets` or `:market_search`
- `trace_id::UInt128`: 128-bit trace identifier (W3C Trace Context)
- `span_id::UInt64`: 64-bit span identifier
- `parent_span_id::UInt64`: parent's `span_id`, or `0` for root spans
- `unix_nano::Int64`: nanoseconds since Unix epoch
- `worker_slot::Int = 0`: `1..nthreads` for per-market spans, `0` for root spans
- `status::Symbol = :ok`: `:ok` or `:error` (meaningful on `:end` events only)
- `attributes::Dict{Symbol,Any}`: span attributes (free-form key/value pairs)
"""
@kwdef struct SpanEvent
    kind::Symbol
    name::Symbol
    trace_id::UInt128
    span_id::UInt64
    parent_span_id::UInt64
    unix_nano::Int64
    worker_slot::Int = 0
    status::Symbol = :ok
    attributes::Dict{Symbol,Any} = Dict{Symbol,Any}()
end

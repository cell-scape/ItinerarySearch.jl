# src/observe/event_log.jl — EventLog core: emit!, checkpoint!, with_phase, close

"""
    mutable struct EventLog

Structured event log with pluggable sinks.

- `events` stores all emitted events (heterogeneous)
- `sinks` are callables with signature `sink(event) -> nothing`
- `enabled` is the master switch; when `false`, `emit!` is a no-op

Designed for single-threaded use within a RuntimeContext.

# Fields
- `events::Vector{Any}`: accumulated events in emission order
- `sinks::Vector{Any}`: callable sinks (functions or callable structs)
- `enabled::Bool`: master on/off switch
"""
@kwdef mutable struct EventLog
    events::Vector{Any} = Any[]
    sinks::Vector{Any} = Any[]
    enabled::Bool = false
end

"""
    `function emit!(log::EventLog, event)::Nothing`
---

# Description
- Emit an event to the log.
- If `log.enabled` is `false`, this is a no-op.
- Pushes `event` to `log.events` and calls each sink synchronously.

# Arguments
1. `log::EventLog`: the log to emit into
2. `event`: any typed event struct

# Returns
- `::Nothing`

# Examples
```julia
julia> log = EventLog(enabled=true);
julia> emit!(log, CustomEvent(name=:test, message="hello"));
julia> length(log.events)
1
```
"""
function emit!(log::EventLog, event)::Nothing
    log.enabled || return nothing
    push!(log.events, event)
    for sink in log.sinks
        sink(event)
    end
    return nothing
end

"""
    `function checkpoint!(log::EventLog)::Nothing`
---

# Description
- Collect system metrics and emit a `SystemMetricsEvent`.
- No-op when `log.enabled` is `false`.

# Arguments
1. `log::EventLog`: the log to checkpoint

# Returns
- `::Nothing`
"""
function checkpoint!(log::EventLog)::Nothing
    log.enabled || return nothing
    emit!(log, collect_system_metrics())
    return nothing
end

"""
    `function with_phase(f::Function, log::EventLog, phase::Symbol)`
---

# Description
- Run `f()` bracketed by `PhaseEvent` start/end emissions.
- Emits a `SystemMetricsEvent` checkpoint after the phase ends.
- When the log is disabled, `f()` still executes normally.

# Arguments
1. `f::Function`: zero-argument callable to execute
2. `log::EventLog`: the log to emit phase events into
3. `phase::Symbol`: name of the phase (e.g., `:ingest`, `:build`, `:search`)

# Returns
- The return value of `f()`

# Examples
```julia
julia> log = EventLog(enabled=true);
julia> result = with_phase(log, :build) do
           42
       end;
julia> result
42
```
"""
function with_phase(f::Function, log::EventLog, phase::Symbol)
    emit!(log, PhaseEvent(phase = phase, action = :start))
    t0 = time_ns()
    result = f()
    elapsed = time_ns() - t0
    emit!(log, PhaseEvent(phase = phase, action = :end, elapsed_ns = elapsed))
    checkpoint!(log)
    return result
end

"""
    `function Base.close(log::EventLog)::Nothing`
---

# Description
- Flush and close all sink IO handles, then clear the sinks list.
- Uses `hasproperty(sink, :io)` to detect IO-backed sinks without requiring
  a concrete type reference (works before `JsonlSink` is defined).

# Arguments
1. `log::EventLog`: the log whose sinks should be flushed and closed

# Returns
- `::Nothing`
"""
function Base.close(log::EventLog)::Nothing
    for sink in log.sinks
        if hasproperty(sink, :io) && sink.io isa IO && isopen(sink.io)
            flush(sink.io)
            close(sink.io)
        end
    end
    empty!(log.sinks)
    return nothing
end

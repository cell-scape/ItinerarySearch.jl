# src/observe/trace_context.jl — Trace/span identity and Unix-nanosecond timestamps

"""
    TraceContext(trace_id::UInt128, parent_span_id::UInt64)

Inherited context for a span. `trace_id` stays constant through a logical trace;
`parent_span_id` is `0` for root spans and the caller's `span_id` otherwise.

Follows W3C Trace Context semantics: the trace id is 128 bits and spans are
identified by 64-bit span ids. Embed a `TraceContext` in any struct that needs
to propagate trace identity across async tasks or function boundaries.

# Fields
- `trace_id::UInt128`: 128-bit trace identifier, constant for the lifetime of a trace
- `parent_span_id::UInt64`: 64-bit parent span id, `0` for root spans
"""
struct TraceContext
    trace_id::UInt128
    parent_span_id::UInt64
end

"""
    `_new_trace_id()::UInt128`
---

# Description
- Generate a fresh 128-bit trace id.
- Uses the default task-local RNG, so safe to call from multiple tasks concurrently.
- Returns a random `UInt128`; zero is possible but occurs with probability 2⁻¹²⁸.

# Returns
- `::UInt128`: random trace identifier
"""
_new_trace_id()::UInt128 = rand(UInt128)

"""
    `_new_span_id()::UInt64`
---

# Description
- Generate a fresh 64-bit span id.
- Uses the default task-local RNG, so safe to call from multiple tasks concurrently.
- Zero is possible but occurs with probability 2⁻⁶⁴.

# Returns
- `::UInt64`: random span identifier
"""
_new_span_id()::UInt64 = rand(UInt64)

# ── Unix-nanosecond timestamp ──────────────────────────────────────────────
#
# `time_ns()` is a monotonic high-resolution clock with an arbitrary epoch.
# We capture the offset between that epoch and the Unix epoch exactly once
# at module load, then add `time_ns()` to it for subsequent reads. This
# gives us nanosecond-resolution Unix timestamps with no drift during a
# search (system-clock adjustments don't affect it after init).

const _UNIX_NANO_ORIGIN = Ref{Int64}(0)

"""
    `_init_unix_nano_origin!()`
---

# Description
- Calibrates the `_UNIX_NANO_ORIGIN` offset between `time_ns()` and the Unix epoch.
- Called once from `ItinerarySearch.__init__()` at module load.
- After this call, `_unix_nano_now()` returns nanoseconds since 1970-01-01 UTC.
"""
function _init_unix_nano_origin!()
    now_ns_wall = round(Int64, Dates.datetime2unix(Dates.now(Dates.UTC)) * 1e9)
    _UNIX_NANO_ORIGIN[] = now_ns_wall - Int64(time_ns())
    return nothing
end

"""
    `_unix_nano_now()::Int64`
---

# Description
- Returns nanoseconds since the Unix epoch (1970-01-01 UTC).
- Monotonic within a process: derived from `time_ns()` plus a one-time wall-clock
  offset captured at module init by `_init_unix_nano_origin!()`.
- System-clock adjustments (NTP, DST, etc.) do not affect readings after init.

# Returns
- `::Int64`: nanoseconds since Unix epoch

# Examples
```julia
julia> t = ItinerarySearch._unix_nano_now();
julia> t > 978_307_200_000_000_000  # after 2001-01-01
true
```
"""
_unix_nano_now()::Int64 = _UNIX_NANO_ORIGIN[] + Int64(time_ns())

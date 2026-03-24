# src/observe/metrics.jl — System metrics collection

"""
    `function collect_system_metrics()::SystemMetricsEvent`
---

# Description
- Capture a snapshot of Julia runtime and system memory state.
- Pure function — no side effects.

# Returns
- `::SystemMetricsEvent`: populated metrics snapshot

# Examples
```julia
julia> evt = collect_system_metrics();
julia> evt.julia_threads == Threads.nthreads()
true
```
"""
function collect_system_metrics()::SystemMetricsEvent
    gc = Base.gc_num()
    return SystemMetricsEvent(
        max_rss = Sys.maxrss(),
        gc_live_bytes = Base.gc_live_bytes(),
        gc_total_pause_ns = gc.total_time,
        gc_pause_count = Int64(gc.pause),
        total_memory = Sys.total_memory(),
        free_memory = Sys.free_memory(),
        julia_threads = Threads.nthreads(),
        cpu_threads = Sys.CPU_THREADS,
    )
end

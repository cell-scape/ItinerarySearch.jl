"""
    MarketSearchFailure(market, exception, backtrace, worker_slot, elapsed_ms)

Sentinel returned in place of `Vector{Itinerary}` when a single market's
`search_itineraries` call raised an exception during `search_markets`.
Captures enough context to triage without replaying the search.

# Fields
- `market::Tuple{String,String,Date}`: `(origin, destination, date)` key
- `exception::Exception`: preserved exception type
- `backtrace::Vector{Base.StackTraces.StackFrame}`: stacktrace at catch site
- `worker_slot::Int`: 1..nthreads pool slot that hit the failure
- `elapsed_ms::Float64`: wall time from task start to catch
"""
struct MarketSearchFailure
    market::Tuple{String,String,Date}
    exception::Exception
    backtrace::Vector{Base.StackTraces.StackFrame}
    worker_slot::Int
    elapsed_ms::Float64
end

"""
    `is_failure(x)::Bool`

Return `true` iff `x` is a `MarketSearchFailure`. Useful for filtering
`search_markets` result dicts without pattern matching.
"""
is_failure(::MarketSearchFailure) = true
is_failure(::Any) = false

"""
    `failed_markets(d::AbstractDict)::Vector{MarketSearchFailure}`

Extract all `MarketSearchFailure` values from a `search_markets` result
dict. Returns an empty vector if there are no failures.
"""
failed_markets(d::AbstractDict) =
    MarketSearchFailure[v for v in values(d) if v isa MarketSearchFailure]

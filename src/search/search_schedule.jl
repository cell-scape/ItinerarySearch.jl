# src/search/search_schedule.jl — Schedule-wide sweeps of all valid itineraries.
#
# `search_schedule` is the headline entry point for "give me every itinerary in
# the schedule that matches my carrier filter". It owns a root `:search_schedule`
# span and delegates the per-date × per-market sweep to the existing dispatcher
# helpers in `graph/builder.jl`, passing the shared trace/span ids and results
# dict through the external-collection keyword path (added in Task 7 sub-task B).
#
# Two entry forms:
#   1. Path form — self-contained: ingests the NewSSIM CSV + optional MCT file,
#      builds the graph per date, sweeps markets, and closes the store.
#   2. Store form — takes a pre-ingested `DuckDBStore`; caller owns its lifecycle.
#
# Two universe modes:
#   :direct    — markets with a direct flight by a filter carrier (fast)
#   :connected — markets where a filter carrier's leg appears in at least one
#                valid itinerary up to `config.max_stops` deep (slower; can be
#                50× the size of :direct on a hub carrier)
#
# Optional `sink::Function` receives `(market_tuple, result)` per completion
# instead of the dict write; useful for streaming results out of a very large
# sweep without materialising the whole dict.

"""
    `search_schedule(newssim_path::AbstractString; dates, carriers=nothing, include_codeshare=false, universe=:direct, mct_path="", sink=nothing, event_sinks=Function[], kwargs...)::Dict`
---

# Description
- Schedule-wide carrier-scoped sweep of all valid itineraries.
- Self-contained form: ingests NewSSIM CSV, builds the graph per date, computes
  the market universe, and sweeps.
- `universe=:direct` — markets with a direct flight by a filter carrier (fast, small).
- `universe=:connected` — markets where a filter carrier appears as any leg of
  a valid itinerary via the connection graph (wider, slower; can be 50× the
  size of :direct on a hub carrier).
- `carriers=nothing` means no filter (all markets on the date).
- `include_codeshare=true` expands the filter set to include codeshare
  partners derived from the schedule.
- `sink::Function` receives `(market_tuple, result)` per completion; the
  returned dict is empty when `sink` is supplied.
- `event_sinks::Vector{<:Function}` receives `SpanEvent`s during the sweep,
  with `:search_schedule` as the root span name.

# Arguments
1. `newssim_path::AbstractString`: path to NewSSIM CSV (.csv or .csv.gz)

# Keyword Arguments
- `dates::Union{Date, AbstractVector{Date}}`: target operating date(s)
- `carriers::Union{Nothing, AbstractVector{<:AbstractString}}=nothing`: carrier filter
- `include_codeshare::Bool=false`: expand to schedule-derived partners
- `universe::Symbol=:direct`: `:direct` or `:connected`
- `mct_path::AbstractString=""`: optional MCT file path
- `sink::Union{Nothing, Function}=nothing`: per-market result callback
- `event_sinks::Vector{<:Function}=Function[]`: SpanEvent sinks
- All other kwargs are forwarded to `SearchConfig`

# Returns
- `::Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}`:
  empty when `sink` is supplied; otherwise keyed dict of results
"""
function search_schedule(
    newssim_path::AbstractString;
    dates::Union{Date,AbstractVector{Date}},
    carriers::Union{Nothing,AbstractVector{<:AbstractString}} = nothing,
    include_codeshare::Bool = false,
    universe::Symbol = :direct,
    mct_path::AbstractString = "",
    sink::Union{Nothing,Function} = nothing,
    event_sinks::Vector{<:Function} = Function[],
    kwargs...,
)::Dict{Tuple{String,String,Date},Union{Vector{Itinerary},MarketSearchFailure}}
    config = SearchConfig(; kwargs...)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)
        isempty(mct_path) || ingest_mct!(store, mct_path)
        return _search_schedule_sweep!(
            store, config, dates, carriers, include_codeshare,
            universe, sink, event_sinks,
        )
    finally
        close(store)
    end
end

"""
    `search_schedule(store::DuckDBStore; dates, carriers=nothing, include_codeshare=false, universe=:direct, sink=nothing, event_sinks=Function[], kwargs...)::Dict`
---

# Description
- Store-form entry point: identical semantics to the path form, but takes a
  pre-ingested `DuckDBStore` so the caller controls its lifecycle (share one
  store across many sweeps; close it yourself when done).
- No `mct_path` argument — ingest MCT into the store yourself before calling.

# Arguments
1. `store::DuckDBStore`: pre-ingested data store (NewSSIM + optional MCT)

# Keyword Arguments
- Identical to the path form (minus `mct_path`).

# Returns
- `::Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}`
"""
function search_schedule(
    store::DuckDBStore;
    dates::Union{Date,AbstractVector{Date}},
    carriers::Union{Nothing,AbstractVector{<:AbstractString}} = nothing,
    include_codeshare::Bool = false,
    universe::Symbol = :direct,
    sink::Union{Nothing,Function} = nothing,
    event_sinks::Vector{<:Function} = Function[],
    kwargs...,
)::Dict{Tuple{String,String,Date},Union{Vector{Itinerary},MarketSearchFailure}}
    config = SearchConfig(; kwargs...)
    return _search_schedule_sweep!(
        store, config, dates, carriers, include_codeshare,
        universe, sink, event_sinks,
    )
end

"""
    `_search_schedule_sweep!(store, config, dates, carriers, include_codeshare, universe, sink, event_sinks)::Dict`
---

# Description
- Internal sweep helper shared by both `search_schedule` entry forms.
- Owns the root `:search_schedule` span; the dispatcher helpers are invoked
  with `external_trace_id` + `external_root_span_id` so per-market spans nest
  correctly and the dispatcher does not emit its own `:search_markets` root.
- Per-date flow:
  1. Build the graph (`build_graph!` with `source=:newssim`)
  2. Pre-expand the carrier filter (codeshare partners, if requested)
  3. Enumerate the market universe — `:direct` via SQL or `:connected` via BFS
     on the live graph
  4. Dispatch to sequential or parallel helper based on
     `config.parallel_markets && Threads.nthreads() > 1`
- When `sink` is supplied the shared results dict stays empty (writes go to
  `sink`). In that case success/failure counts are emitted as `-1` in the
  root-span :end attributes to signal "not counted".

# Arguments
1. `store::DuckDBStore`: pre-ingested store (newssim + optional MCT)
2. `config::SearchConfig`: shared search configuration
3. `dates::Union{Date, AbstractVector{Date}}`: target dates (single or vector)
4. `carriers::Union{Nothing, AbstractVector{<:AbstractString}}`: carrier filter
5. `include_codeshare::Bool`: expand to codeshare partners
6. `universe::Symbol`: `:direct` or `:connected`
7. `sink::Union{Nothing, Function}`: per-market callback or `nothing`
8. `event_sinks::Vector{<:Function}`: SpanEvent sinks

# Returns
- `::Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}`

# Throws
- `ArgumentError`: when `universe` is not `:direct` or `:connected`
"""
function _search_schedule_sweep!(
    store::DuckDBStore,
    config::SearchConfig,
    dates::Union{Date,AbstractVector{Date}},
    carriers::Union{Nothing,AbstractVector{<:AbstractString}},
    include_codeshare::Bool,
    universe::Symbol,
    sink::Union{Nothing,Function},
    event_sinks::Vector{<:Function},
)::Dict{Tuple{String,String,Date},Union{Vector{Itinerary},MarketSearchFailure}}
    universe ∈ (:direct, :connected) || throw(ArgumentError(
        "unknown universe mode: $universe. Valid: :direct, :connected",
    ))

    date_vec = dates isa Date ? [dates] : collect(dates)

    # Root-span ids for the whole sweep (one trace across all dates).
    trace_id = _new_trace_id()
    root_span_id = _new_span_id()
    root_start_ns = _unix_nano_now()

    emit_root = function(ev::SpanEvent)
        for esink in event_sinks
            esink(ev)
        end
        return nothing
    end

    emit_root(SpanEvent(
        kind=:start, name=:search_schedule,
        trace_id=trace_id, span_id=root_span_id, parent_span_id=UInt64(0),
        unix_nano=root_start_ns,
        attributes=Dict{Symbol,Any}(
            :universe_mode     => universe,
            :carriers          => carriers,
            :include_codeshare => include_codeshare,
            :date_count        => length(date_vec),
        ),
    ))

    results = Dict{Tuple{String,String,Date},Union{Vector{Itinerary},MarketSearchFailure}}()
    results_lock = ReentrantLock()
    total_markets = 0

    for target in date_vec
        graph = build_graph!(store, config, target; source = :newssim)

        # Pre-expand filter set once per date (handles codeshare partners).
        # Pass the expanded list to the universe strategies with
        # include_codeshare=false, since expansion already happened.
        filter_set_exp = _compute_filter_set(store, target, carriers, include_codeshare)
        filter_vec = filter_set_exp === nothing ? nothing : collect(filter_set_exp)

        u = if universe === :direct
            _universe_from_carriers_direct(store, target, filter_vec, false)
        else  # :connected
            _universe_from_carriers_connected(graph, target, filter_vec, false)
        end

        total_markets += length(u.tuples)
        markets_for_date = [(o, d) for (o, d, _) in u.tuples]

        isempty(markets_for_date) && continue

        use_parallel = config.parallel_markets && Threads.nthreads() > 1
        if use_parallel
            _search_markets_parallel_all_dates(
                config, store, [target], markets_for_date, :newssim,
                event_sinks, sink,
                results, results_lock, trace_id, root_span_id,
                graph,
            )
        else
            _search_markets_sequential_all_dates(
                config, store, [target], markets_for_date, :newssim,
                event_sinks, sink,
                results, results_lock, trace_id, root_span_id,
                graph,
            )
        end
    end

    # Root-span :end attributes.
    # Note: when `sink` is supplied, `results` is empty because the worker
    # wrote via `sink`, so we cannot compute success/failure counts. Emit -1
    # sentinels in that case so consumers can distinguish "not counted" from
    # a real zero count.
    success_count = sink === nothing ?
        count(v -> v isa Vector{Itinerary}, values(results)) : -1
    failure_count = sink === nothing ?
        count(v -> v isa MarketSearchFailure, values(results)) : -1
    root_end_ns = _unix_nano_now()
    emit_root(SpanEvent(
        kind=:end, name=:search_schedule,
        trace_id=trace_id, span_id=root_span_id, parent_span_id=UInt64(0),
        unix_nano=root_end_ns,
        status = (sink !== nothing || failure_count == 0) ? :ok : :error,
        attributes=Dict{Symbol,Any}(
            :success_count => success_count,
            :failure_count => failure_count,
            :market_count  => total_markets,
            :elapsed_ms    => (root_end_ns - root_start_ns) / 1e6,
        ),
    ))

    return results
end

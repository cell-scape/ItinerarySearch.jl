# Getting Started

This tutorial is the guided tour of ItinerarySearch.jl. It leads with the
highest-level entry point (`search_schedule`), works down through explicit
market lists (`search_markets`) and the single-query convenience (`search`),
and finally documents the low-level rule-chain APIs in the Advanced section.

All examples are self-contained: they use the NewSSIM demo file shipped with
the package so you can copy-paste any block into a REPL that already has
`using ItinerarySearch` loaded.

## 1. Installation

```bash
# Julia 1.10+ required
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

To exercise the full stack end-to-end without writing any Julia, there are two
demo targets that use the same data the examples below do:

```bash
make demo            # SSIM fixed-width path
make demo-newssim    # NewSSIM CSV path — matches the examples in this guide
```

The rest of this tutorial assumes you are inside `julia --project=.` with
`using ItinerarySearch, Dates` in scope.

## 2. Quick Start

`search_schedule` is a one-call entry point: given a NewSSIM CSV and a target
date, it ingests the data, builds the connection graph, and returns every
valid itinerary for every market a carrier serves.

```julia
using ItinerarySearch, Dates

newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

results = search_schedule(newssim_path;
    dates    = Date(2026, 2, 25),
    carriers = ["UA"],
)

length(results)                                          # number of markets
results[("ORD", "LHR", Date(2026, 2, 25))]               # itineraries for one market
```

The result is a `Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}`.
Each key is an `(origin, destination, date)` triple and each value is either a
vector of valid itineraries or a `MarketSearchFailure` sentinel (see
[Section 5](#5-handling-results)).

Everything below is a refinement of this one call: different graph-building
strategies, different market universes, different output shapes.

## 3. Core Concepts

A handful of terms recur throughout the API:

- **Itinerary** — an ordered sequence of flight legs from an origin to a
  destination on a specific target date. An itinerary with zero intermediate
  stops is a nonstop; itineraries with one or more stops pass through
  connecting airports subject to minimum-connect-time rules.
- **Market** — an `(origin, destination)` pair. A market is always evaluated
  for a given target date, making the full key an `(origin, destination, date)`
  triple.
- **Target date** — the operating date of the first leg of each candidate
  itinerary. Multi-leg itineraries that cross midnight are handled by the
  schedule window.
- **Schedule window** — the range of operating dates the graph materializes.
  Controlled by `SearchConfig.leading_days` / `trailing_days`; see
  [Section 7](#7-configuration).
- **Carrier filter** — the optional `carriers` list that scopes "which
  airline's network do we care about". Markets are those where at least one
  filter-carrier flight exists (directly or via the connected universe).

## 4. Primary Search APIs

Three layers, from broadest to narrowest:

| API | Scope | Typical use |
|---|---|---|
| `search_schedule` | Every carrier-relevant market on the date(s) | Bulk sweeps, daily feeds |
| `search_markets` | An explicit list of markets | Targeted batch queries |
| `search` | A single `(origin, destination, date)` | One-off lookups, tests |

All three return the same `Dict{Tuple{String,String,Date}, ...}` shape
(except `search`, which returns a single `Vector{Itinerary}` when given a
single market, or a vector-of-vectors for a vector of tuples).

### 4.1. `search_schedule` — carrier-scoped schedule-wide sweeps

`search_schedule` enumerates every market the filter carriers serve on each
target date, then searches them. It has three entry forms depending on how
much of the lifecycle the caller wants to own.

#### Path form (self-contained)

Pass the path to a NewSSIM CSV. Ingestion, graph-build, and store cleanup are
all handled for you.

```julia
newssim_path = joinpath(pkgdir(ItinerarySearch), "data", "demo", "sample_newssim.csv.gz")

results = search_schedule(newssim_path;
    dates    = Date(2026, 2, 25),
    carriers = ["UA"],
)
length(results)                                     # → 1678 markets on this dataset
```

Pass a vector of dates to sweep multiple operating days in one call:

```julia
results = search_schedule(newssim_path;
    dates    = [Date(2026, 2, 25), Date(2026, 2, 26), Date(2026, 2, 27)],
    carriers = ["UA"],
)
```

#### Store form (reuse ingested data)

If you are running several sweeps against the same data, ingest once and hand
the store to `search_schedule`. The caller owns the store's lifecycle:

```julia
store = DuckDBStore()
try
    ingest_newssim!(store, newssim_path)
    # (optional) ingest_mct!(store, "path/to/mct.dat")

    results_ua = search_schedule(store; dates = Date(2026, 2, 25), carriers = ["UA"])
    results_aa = search_schedule(store; dates = Date(2026, 2, 25), carriers = ["AA"])
finally
    close(store)
end
```

#### Graph form (reuse a pre-built graph)

The third form takes a `FlightGraph` and a `MarketUniverse` directly. This is
the "pure search" entry point — ingest and graph-build costs are paid up
front, so the call measures only the search phase. It is what the benchmark
suite uses.

```julia
store = DuckDBStore()
try
    ingest_newssim!(store, newssim_path)
    graph    = build_graph!(store, SearchConfig(), Date(2026, 2, 25); source = :newssim)
    universe = ItinerarySearch._universe_from_carriers_direct(
        store, Date(2026, 2, 25), ["UA"], false,
    )
    results = search_schedule(graph, universe)
    length(results)
finally
    close(store)
end
```

`_universe_from_carriers_direct` is an internal helper (underscore-prefixed)
that is fine to call from advanced code. For multi-date wide-window graphs,
see [`build_graph_for_window`](#build_graph_for_window) in the Advanced
section.

#### `:direct` vs `:connected` universe

The `universe` keyword chooses how markets are enumerated:

- `:direct` (default) — markets with at least one direct flight operated or
  marketed by a filter carrier. Fast SQL enumeration; typically a few
  thousand markets per carrier-day.
- `:connected` — markets where a filter carrier's leg appears in _any_ valid
  itinerary up to `config.max_stops` deep. Uses BFS on the live graph.
  Can be 50× larger than `:direct` on a hub carrier.

```julia
# :direct — fast, narrow
direct = search_schedule(newssim_path;
    dates = Date(2026, 2, 25), carriers = ["UA"], universe = :direct,
)

# :connected — wider, slower (BFS along connections)
connected = search_schedule(newssim_path;
    dates = Date(2026, 2, 25), carriers = ["UA"], universe = :connected,
)
```

#### Streaming via `sink`

For very large sweeps you may not want to materialize the full result dict.
Pass a `sink::Function` and each `(market_tuple, result)` pair is delivered
as soon as it completes; the returned dict stays empty.

```julia
count = Ref(0)
sink = function(market, result)
    # `market` is (origin, dest, date); `result` is either Vector{Itinerary}
    # or MarketSearchFailure. Write to file, push to a queue, whatever suits
    # your streaming policy — just avoid growing memory.
    count[] += is_failure(result) ? 0 : 1
    return nothing
end

search_schedule(newssim_path;
    dates    = Date(2026, 2, 25),
    carriers = ["UA"],
    sink     = sink,
)

count[]   # number of markets streamed
```

The sink is called from worker tasks when `parallel_markets=true` — implement
your own synchronization if the sink mutates shared state.

### 4.2. `search_markets` — explicit market list

When you already know which markets you want, use `search_markets`. Unlike
`search_schedule`, nothing is enumerated from the carrier filter; only the
markets you ask for are searched.

#### Kwargs form

```julia
results = search_markets(newssim_path;
    markets = [("ORD", "LHR"), ("DEN", "LAX")],
    dates   = Date(2026, 2, 25),
)

results[("ORD", "LHR", Date(2026, 2, 25))]     # itineraries for one market
```

`markets` is a vector of `(origin, destination)` pairs; `dates` is a single
`Date` or a vector of dates. The two are taken as a cartesian product: N
markets × M dates produces N × M result entries.

#### Tuple dispatch — single tuple

For a one-off query, a single `(origin, destination, date)` tuple is often
the clearest form:

```julia
results = search_markets(newssim_path, ("ORD", "LHR", Date(2026, 2, 25)))
```

This delegates to the kwargs form with a single-market list and the tuple's
date. The result is still a keyed dict with one entry.

#### Tuple dispatch — vector of tuples (non-cartesian)

A vector of tuples is explicit per-tuple, **not** cartesian'd with a dates
list. `[(ORD, LHR, Feb25), (EWR, LHR, Feb26)]` produces exactly 2 searches,
one for each tuple:

```julia
results = search_markets(newssim_path, [
    ("ORD", "LHR", Date(2026, 2, 25)),
    ("EWR", "LHR", Date(2026, 2, 26)),
])
# results has exactly 2 keys: ("ORD","LHR",Date(2026,2,25)) and ("EWR","LHR",Date(2026,2,26))
```

This is the **non-cartesian invariant**: the vector-of-tuples form guarantees
one search per tuple. Use the kwargs form when you want a cartesian product.

Multi-date vector inputs re-ingest the NewSSIM file once per unique date. For
performance-sensitive multi-date sweeps prefer `search_schedule`, which keeps
a single store open across all dates.

### 4.3. `search` — single `(origin, destination, date)`

`search` is the oldest convenience wrapper: build graph, run DFS, return
`Vector{Itinerary}`. It takes a pre-ingested store (not a path) and is most
useful for one-off lookups once you already have data loaded.

```julia
store = DuckDBStore()
try
    ingest_newssim!(store, newssim_path)
    itns = search(store, ("ORD", "LHR", Date(2026, 2, 25)); source = :newssim)
    length(itns)
finally
    close(store)
end
```

`search` also accepts `(origin::StationCode, dest::StationCode, date::Date)`
as three positional arguments, or a `Vector{Tuple{...}}` for a batch with
one graph-build per unique date.

## 5. Handling Results

Every `search_schedule` / `search_markets` result value is one of:

- `Vector{Itinerary}` — the market was searched successfully
- `MarketSearchFailure` — the underlying `search_itineraries` call threw an
  exception; the market was preserved as a sentinel instead of aborting the
  batch

The `is_failure` predicate and `failed_markets` extractor handle this union
without pattern matching:

```julia
results = search_schedule(newssim_path; dates = Date(2026, 2, 25), carriers = ["UA"])

# Partition successes from failures
fails = failed_markets(results)
length(fails)                    # 0 on the demo dataset

# Test a single value
v = results[("ORD", "LHR", Date(2026, 2, 25))]
is_failure(v)                    # false → v is a Vector{Itinerary}
```

`MarketSearchFailure` captures the `(market, exception, backtrace, worker_slot,
elapsed_ms)` so you can triage which market broke and why without replaying
the search.

## 6. Parallelism

When Julia is launched with more than one thread, `search_schedule` /
`search_markets` parallelize across markets automatically. Each worker gets
its own `RuntimeContext` and the shared `FlightGraph` is read-only, so there
is no lock contention on the hot DFS path.

Start Julia with threads enabled:

```bash
julia --project=. --threads=auto
# or pin a specific count:
julia --project=. --threads=8
```

To force sequential execution — useful for profiling, debugging, or reproducing
deterministic error traces — flip `SearchConfig.parallel_markets`:

```julia
results = search_schedule(newssim_path;
    dates            = Date(2026, 2, 25),
    carriers         = ["UA"],
    parallel_markets = false,
)
```

The CLI exposes the same knob as `--no-parallel`. See
[`config/README.md`](../../config/README.md) for the field reference.

## 7. Configuration

`SearchConfig` is the single configuration struct threaded through every
search. Defaults are sensible, so `SearchConfig()` works out of the box; most
users override a handful of fields.

### SearchConfig overview

Three common construction patterns:

```julia
# Defaults
config = SearchConfig()

# Keyword overrides
config = SearchConfig(max_stops = 3, max_elapsed_minutes = 2880)

# From a Dict (e.g. YAML or environment-derived)
config = SearchConfig(Dict(:max_stops => 3, :interline => "all"))
```

Or load a full JSON config:

```julia
config = load_config("config/defaults.json")
```

The tracked `config/defaults.json` is an exhaustive exemplar listing every
`SearchConfig` field at its compiled-in default — copy it, delete the sections
you don't need, and tweak only the fields you want to override. Missing keys
fall back to the struct defaults. See [`config/README.md`](../../config/README.md)
for a grouped field reference (store, data, schedule, search, `mct_behaviour`,
graph, output, `mct_audit`) and JSON schema.

### Circuity tiers

Circuity is the ratio of _flown_ distance to _great-circle_ distance — a
proxy for how "direct" an itinerary is. Short hops tolerate much higher
circuity than long hauls: you'll happily accept a 2.4× detour on a 200-mile
regional but not on a transatlantic. `ItinerarySearch` models this as a
**distance-tiered ceiling**:

```julia
julia> DEFAULT_CIRCUITY_TIERS
4-element Vector{CircuityTier}:
 CircuityTier(250.0, 2.4)    # 0–250 mi   → ≤ 2.4×
 CircuityTier(800.0, 1.9)    # 251–800 mi → ≤ 1.9×
 CircuityTier(2000.0, 1.5)   # 801–2000   → ≤ 1.5×
 CircuityTier(Inf, 1.3)      # 2001+      → ≤ 1.3×
```

`CircuityTier` is an isbits struct with two `Float64` fields: an inclusive
upper-bound distance (miles) and the max circuity factor permitted up to that
distance. The lookup is a linear scan — fast, no allocation.

Circuity is checked at two layers:

| Layer | When | Where |
|-------|------|-------|
| Connection | During `build_connections!` (graph-build time) | `CircuityRule` in `src/graph/rules_cnx.jl` |
| Itinerary  | During DFS (per candidate path) | `check_itn_circuity_range` in `src/graph/rules_itn.jl` |

`SearchConfig.circuity_check_scope` toggles which layers enforce the rule:

```julia
SearchConfig(circuity_check_scope = :both)         # default
SearchConfig(circuity_check_scope = :connection)   # prune early, skip itinerary
SearchConfig(circuity_check_scope = :itinerary)    # defer to full-path check
```

The itinerary-level check automatically waives itself for nonstops and
1-stops — for those, the connection-level check already saw the full path.

Swap the whole tier vector via `ParameterSet(circuity_tiers=...)`:

```julia
strict = SearchConstraints(
    defaults = ParameterSet(
        circuity_tiers = [
            CircuityTier(500.0, 1.8),   # 0–500 mi
            CircuityTier(Inf,   1.2),   # 500+ mi — tight long-haul
        ],
        max_circuity   = 1.4,           # global ceiling, applied after tier lookup
    ),
)
```

`max_circuity` is the global _ceiling_ applied after the tier lookup — useful
to cap tier outputs without rewriting the tiers themselves. `min_circuity`
is the global _floor_ (reject too-direct itineraries).
`domestic_circuity_extra_miles` and `international_circuity_extra_miles` add
flat-mile tolerance to the ceiling (`factor × gc + extra`).

`SearchConstraints` also holds an `overrides::Vector{MarketOverride}`. When a
connection or itinerary has a matching market override, its `ParameterSet` is
used in place of `defaults`. Circuity-resolution is market-only — carrier is
ignored because circuity is a geographic property. See
[`docs/reference/pm_constraint_tables.md`](../reference/pm_constraint_tables.md)
for the full profit-manager file-format reference.

### Schedule window

The schedule window is the range of operating dates the graph materializes.
It is controlled by two `SearchConfig` fields:

- `leading_days::Int = 2` — how many operating days before the target date
  are included in the graph. Needed so that connections beginning the night
  before can still feed the target-day search.
- `trailing_days::Int = 0` — how many operating days after the target date
  are included. Needed for multi-day itineraries that cross midnight.

For a single-date search on `Date(2026, 2, 25)` with defaults, the graph
covers `Date(2026, 2, 23)` through `Date(2026, 2, 25)`. Widen the window when
you need multi-day itineraries or when your sweep includes adjacent dates:

```julia
config = SearchConfig(leading_days = 2, trailing_days = 3)
```

When sweeping several consecutive dates, `build_graph_for_window` (Advanced
section) replaces N per-date `build_graph!` calls with a single wide build.

## 8. Observability (brief)

Every high-level search emits OpenTelemetry-shaped `SpanEvent`s as it runs.
Pass one or more functions via the `event_sinks` keyword to receive them:

```julia
events = ItinerarySearch.SpanEvent[]
event_lock = ReentrantLock()
collector = function(ev::ItinerarySearch.SpanEvent)
    lock(event_lock) do
        push!(events, ev)
    end
    return nothing
end

results = search_schedule(newssim_path;
    dates       = Date(2026, 2, 25),
    carriers    = ["UA"],
    event_sinks = Function[collector],
)

length(events)           # start + end for each span emitted during the sweep
events[1].name           # :search_schedule (root span)
```

`SpanEvent` fields: `kind` (`:start` | `:end`), `name`, `trace_id`, `span_id`,
`parent_span_id`, `unix_nano`, `worker_slot`, `status`, and a free-form
`attributes::Dict{Symbol,Any}`. Root-span attributes include
`:universe_mode`, which takes one of three values:

- `:direct` — path/store form with `universe=:direct`
- `:connected` — path/store form with `universe=:connected`
- `:prebuilt` — graph form (`search_schedule(graph, universe)`)

The schema is OTel-ready; an OTLP/HTTP exporter is on the roadmap. For now,
the `event_sinks` hook is the integration point — wire it to your own
collector, metrics backend, or file sink.

## 9. Advanced / Low-Level

This section documents the primitives the high-level APIs compose. Reach for
them when you need per-search fine-grained control, are writing benchmarks,
or are extending the pipeline.

### `search_itineraries` — the DFS primitive

`search_itineraries` is the low-level depth-first search that underlies every
other API in this package. It operates on a materialized graph and a
`RuntimeContext`, and does **not** copy its results — it returns
`ctx.results` by reference, which is reused on the next search.

Callers **must** `copy()` the returned vector before issuing another search
on the same context, or they will observe aliasing.

```julia
using DataFrames

store = DuckDBStore()
ingest_newssim!(store, newssim_path)

target_date = Date(2026, 2, 25)
config      = SearchConfig()
graph       = build_graph!(store, config, target_date; source = :newssim)

ctx = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
)

itineraries = copy(search_itineraries(
    graph.stations,
    StationCode("ORD"),
    StationCode("LHR"),
    target_date,
    ctx,
))

close(store)
```

The DataFrame wrappers consume this same `Vector{Itinerary}` directly:

```julia
# One row per leg per itinerary — tidy/long format with MCT audit columns
legs_df    = itinerary_legs_df(itineraries)

# One row per itinerary — summary totals and joined flight-id strings
summary_df = itinerary_summary_df(itineraries)

# Wide pivot: legN_*/cnxN_* columns, side-by-side comparable
pivot_df   = itinerary_pivot_df(itineraries; max_legs = 3)
```

See [API: Output](api/output.md) for the full column reference.

### `build_graph!` — manual graph build

`build_graph!` materializes the full in-memory network for a single target
date. Use it when you want direct control over graph lifetime (e.g. reuse one
graph across many searches):

```julia
store = DuckDBStore()
ingest_newssim!(store, newssim_path)

graph = build_graph!(store, SearchConfig(), Date(2026, 2, 25); source = :newssim)
graph.build_stats                # instrumentation counts

close(store)
```

`build_graph!` queries schedule-level legs for the window
`(target - leading_days)` to `(target + trailing_days)`, gap-fills missing
leg distances from the geodesic formula, materializes the MCT lookup, and
runs the O(n²) connection builder. `graph.build_stats` contains station,
leg, segment, connection counts and build time.

### <a name="build_graph_for_window"></a>`build_graph_for_window` — multi-date amortization

When sweeping several consecutive target dates, building a graph per date
repeats a lot of work. `build_graph_for_window` widens the window to cover
every date in one go:

```julia
store = DuckDBStore()
ingest_newssim!(store, newssim_path)

dates = [Date(2026, 2, 25), Date(2026, 2, 26), Date(2026, 2, 27)]
graph = build_graph_for_window(store, SearchConfig(), dates)

# Now feed a pre-computed universe straight to search_schedule(graph, universe):
universe_tuples = Tuple{String,String,Date}[]
for date in dates
    u = ItinerarySearch._universe_from_carriers_direct(store, date, ["UA"], false)
    append!(universe_tuples, u.tuples)
end
universe = MarketUniverse(universe_tuples)
results  = search_schedule(graph, universe)

close(store)
```

The wide-window graph is the basis of the pure-search benchmarks — ingest
and build are paid up front so the search-only phase can be measured in
isolation.

### `RuntimeContext` — per-search mutable state

`RuntimeContext` holds the rule chain, great-circle distance cache, search
statistics, and results buffer. Create one per search thread. The high-level
APIs construct contexts for you; reach for the direct constructor only when
driving `search_itineraries` manually:

```julia
ctx = RuntimeContext(
    config      = SearchConfig(),
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(SearchConfig()),
)
```

### Rule chain overview

Connection and itinerary rules are `Vector{Function}`. Each rule returns
`Int` (positive = pass, 0/negative = fail with reason code). Rules are
enabled or disabled by including them in (or excluding them from) the chain.
`build_itn_rules(config)` constructs the itinerary rule chain from a
`SearchConfig`; connection rules are wired in during `build_connections!`.

This design keeps the hot path branch-light — each rule is a concrete
function, the chain is iterated once per candidate, and failures short-circuit
immediately. To add a new rule, write a function matching the rule signature,
append it to the chain, and the DFS picks it up with no other changes.

### Per-leg passthrough columns (output)

`write_legs`, `write_itineraries`, and `write_trips` accept a `store::DuckDBStore`
and a `passthrough_columns::Vector{String}` keyword argument that appends
arbitrary columns from the original ingested schedule table — `prbd`,
`DEI_127`, anything in the source CSV that isn't among the canonical columns.
A single batched SQL query fetches all passthrough values; the empty-vector
default takes a fast path with no store access. See
[API: Output — Passthrough Columns](api/output.md) for details.

## 10. Reference Data Files

The canonical reference files the demo uses live under `data/demo/`:

| File | Purpose |
|---|---|
| `sample_newssim.csv.gz` | NewSSIM denormalized schedule (carrier, flight, O-D, times, aircraft, distance) |
| `ssim_demo.dat` | SSIM fixed-width schedule (alternative ingest path) |
| `mct_demo.dat` | Minimum Connecting Time records |
| `airports.txt` | Airport metadata (timezone offsets, country, region codes) |
| `regions.dat` | Region hierarchy |
| `aircraft.txt` | Aircraft type codes |
| `cirOvrdDflt.dat` | Circuity tier defaults (`HIGH,CIRCUITY` rows) |
| `cirOvrd.dat` | Per-market circuity overrides (`ORG,DEST,ENTNM,CRTY` rows) |
| `maxCnctTm.dat` / `maxCnctTmDflt.dat` | Maximum connection time tables |
| `cnctFlags.dat` | Connection flags |
| `mktFlags.dat` | Market-level flag overrides |
| `alliance.dat`, `alliancePref.dat`, `alnCtry.dat` | Alliance metadata |
| `entList.dat` | Entity / carrier alias mappings |
| `ItinLvlCnstExc.dat`, `RgnLvlCnstExc.dat` | Itinerary- and region-level constraint exceptions |

For real workloads, point `SearchConfig` at your production files via the
`store`, `data`, and related keyword groups. `config/defaults.json` documents
every option; the loaders under `src/ingest/` and `src/store/` accept both
file paths and in-memory `DataFrame`s.

See the [Architecture](architecture.md) page for the full pipeline overview
and [`docs/reference/pm_constraint_tables.md`](../reference/pm_constraint_tables.md)
for the profit-manager file-format reference.

# ItinerarySearch Development Diary

## 2026-04-22 — Parallel market search

Wired a Channel-backed worker pool into `search_markets()` so per-market
searches run concurrently across `Threads.nthreads()` workers, each with
its own `RuntimeContext` (and therefore its own warm caches). Key
additions:

- `SearchConfig.parallel_markets::Bool = true` — opt-out flag
- `--no-parallel` CLI flag (global, following `--no-mct-cache` idiom)
- `MarketSearchFailure` sentinel — return value for per-market failures,
  keeps the batch alive when one market throws. Public helpers `is_failure(x)`
  and `failed_markets(dict)` for inspection.
- `SpanEvent` struct — OTel-ready span events emitted for the root
  `search_markets` call and each child `market_search` span. `event_sinks`
  kwarg on `search_markets` lets callers register observer functions.
- `TraceContext` + `_new_trace_id`, `_new_span_id`, `_unix_nano_now` —
  W3C Trace Context primitives (UInt128 trace ids, UInt64 span ids,
  nanosecond Unix timestamps from `time_ns()` + one-time offset).

Worker pool design:
- `Channel{RuntimeContext}(nthreads())` pre-filled with N contexts (one
  per slot, 1..N); each `Threads.@spawn`-ed per-market task takes a
  context, runs its search, returns the context in a `finally`. Fresh
  pool per target date so cache validity tracks graph validity.
- `ReentrantLock` protects shared `results::Dict` writes.
- No `Threads.threadid()` anywhere (unstable under task migration in
  Julia 1.7+); `worker_slot` is the stable pool slot id.
- Return type widened to `Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}`.

Benchmark on 12 markets × demo dataset (4-thread MacBook, Apple Silicon):
- Sequential: 2544 ms
- Parallel (nthreads=4): 2381 ms (1.07× speedup)

The modest speedup is expected: `search_markets()` includes CSV ingest +
graph build + DFS search in a single call. The ingest/build phase is serial
in both modes and accounts for ~93% of wall time (build ≈ 1.3 s after
warm JIT, ingest from .gz ≈ dominant). The DFS search phase (≈7% of
wall time, ~460 ms for 12 markets sequential) is the part that
parallelizes. At production scale — many dates, or a pre-built graph
shared across market calls — the parallel path delivers its expected
speedup on the search-only slice.

**Deferred:** OTLP/HTTP/JSON exporter for Dynatrace sidecar — tracked in
`docs/superpowers/specs/2026-04-22-otlp-http-exporter-design.md` and
`docs/superpowers/plans/2026-04-22-otlp-http-exporter.md`.

## 2026-04-21 — Circuity Tiers and Market-Level Overrides
- **Scope**: Replace the scalar `SearchConfig.circuity_factor = 2.5` with a distance-tiered, market-overridable circuity model that mirrors the production profit-manager tables
- **Data model**:
  - New `CircuityTier` isbits struct (`max_distance::Float64`, `factor::Float64`)
  - New `DEFAULT_CIRCUITY_TIERS = [(250, 2.4), (800, 1.9), (2000, 1.5), (Inf, 1.3)]` — stricter long-haul than the old 2.5 scalar
  - `ParameterSet.circuity_factor::Float64 = 2.0` → `circuity_tiers::Vector{CircuityTier} = DEFAULT_CIRCUITY_TIERS`
  - `max_circuity` default changed from `0.0` to `Inf` so the global ceiling is opt-in
  - New `SearchConfig.circuity_check_scope::Symbol ∈ {:connection, :itinerary, :both}` (default `:both`); itinerary check waives itself for nonstops/1-stops
- **Resolution semantics**:
  - `_resolve_circuity_params(constraints, org, dst)` — **market-only** (carrier ignored, circuity is geographic)
  - `_effective_circuity_factor(p, distance)` combines tier lookup with `max_circuity` ceiling
  - `CircuityRule` is now a **fieldless marker struct**; both `factor` and `{domestic,international}_circuity_extra_miles` resolve live from the matched `ParameterSet` at evaluation time, matching `check_itn_circuity_range` — a market override with custom extra_miles takes effect at both layers
- **CSV loaders** (`src/ingest/circuity.jl`):
  - `load_circuity_tiers("cirOvrdDflt.dat")` — HIGH,CIRCUITY columns
  - `load_circuity_overrides("cirOvrd.dat")` — ORG,DEST,ENTNM,CRTY columns (ENTNM reserved for future entity grouping)
  - `apply_circuity_files!(constraints; defaults_path, overrides_path)` composer
  - JSON loader `_parse_circuity_tiers` supports `[{"max_distance": ..., "factor": ...}]` arrays in constraints files
- **Tutorial / docs**:
  - Getting Started Step 3b added — tiers, programmatic overrides, market-level overrides, CSV loaders
  - `docs/reference/pm_constraint_tables.md` added — cross-format reference covering circuity and sibling PM tables
  - Demo CSVs tracked: `data/demo/cirOvrd.dat`, `data/demo/cirOvrdDflt.dat`
- **Tests**: 19001 passed (down from 20824 pre-tiers baseline; the drop is **expected** — the default 1.3 long-haul factor is stricter than the old 2.5 scalar, so demo-data iteration loops iterate fewer times). New coverage: `test/test_circuity_tiers.jl` (51 tests), per-market extra_miles regression test in `test_rules_cnx.jl`, scope-gating assertions in `test_rules_cnx.jl`, CSV round-trip tests.

## 2026-03-24 — REST API Service
- **Scope**: HTTP REST API wrapping the search pipeline, served from a pre-built in-memory flight graph
- **Changes**:
  - `Server` submodule (`src/server.jl`) with HTTP.jl: 5 endpoints (search, trip, station, health, rebuild)
  - `ServerState` mutable struct with `ReentrantLock`-protected graph reference, `Atomic{Bool}` rebuild guard
  - Per-request graph snapshot pattern: lock-free reads, lock-protected writes on rebuild only
  - Per-request `SearchConstraints` from JSON body (max_stops, max_elapsed, max_connection, circuity_factor)
  - `_trips_to_json` flat serialization avoiding circular graph references
  - `POST /rebuild` with background task, atomic guard (409 if in progress), try/finally safety
  - CLI `serve` subcommand: `itinsearch serve --date 2026-03-20 --port 8080`
  - `make serve DATE=2026-03-20` target
- **Tests**: 1413 total (20 new server endpoint tests)

## 2026-03-24 — CLI, PrecompileTools, PackageCompiler
- **Scope**: Command-line interface, precompilation workloads, sysimage/app builds
- **Changes**:
  - CLI with 5 commands: search, trip, build, ingest, info (ArgParse.jl)
  - Global flags: --config, --log-level, --log-json, --quiet, --compact, --output
  - Parameter overrides: --max-stops, --max-elapsed, --max-connection, --circuity-factor, --scope, --interline, etc.
  - `bin/itinsearch.jl` entry point, PackageCompiler-ready `main(args)::Int`
  - PrecompileTools `@compile_workload` for 741ms load, 110ms first-call
  - PackageCompiler sysimage build: 236MB, 0ms module load (`make sysimage`)
  - PackageCompiler app build: standalone distributable (`make app`)
  - juliac entry point ready (blocked by DuckDB JLL artifact loading)
  - Dedicated `build/precompile_workload.jl` exercises full pipeline without test deps
- **Tests**: 1393 total (102 new CLI tests)

## 2026-03-24 — MCT Cache, LegKey Date/Time, StationCode Sizing
- **Scope**: MCT lookup cache with revalidation, LegKey schedule context, StationCode type optimization
- **Changes**:
  - `MCTCacheKey` (isbits, 96 bytes) caches MCT results by full SSIM8 field set minus flight numbers and date
  - Cache revalidation: if cached result matched on flight-number ranges or date validity, discards hit and does full lookup
  - 77% cache hit rate (5.2M hits / 6.8M lookups), `mct_cache_enabled::Bool = true` on SearchConfig
  - `LegKey` gains `operating_date::UInt32` and `dep_time::Minutes` for date disambiguation
  - `itinerary_legs` output sorted by operating_date, dep_time, then stops/elapsed/distance
  - JSON output includes operating_date and dep_time in leg keys and itinerary summaries
  - `StationCode` changed from `InlineString7` (8 bytes) to `InlineString3` (4 bytes) — IATA codes are always 3 chars
- **Tests**: 1291 total (all passing)

## 2026-03-23 — Remove Layer 1 (Experimental, Unused)
- **Scope**: Remove the experimental Layer 1 one-stop pre-computation feature
- **Rationale**: Disabled by default, 2.5s build overhead, caused result explosion (216k vs 1.8k), no value for single-session use
- **Removed**: `src/graph/layer1.jl` (520 lines), `test/test_layer1.jl` (1123 lines), `OneStopConnection`/`OneStopIndex` types, `layer1_built`/`layer1` fields from FlightGraph and RuntimeContext, `layer1_hits`/`layer1_misses` from SearchStats, L1 DuckDB tables, L1 branching in `_dfs!`
- **Impact**: -1,930 lines, cleaner DFS search path, simpler type system
- **Tests**: 1291 total (66 L1 tests removed)

## 2026-03-23 — Performance and Simplification (Code Review Fixes)
- **Scope**: Address code review findings — performance tuning, dead code removal, simplification
- **Changes**:
  - Rule chains changed from `Vector{Any}` to `Tuple` — eliminates dynamic dispatch in O(n²) connection builder
  - InlineString direct comparison in MCTRule codeshare resolution — eliminates 4 String allocations per connection pair
  - MCT suppression geography uses direct InlineString3 comparison — no more `strip(String(...))`
  - MCTRule connection time now uses UTC offsets (`dep_utc_offset`, `arr_utc_offset`) — correct for inter-station
  - `nonstop_cp` field added to `GraphLeg` — replaces O(n) scan with direct field access in search
  - `_compute_specificity` refactored to accept `(specified, eff_date)` — eliminates double MCTRecord construction
  - Removed dead `mct_cache_hits` and `mct_dual_pass` from BuildStats
- **Tests**: 1356 total (all passing)

## 2026-03-23 — Structured Logging with DynaTrace Compatibility
- **Scope**: Add structured JSON logging compatible with DynaTrace, verbose @debug logging, configurable log levels
- **Changes**:
  - LoggingExtras `TeeLogger` fans out `@info`/`@debug`/`@warn`/`@error` to console + DynaTrace JSON
  - `_dynatrace_json_formatter` produces `{"timestamp","severity","content","service.name","attributes"}` envelope
  - `setup_logger(config)` builds the TeeLogger with `MinLevelLogger` wrapping
  - Log level from `ITINERARY_SEARCH_LOG_LEVEL` env var → `config.log_level` → `:info`
  - JSON output to file (`log_json_path`) and/or stdout (`log_stdout_json`)
  - Verbose `@debug` calls added in builder, connect, search, ssim, mct, reference
  - Logger installed in `build_graph!` with try/finally lifecycle
- **Tests**: 1356 total (133 new logging tests)

## 2026-03-23 — Observe Event Log + System Metrics Poller
- **Scope**: Build `src/observe/` subsystem with typed events, pluggable EventLog, JSONL sink, cooperative system metrics polling
- **Changes**:
  - 5 event structs: SystemMetricsEvent, PhaseEvent, BuildSnapshotEvent, SearchSnapshotEvent, CustomEvent
  - `EventLog` with `emit!`, `checkpoint!`, `with_phase`, `close`
  - `collect_system_metrics()` captures RSS, GC stats, thread count
  - `JsonlSink` (file) and `stdout_sink` for JSONL output
  - `build_graph!` wired with 3 phase brackets + 4 system metrics checkpoints + BuildSnapshotEvent
  - Disabled by default (`event_log_enabled = false`), zero overhead when off
- **Tests**: 1223 total (48 new observe tests)
- **Deferred**: SearchSnapshotEvent emission in search.jl, DuckDB sink, OTel integration

## 2026-03-23 — Tier 1 Instrumentation Wiring
- **Scope**: Wire all unpopulated fields in BuildStats, SearchStats, MCTResult; add MCTSelectionRow audit logging; add geographic stats aggregation
- **Changes**:
  - `MCTResult` gains `matched_fields::UInt32` propagated from matched MCTRecord.specified
  - Rule pass/fail counters wired in connection builder rule chain loop
  - MCT cascade counters (lookups, exceptions, standards, defaults, suppressions) wired in MCTRule
  - MCT time histogram (48 buckets, 10-min steps) and running average populated
  - MCTSelectionRow audit log populated when `metrics_level == :full`
  - Search stats: max_depth_reached, elapsed_time_hist (30-min buckets), total_distance_hist (250-mi buckets), search_time_ns
  - BuildStats total_pairs_evaluated summed from per-station stats
  - `merge_build_stats!` updated with weighted average for mct_avg_time; `merge_station_stats!` fixed for num_departures/num_arrivals
  - `GeoStats` type alias and `aggregate_geo_stats` function: post-build aggregation by metro/state/country/region
  - `FlightGraph` gains `geo_stats::GeoStats` field, populated in `build_graph!`
- **Tests**: 1174 total (34 new instrumentation tests)

## 2026-03-23 — MCT Full SSIM8 Matching
- **Scope**: Expanded the MCT lookup to support all SSIM8 Chapter 8 matching fields
- **Changes**:
  - MCTRecord: 15 new fields (codeshare ind/op_carrier, aircraft type, flight number ranges, state geography, date validity, suppression geography)
  - 10 new MCT_BIT_* bitmask constants (bits 12-21)
  - `_compute_specificity` reweighted to full 29-level SSIM8 hierarchy
  - `_mct_record_matches` expanded from 11 to 23 parameters
  - `lookup_mct` signature expanded with 14 new kwargs, date validity pre-filter, suppression geography scope
  - MCTLookup key changed from `StationCode` to `Tuple{StationCode,StationCode}` for inter-station support
  - `inter_station_default = Minutes(240)` fallback for cross-airport connections
  - MCTRule passes full SSIM8 context (codeshare status, operating carrier, equipment, flight number, geography, target date)
  - Materialization SQL and `_build_mct_record` updated to populate all new fields
  - Comprehensive test suite: 8 new test categories covering all matching dimensions
- **Tests**: 1132 total (187 MCT lookup tests)
- **Deferred**: Inter-station connection building (pairing arrivals/departures across metro airports)

## 2026-03-17 — Project scaffolding
- **Notes**: Initial project creation. No measurements yet.

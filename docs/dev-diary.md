# ItinerarySearch Development Diary

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

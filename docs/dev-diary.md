# ItinerarySearch Development Diary

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

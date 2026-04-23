# Benchmark Results — ItinerarySearch.jl

Rolling log of benchmark timings. Each entry is one commit's run of
`benchmark/bench_schedule.jl`, `benchmark/bench_schedule_pure.jl`, and
`benchmark/bench_markets.jl` across the four documented thread configurations.

**How to add an entry:**
1. Run benchmarks under `JULIA_NUM_THREADS=1`, `=2`, `=4`, `=auto`
2. Capture the minimum-of-`@be` time for each scenario at each thread count
3. Add a new row at the top of the applicable table with the commit SHA + date
4. Note any anomalies (first-run JIT spike, noise outliers, etc.)

**Machine context:**
- Apple Silicon MacBook, 4 P-cores + 4 E-cores
- `JULIA_NUM_THREADS=auto` resolves to 4 on this machine (P-cores only)
- Julia 1.10+
- Chairmarks.jl for timing

Numbers are not directly comparable across machines. When benchmarking
elsewhere, start a new "Machine context" block with its own tables.

---

## Pure search (isolated) — setup excluded from timing

Measures only `search_schedule(graph, universe)` with a pre-built graph and
pre-computed universe. Ingest + graph build are paid outside the timed block.
This is where parallelism gain is visible without Amdahl pollution from
serial setup.

### `search_schedule(graph, universe)` — `:direct`, UA filter, 1 date, demo dataset (1,678 markets)

| Commit   | Date       | 1 thread  | 2 threads | 4 threads | auto     | Notes |
|----------|------------|-----------|-----------|-----------|----------|-------|
| e8e5674  | 2026-04-23 | 10418 ms  | 4326 ms   | 2356 ms   | 3882 ms  | 1→4 speedup: **4.42×** (super-linear, likely cache effects per-thread). `auto` (4T) showed 64% variance vs explicit 4T on this scenario — Chairmarks is 1-sample at this size; cross-check recommended. |

### `search_schedule(graph, universe)` — `:direct`, no filter, 1 date, demo dataset (1,678 markets)

| Commit   | Date       | 1 thread  | 2 threads | 4 threads | auto     | Notes |
|----------|------------|-----------|-----------|-----------|----------|-------|
| e8e5674  | 2026-04-23 | 9250 ms   | 4225 ms   | 2241 ms   | 2488 ms  | 1→4 speedup: **4.13×**. `:direct all` is faster than `:direct UA` at every thread count — UA filter adds per-leg matching overhead that unfiltered doesn't pay. |

### `search_schedule(wide_graph, universe)` — multi-date wide-window, `:direct UA` × 3 dates, demo dataset (5,111 markets)

| Commit   | Date       | 1 thread  | 2 threads | 4 threads | auto     | Notes |
|----------|------------|-----------|-----------|-----------|----------|-------|
| e8e5674  | 2026-04-23 | 31658 ms  | 14522 ms  | 10506 ms  | 8317 ms  | 1→4 speedup: **3.01×**. Wide-window graph built via `build_graph_for_window` covering 3 dates. `auto` (4T) faster than explicit 4T on this scenario (run variance). |

---

## End-to-end (whole call) — setup included from timing

Measures `search_schedule(path; ...)` (and `search_markets`) including CSV
ingest, MCT ingest (if provided), graph build, and search. Ingest+build
typically dominate wall time on single-day runs; this section exists to
detect regressions in those phases, not to measure parallelism. For pure
search-phase timing, see the "Pure search (isolated)" section above.

### `search_schedule` — `:direct`, UA filter, 1 date, demo dataset

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| 16b1a1f  | 2026-04-23 | 16836 ms | 12333 ms  | 9851 ms   | 9986 ms | First 4T run was 12733 ms (noise); cleaner re-run 9851 ms. |

### `search_schedule` — `:direct`, no filter, 1 date, demo dataset

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| 16b1a1f  | 2026-04-23 | 13590 ms | 8493 ms   | 5889 ms   | 7000 ms |       |

### `search_markets` — tuple vector form, 4 markets, 1 date, demo dataset

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| 16b1a1f  | 2026-04-23 | 2639 ms  | 2433 ms   | 2370 ms   | 2458 ms | Bounded by serial CSV ingest + graph build. |

### `search_markets` — 12 markets × 1 date (from PR #5, commit eb63202)

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| eb63202  | 2026-04-22 | 2544 ms  |           |           |         | Pre-multi-thread-bench task; only nthreads=4 sampled at 2381 ms (1.07×). |

---

## Regression detection

When adding a new entry, scan down each column for that scenario:

- **Stable number across commits** — no regression.
- **Significantly slower (>10%)** — investigate. Likely causes:
  - New work added on the hot path
  - Cache invalidation regression
  - Rule chain expansion
  - GC pressure from new allocations
- **Significantly faster (>10%)** — worth noting the cause so it isn't accidentally undone later.

10% is a rough threshold; Chairmarks minimum-of-many samples is noisy by
~5% on Apple Silicon. Cross-check with a re-run before flagging.

**Comparing pure-search to end-to-end:** the ratio between a pure-search
number and its end-to-end counterpart reveals the serial (ingest + build)
overhead. For `:direct UA` at 4 threads on commit e8e5674: 2.4s pure vs
9.9s end-to-end → ~7.5s of serial setup per call. If that ratio changes
substantially, either the ingest path or the graph build changed — the
split between the two sections helps localize regressions to the right
phase.

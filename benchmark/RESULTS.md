# Benchmark Results — ItinerarySearch.jl

Rolling log of benchmark timings. Each entry is one commit's run of
`benchmark/bench_schedule.jl` and `benchmark/bench_markets.jl` across
the four documented thread configurations.

**How to add an entry:**
1. Run both benchmarks under `JULIA_NUM_THREADS=1`, `=2`, `=4`, `=auto`
2. Capture the minimum-of-`@be` time for each scenario at each thread count
3. Add a new row at the top of each applicable table with the commit SHA + date
4. Note any anomalies (first-run JIT spike, noise outliers, etc.)

**Machine context:**
- Apple Silicon MacBook, 4 P-cores + 4 E-cores
- `JULIA_NUM_THREADS=auto` resolves to 4 on this machine (P-cores only)
- Julia 1.10+
- Chairmarks.jl for timing

Numbers are not directly comparable across machines. When benchmarking
elsewhere, start a new "Machine context" block with its own tables.

---

## `search_schedule` — `:direct`, UA filter, 1 date, demo dataset

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| 16b1a1f  | 2026-04-23 | 16836 ms | 12333 ms  | 9851 ms   | 9986 ms | First 4T run was 12733 ms (noise); cleaner re-run 9851 ms. |

## `search_schedule` — `:direct`, no filter, 1 date, demo dataset

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| 16b1a1f  | 2026-04-23 | 13590 ms | 8493 ms   | 5889 ms   | 7000 ms |       |

## `search_markets` — tuple vector form, 4 markets, 1 date, demo dataset

| Commit   | Date       | 1 thread | 2 threads | 4 threads | auto    | Notes |
|----------|------------|----------|-----------|-----------|---------|-------|
| 16b1a1f  | 2026-04-23 | 2639 ms  | 2433 ms   | 2370 ms   | 2458 ms | Bounded by serial CSV ingest + graph build. |

---

## `search_markets` — 12 markets × 1 date (from PR #5, commit eb63202)

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

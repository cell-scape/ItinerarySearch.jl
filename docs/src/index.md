# ItinerarySearch.jl

High-performance itinerary building and search for airline schedules.

ItinerarySearch.jl ingests OAG/SSIM schedule data and MCT (Minimum Connecting Time) tables, builds an in-memory flight connection graph, and serves multi-stop itinerary search queries. It is a standalone Julia package implementing the Module D itinerary-building responsibility, designed as a reusable library extracted from TripBuilder.jl.

## Features

- **SSIM ingest** — streaming fixed-width parser for OAG/SSIM Type 1-5 records, with EDF expansion, codeshare (DEI 50) resolution, and segment building
- **NewSSIM CSV ingest** — alternative CSV-based ingest path (`ingest_newssim!`) for denormalized schedule files; auto-detects delimiter (comma, pipe, tab) and handles .gz compression
- **MCT lookup** — Full SSIM8 Chapter 8 matching with 29-level specificity cascade, codeshare indicators, aircraft type, flight number ranges, state geography, date validity, suppression geography, and inter-station (multi-airport city) support
- **Graph-based connection building** — O(n²) rule-chain pass producing `GraphConnection` edges at every station
- **DFS search with pruning** — depth-first traversal with elapsed-time, circuity, direction, and stop-count pruning
- **Trip search with scoring** — multi-leg trip pairing with configurable weighted scoring
- **Multiple output formats** — CSV files, JSON (full and compact), `itinerary_long_format` / `itinerary_wide_format` tables, and `DataFrame`-returning wrappers (`itinerary_legs_df`, `itinerary_summary_df`, `itinerary_pivot_df`) for direct Tables.jl / CSV.jl / Arrow.jl interop
- **Interactive visualizations** — self-contained HTML network map (Leaflet), timeline (D3 Gantt), and trip comparison chart
- **Observability** — structured event log with typed events and JSONL sink, DynaTrace-compatible JSON logging via LoggingExtras TeeLogger, cooperative system metrics polling, Tier 1 instrumentation (rule counters, MCT cascade stats, geographic aggregation)
- **CLI** — `itinsearch search ORD LHR 2026-03-20` with 6 commands (search, trip, build, ingest, info, serve), global flags (`--newssim`, `--delimiter`, etc.), per-invocation parameter overrides, JSON output to stdout or file
- **REST API** — HTTP service with search, trip, station, health, and rebuild endpoints; concurrent request handling via HTTP.jl; lock-protected graph refresh; per-request constraint overrides
- **Compilation** — PrecompileTools workload for fast first-use; PackageCompiler sysimage (0ms load) and standalone app builds

## Installation

```julia
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

Requires Julia 1.10+.

## Input Files

Place in `data/input/` (or configure paths in `SearchConfig`):

| File | Description |
|------|-------------|
| `uaoa_ssim.new.dat` | SSIM schedule (Type 1-5 records) |
| `MCTIMFILUA.DAT` | MCT (Minimum Connecting Time) data |
| `mdstua.txt` | Airport reference table |
| `REGIMFILUA.DAT` | Region-to-airport mapping |
| `aircraft.txt` | Aircraft type reference |
| `oa_control_table.csv` | OA carrier control table |

## Quick Example

```julia
using ItinerarySearch
using Dates

config = SearchConfig()
store  = DuckDBStore()
load_schedule!(store, config)

target = Date(2026, 3, 20)
graph  = build_graph!(store, config, target)

ctx = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
)

refs = itinerary_legs(
    graph.stations,
    StationCode("ORD"),
    StationCode("LHR"),
    target,
    ctx,
)

println("Found $(length(refs)) itineraries")
close(store)
```

## Next Steps

- [Architecture](architecture.md) — system overview, data pipeline, and type hierarchy
- [Getting Started](getting-started.md) — end-to-end tutorial with all major features
- [MCT Lookup](mct-lookup.md) — SSIM Chapter 8 cascade implementation walkthrough
- [API Reference](api/types.md) — complete function and type documentation
- [Itinerary Leg Index](leg-index.md) — compact leg index output format reference

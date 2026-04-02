# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ItinerarySearch.jl is a Julia package for high-performance itinerary building and search. It is a standalone service extracted from TripBuilder.jl, designed to ingest OAG/SSIM schedule data and MCT (Minimum Connecting Time) data, build a flight connection graph, and serve itinerary search queries.

The package targets the Module D itinerary-building responsibility from the C reference project (`../NetworkPlanning-DT.DGZ-Passenger-Reaccomodation-BE`), implemented as a reusable, performance-optimized Julia library.

## Build & Test Commands

```bash
# Activate and instantiate the project
julia --project=. -e 'using Pkg; Pkg.instantiate()'

# Run tests
julia --project=. -e 'using Pkg; Pkg.test()'
# or via Makefile:
make test

# Load the package in REPL
julia --project=. -e 'using ItinerarySearch'

# Run benchmarks
make bench

# Run demo script
make demo

# NewSSIM CSV ingest (alternative to SSIM fixed-width)
julia --project=. bin/itinsearch.jl --newssim data/demo/sample_newssim.csv.gz \
    search ORD LHR 2026-06-15
julia --project=. bin/itinsearch.jl --newssim data/input/schedule.csv --delimiter '|' \
    build --date 2026-06-15
```

Julia 1.10+ required.

## Architecture

### Module Structure

The main module `ItinerarySearch` (`src/ItinerarySearch.jl`) uses standard `include()` in dependency order:

- **types/** — Core type definitions. Included in dependency order:
  - `aliases.jl` — `InlineString`-based domain type aliases (`StationCode`, `AirlineCode`, etc.) for `isbits`-friendly fixed-width strings
  - `enums.jl` — `CEnum.@cenum` types (cabin, traffic restriction codes, etc.)
  - Additional struct types added in later tasks
- **ingest/** — Streaming parsers for SSIM fixed-width files, MCT data, and reference tables. Designed for large files using `Mmap` and byte-range column specs. Also includes `newssim.jl` for CSV ingest of denormalized NewSSIM schedule files (`ingest_newssim!`, `detect_delimiter`).
- **store/** — `DuckDBStore` singleton for ingest and query of all tables. SQL-based post-ingest pipeline (join, enrich, filter).
- **graph/** — Struct-based connection graph (stations, legs, connect points). DFS itinerary search.
- **api/** — High-level search interface and `SearchConfig` (JSON-configurable parameters).
- **observe/** — Logging, metrics, and observability hooks.

### Key Design Principles

**InlineString type aliases** — Domain string types (e.g., `StationCode`, `AirlineCode`) are aliases for `InlineStrings.String7` or similar. This keeps them `isbits`, stack-allocated, and suitable for `Vector` without boxing. Avoids the `StaticString` pitfalls of earlier projects (pointer calls, hangs).

**CEnum enums** — All enums use `CEnum.@cenum` for C-ABI compatibility and bitmask operations. Key types: `Cabin`, `TrafficRestrictionCode`, `CnxType`.

**DuckDB singleton store** — All tabular data (schedules, MCT, airports, regions) flows through a single `DuckDBStore`. Ingest is streaming; queries return `DataFrame` or arrow streams.

**Concrete-typed vectors** — Hot-path structs use concrete element types (e.g., `Vector{Leg}` not `Vector{AbstractLeg}`) to eliminate dynamic dispatch in the O(n²) connection builder and DFS search.

**Rule chains** — Connection and itinerary rules are `Vector{Function}`. Each rule returns `Int` (positive = pass, 0/negative = fail with reason code). Rules are enabled/disabled by including or excluding them from the chain.

**SearchConfig** — JSON-configurable search parameters (window sizes, rule toggles, MCT overrides). Loaded via `JSON3` at startup or per-request.

### Key Types

- `StationCode` / `AirlineCode` / `FlightNumber` — `InlineString` aliases
- `MCT_DD` — Duration in minutes (Int16)
- `Cabin` — CEnum for booking class (F/C/Y etc.)
- `LegRecord` — Flight leg struct with 41 fields using canonical names: `carrier`, `flight_number`, `departure_station`, `arrival_station`, `passenger_departure_time`, `passenger_arrival_time`, `aircraft_type`, `distance`, etc.
- `GraphStation` — Airport node with departures/arrivals/connections vectors
- `GraphConnection` — Connection between two legs with MCT, status bitmask
- `Itinerary` — Sequence of GraphConnections
- `SearchConfig` — JSON-deserialized search parameters

### Data Pipeline

**SSIM fixed-width path (default):**

1. `ingest_ssim!()` — Stream SSIM fixed-width file into DuckDB
2. `ingest_mct!()` — Stream MCT file into DuckDB
3. `load_reference_tables!()` — Load airports, regions, aircraft from reference files
4. SQL post-ingest pipeline — Join, enrich, filter in DuckDB
5. `build_graph!(store, config, date)` — Materialize graph from DuckDB query results
6. `build_connections!()` — Apply rule chain to build `GraphConnection` graph
7. `search_itineraries()` — DFS search from origin to destinations

**NewSSIM CSV path (alternative):**

1. `ingest_newssim!(store, path; delimiter=nothing)` — Load denormalized CSV into DuckDB `newssim` table (auto-detects delimiter; supports .gz)
2. `ingest_mct!()` — MCT ingest (same as SSIM path)
3. `build_graph!(store, config, date; source=:newssim)` — Queries `newssim` table instead of SSIM pipeline tables
4. Connection building and search proceed identically

## Development Notes

- Tests live in `test/runtests.jl`; use `@testset` blocks per subsystem
- Benchmarks live in `benchmark/run_benchmarks.jl` using `Chairmarks`
- Demo script at `scripts/demo.jl` runs the full pipeline on `data/demo/` files
- Reference specs: `../TripBuilder/docs/reference/` (SSIM layouts, MCT layouts, TRC codes)
- Reference implementation: `../TripBuilder/` (working Module D pipeline)

## Docstring Style

Nontrivial functions (not obvious one-liners) should have docstrings. Template:

```julia
"""
    `function name(arg0::T, arg1::T; kwarg1::T=default)::ReturnType`
---

# Description
- Brief purpose in bulleted list
- Note mutations (!) and surprising behavior

# Arguments
1. `arg0::T`: description
2. `arg1::T`: description

# Keyword Arguments
- `kwarg1::T=default`: description (first section to cut if docstring is too long)

# Returns
- `::ReturnType`: description

# Examples
```julia
julia> ret = name(a0, a1);
```
"""
```

- Signature in backticks, include `function` keyword if used
- Also add docstrings for complex types (structs, enums) and modules

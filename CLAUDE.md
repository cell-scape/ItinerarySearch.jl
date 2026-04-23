# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# Your Role

You are a professional software engineer specializing in Julia. You are excellent at identifying tradeoffs, and always like to supply more than one alternative to a problem. You are suspicious of your own output until it can be verified. You are cordial but do not accept ideas simply because I supplied them. I want to know if my ideas are sound, and am equally suspicious of my own ideas until they are proven. You are concerned about codebases that are written but never read, and ensure that they are well documented and have educational and tutorial content for quick and effective onboarding and training on maintainance of the code.

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

# Run NewSSIM demo (CSV ingest path)
make demo-newssim

# NewSSIM CSV ingest (alternative to SSIM fixed-width)
julia --project=. bin/itinsearch.jl --newssim data/demo/sample_newssim.csv.gz \
    search ORD LHR 2026-06-15
julia --project=. bin/itinsearch.jl --newssim data/input/schedule.csv --delimiter '|' \
    build --date 2026-06-15
```

Julia 1.10+ required.

### MCT Audit Inspector

```bash
# Interactive inspector (plain text)
make mct-inspect FILE=data/input/UA_Misconnect_Report.csv

# Interactive inspector (Term.jl styled — colored panels and tables)
make mct-inspect-styled FILE=data/input/UA_Misconnect_Report.csv

# Key commands: x (detail view), x nn (operating fallback), i (sorted cascade),
#   b (back), c (next), s N (skip), m (filter mismatches), d (auto-detail), h (help)
```

## Architecture

### Module Structure

The main module `ItinerarySearch` (`src/ItinerarySearch.jl`) uses standard `include()` in dependency order:

- **types/** — Core type definitions. Included in dependency order:
  - `aliases.jl` — `InlineString`-based domain type aliases (`StationCode`, `AirlineCode`, etc.) for `isbits`-friendly fixed-width strings
  - `enums.jl` — `CEnum.@cenum` types (cabin, traffic restriction codes, etc.)
  - Additional struct types added in later tasks
- **ingest/** — Streaming parsers for SSIM fixed-width files, MCT data, and reference tables. Designed for large files using `Mmap` and byte-range column specs. Also includes `newssim.jl` for CSV ingest of denormalized NewSSIM schedule files (`ingest_newssim!`, `detect_delimiter`) and `newssim_materialize.jl` for materializing NewSSIM data into graph-ready leg records.
- **store/** — `DuckDBStore` singleton for ingest and query of all tables. SQL-based post-ingest pipeline (join, enrich, filter).
- **graph/** — Struct-based connection graph (stations, legs, connect points). DFS itinerary search.
- **audit/** — MCT audit tooling: misconnect replay, interactive inspector, audit logging.
- **api/** — High-level search interface and `SearchConfig` (JSON-configurable parameters).
- **ext/** — Package extensions. `TermExt.jl` provides Term.jl styled rendering for the MCT inspector (loaded automatically when Term.jl is available).
- **search/** — Universe-enumeration strategies (e.g. `_universe_from_carriers_direct`,
  `_universe_from_carriers_connected`) that feed `search_schedule` — a carrier-scoped
  schedule-wide sweep with two universe modes (`:direct` and `:connected`) and an
  optional `sink::Function` callback for streaming results.
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
- `GraphConnection` — Connection between two legs with MCT, status bitmask, `MCTResult`
- `Itinerary` — Sequence of GraphConnections
- `ConnectionRef` — Compact MCT audit reference (station, cnx_time, mct_time, mct_source, mct_status, mct_id)
- `ItineraryRef` — Serializable itinerary with `legs::Vector{LegKey}` and `connections::Vector{ConnectionRef}`
- `ParameterSet` — Tunable parameters for connection/itinerary validation: numeric ranges (min/max connection time, elapsed, flight time, layover, distance, circuity, stops), categorical allow/deny sets (carriers, countries, regions, stations, aircraft, service types), and circuity extra miles (domestic/international)
- `SearchConfig` — JSON-deserialized search parameters (includes `mct_codeshare_mode`, `mct_schengen_mode`, `mct_serial_ascending`, `mct_suppressions_enabled`, `maft_enabled`, `interline_dcnx_enabled`, `crs_cnx_enabled`)
- `MarketUniverse` — Public struct wrapping a concrete `Vector{Tuple{String,String,Date}}`
  of markets to search. Produced by enumeration strategies; consumed by `search_schedule`.
- `build_graph_for_window(store, config, dates)` — Build a graph covering every
  target date in a range. Used to amortize graph-build cost across multi-date
  sweeps when the search phase dominates.

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

## Documentation

When code changes are made, update any documentation pertaining to that code if necessary.

Update the @docs/dev-diary.md file with a brief summary when we make significant changes to the project.

## State Snapshot

When closing a session, write a snapshot of the current state to the file PREV_SESSION.md so that it is easy to pick up where we left off.
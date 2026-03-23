# ItinerarySearch.jl

High-performance itinerary building and search for airline schedules.

ItinerarySearch.jl ingests OAG/SSIM schedule data and MCT (Minimum Connecting Time) tables, builds an in-memory flight connection graph, and serves multi-stop itinerary search queries. It is designed as a reusable, performance-optimized Julia library implementing the Module D itinerary-building responsibility.

## Features

- **SSIM ingest** — streaming fixed-width parser for OAG/SSIM Type 1-5 records, with EDF expansion, codeshare (DEI 50) resolution, and segment building
- **MCT lookup** — SSIM8 specificity cascade with station-standard, exception, and global-default fallback tiers
- **Graph-based connection building** — O(n²) rule-chain pass producing `GraphConnection` edges at every station
- **DFS search with pruning** — depth-first traversal with elapsed-time, circuity, direction, and stop-count pruning; optional Layer 1 one-stop pre-computation for repeated searches
- **Trip search with scoring** — multi-leg trip pairing with configurable weighted scoring (`TripScoringWeights`)
- **Multiple output formats** — PSV files, JSON (full and compact with `ItineraryRef` summary), `itinerary_long_format` / `itinerary_wide_format` tables
- **Interactive visualizations** — self-contained HTML network map (Leaflet), timeline (D3 Gantt), and trip comparison (D3 stacked bar)
- **DuckDB singleton store** — all tabular data flows through a single `DuckDBStore`; SQL post-ingest pipeline handles joins, enrichment, and filtering

## Quick Start

```julia
using ItinerarySearch
using Dates

# 1. Load the schedule (SSIM + MCT + reference tables → DuckDB)
config = SearchConfig()         # defaults point to data/input/
store  = DuckDBStore()
load_schedule!(store, config)

# 2. Build the flight graph for a target date
target = Date(2026, 3, 20)
graph  = build_graph!(store, config, target)

# 3. Create a search context
ctx = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
)

# 4. Search a single O-D pair
refs = itinerary_legs(
    graph.stations,
    StationCode("ORD"),
    StationCode("LHR"),
    target,
    ctx,
)
# refs is a Vector{ItineraryRef}, sorted by stops → elapsed → distance

# 5. Write JSON for multiple O-D pairs
json = itinerary_legs_json(graph.stations, ctx;
    origins      = ["ORD", "DEN"],
    destinations = ["LHR", "SFO"],
    dates        = target,
)
write("data/output/itineraries.json", json)

close(store)
```

## Architecture

The pipeline flows from raw files through DuckDB into an in-memory graph, then into search and output:

```
SSIM file ──┐
MCT file  ──┼──► DuckDB Store ──► FlightGraph ──► DFS Search ──► Output
Ref tables ─┘
```

See [docs/src/architecture.md](docs/src/architecture.md) for the full Mermaid diagram, type hierarchy, and design-decision rationale.

## Key Types

**Record layer** (DuckDB bridge, immutable, `isbits`-friendly):

| Type | Purpose |
|------|---------|
| `LegRecord` | Full 41-field flight leg from SSIM ingest |
| `StationRecord` | Airport reference data (coordinates, timezone, region) |
| `SegmentRecord` | Precomputed segment-level aggregates |
| `MCTResult` | MCT cascade result with source and specificity |
| `LegKey` | Compact cross-reference to a leg (row_number + flight identity) |
| `ItineraryRef` | Serializable itinerary summary with ordered `Vector{LegKey}` |

**Graph layer** (mutable, pointer-linked):

| Type | Purpose |
|------|---------|
| `GraphStation` | Airport node; holds `departures`, `arrivals`, `connections` |
| `GraphLeg` | Flight leg node; holds `connect_to` / `connect_from` edges |
| `GraphSegment` | Segment node grouping legs sharing the same flight identity |
| `GraphConnection` | Connection edge between two legs at a station, with MCT and status |
| `Itinerary` | Ordered sequence of `GraphConnection` edges |
| `Trip` | Multi-leg booking container grouping one or more `Itinerary` objects |
| `TripLeg` | One segment of a multi-leg trip search request |
| `TripScoringWeights` | Configurable weights for trip scoring |
| `FlightGraph` | Top-level container for the entire materialized network |

**Search context**:

| Type | Purpose |
|------|---------|
| `RuntimeContext` | Per-thread mutable search state (rule chains, caches, results) |
| `SearchConstraints` | Global defaults plus per-market `ParameterSet` overrides |
| `MarketOverride` | Per-market parameter override with specificity cascade |
| `ParameterSet` | All tunable parameters for connection and itinerary validation |

## Configuration

`SearchConfig` is immutable and JSON-loadable. Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_stops` | `2` | Maximum intermediate stops (0 = nonstop only) |
| `leading_days` | `2` | Days before target date included in schedule window |
| `trailing_days` | `0` | Days after target date included in schedule window |
| `max_connection_minutes` | `480` | Maximum connection time (8 hours) |
| `max_elapsed_minutes` | `1440` | Maximum total elapsed time (24 hours) |
| `circuity_factor` | `2.5` | Maximum ratio of flown distance to great-circle distance |
| `circuity_extra_miles` | `500.0` | Flat mileage tolerance added to circuity threshold |
| `scope` | `SCOPE_ALL` | `SCOPE_DOM`, `SCOPE_INTL`, or `SCOPE_ALL` |
| `interline` | `INTERLINE_CODESHARE` | `INTERLINE_ONLINE`, `INTERLINE_CODESHARE`, or `INTERLINE_ALL` |
| `distance_formula` | `:haversine` | `:haversine` or `:vincenty` |
| `allow_roundtrips` | `false` | When `true`, round-trip paths are split at the farthest point |

Load from JSON:

```julia
config = load_config("config/defaults.json")
```

## Output Formats

| Format | Function | Description |
|--------|----------|-------------|
| `Vector{ItineraryRef}` | `itinerary_legs` | Single O-D, sorted/deduped compact index |
| Nested Dict | `itinerary_legs_multi` | Multiple O-D pairs, keyed by date → origin → dest |
| JSON string | `itinerary_legs_json` | Same as multi, full or compact, for external consumers |
| PSV (legs) | `write_legs` | All valid legs for a date, pipe-delimited |
| PSV (itineraries) | `write_itineraries` | One row per leg per itinerary |
| PSV (trips) | `write_trips` | One row per leg per itinerary per trip |
| Long format | `itinerary_long_format` | `Vector{NamedTuple}`, one row per leg |
| Wide format | `itinerary_wide_format` | `Vector{NamedTuple}`, one row per itinerary |
| Network map | `viz_network_map` | Self-contained HTML with Leaflet map |
| Timeline | `viz_timeline` | Self-contained HTML D3 Gantt-style chart |
| Trip comparison | `viz_trip_comparison` | Self-contained HTML D3 stacked bar chart |

## Benchmarks

Measured on the full United Airlines / OA carrier schedule (demo dataset):

| Phase | Time |
|-------|------|
| Graph build (`build_graph!`) | ~5 s |
| DFS search per O-D (e.g. ORD→LHR) | 9–31 ms |
| Layer 1 build (optional, multi-threaded) | ~2.5 s |
| Test suite | 1000+ assertions, instant feedback |

Search times vary by network density. Hub-to-hub international pairs with many 2-stop paths take longer than short domestic pairs.

## Installation

```julia
# From the project directory:
julia --project=. -e 'using Pkg; Pkg.instantiate()'

# Run tests:
make test

# Run the demo pipeline on data/demo/:
make demo

# Run benchmarks:
make bench
```

Requires Julia 1.10+.

## Input Files

Place in `data/input/`:

| File | Description |
|------|-------------|
| `uaoa_ssim.new.dat` | SSIM schedule (Type 1-5 records) |
| `MCTIMFILUA.DAT` | MCT (Minimum Connecting Time) data |
| `mdstua.txt` | Airport reference table (MDSTUA format) |
| `REGIMFILUA.DAT` | Region-to-airport mapping |
| `aircraft.txt` | Aircraft type reference |
| `oa_control_table.csv` | OA carrier control table |

## License

See LICENSE file.

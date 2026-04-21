# ItinerarySearch.jl

High-performance itinerary building and search for airline schedules.

ItinerarySearch.jl ingests OAG/SSIM schedule data and MCT (Minimum Connecting Time) tables, builds an in-memory flight connection graph, and serves multi-stop itinerary search queries — as a library, CLI tool, or REST API service. Designed as a reusable, performance-optimized Julia package for the Module D itinerary-building responsibility.

## Features

- **SSIM ingest** — streaming fixed-width parser for OAG/SSIM Type 1-5 records, with EDF expansion, codeshare (DEI 50) resolution, and segment building
- **NewSSIM CSV ingest** — alternative CSV-based ingest path (`ingest_newssim!`) for denormalized schedule files; auto-detects delimiter (comma, pipe, tab) and handles .gz compression
- **DataFrame ingest** — all ingest functions accept `AbstractDataFrame` directly, so library users with in-memory data can skip file I/O (`ingest_newssim!`, `ingest_mct!`, `load_airports!`, `load_regions!`, `load_aircrafts!`, `load_oa_control!`)
- **MCT lookup** — full SSIM8 Chapter 8 matching with 29-level specificity cascade, codeshare indicators, aircraft type, flight number ranges, state geography, date validity, suppression geography, and inter-station (multi-airport city) support; result cache with revalidation (~77% hit rate)
- **Graph-based connection building** — O(n²) rule-chain pass producing `GraphConnection` edges at every station; Tuple-dispatched rules for fully specialized compilation
- **DFS search with pruning** — depth-first traversal with elapsed-time, circuity, direction, and stop-count pruning
- **Trip search with scoring** — multi-leg trip pairing with configurable weighted scoring (`TripScoringWeights`)
- **Multiple output formats** — CSV files, JSON (full and compact with `ItineraryRef` summary), `itinerary_long_format` / `itinerary_wide_format` tables
- **Interactive visualizations** — self-contained HTML network map (Leaflet), timeline (D3 Gantt), and trip comparison (D3 stacked bar)
- **MCT audit inspector** — interactive REPL for stepping through misconnect reports with unified leg+MCT detail tables, sorted cascade browser (best match first), codeshare option selector (`x yy/yn/ny/nn`), Schengen/EUR region fallback, and connection-station suppression geography. Optional Term.jl extension for colored panels (`make mct-inspect-styled`)
- **Observability** — structured event log with typed events and JSONL sink, DynaTrace-compatible JSON logging via LoggingExtras TeeLogger, cooperative system metrics polling, Tier 1 instrumentation (rule counters, MCT cascade stats, geographic aggregation)
- **CLI** — `itinsearch` with 6 commands (search, trip, build, ingest, info, serve), global flags, per-invocation parameter overrides
- **REST API** — HTTP service with search, trip, station, health, and rebuild endpoints; concurrent request handling; lock-protected graph refresh; per-request constraint overrides
- **Compilation** — PrecompileTools workload for fast first-use; PackageCompiler sysimage (0ms load) and standalone app builds
- **DuckDB store** — all tabular data flows through a single `DuckDBStore`; SQL post-ingest pipeline handles joins, enrichment, and filtering

See [`docs/diagrams/`](docs/diagrams/) for editable drawio diagrams of the
module layout, ingest pipelines, search workflow, entry points, and data model.

## Quick Start

### Library Usage

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

refs = itinerary_legs(graph.stations, StationCode("ORD"), StationCode("LHR"), target, ctx)
println("Found $(length(refs)) itineraries")

json = itinerary_legs_json(graph.stations, ctx;
    origins = ["ORD", "DEN"], destinations = ["LHR", "SFO"], dates = target)
write("data/output/itineraries.json", json)

close(store)
```

### NewSSIM Library Usage

The `search_markets` convenience wrapper handles store creation, ingest, graph building, and cleanup in one call — just supply files, markets, dates, and any `SearchConfig` overrides:

```julia
using ItinerarySearch
using Dates

results = search_markets("data/demo/sample_newssim.csv.gz";
    markets  = [("ORD","LHR"), ("DEN","LAX"), ("IAH","EWR")],
    dates    = [Date(2026, 2, 26)],
    mct_path = "data/demo/mct_demo.dat",
    max_stops = 2,
)

# Results are keyed by (origin, dest, date)
for ((org, dst, date), itns) in results
    println("$(org)→$(dst) $(date): $(length(itns)) itineraries")
    for itn in itns
        println("  $(itn.num_stops)-stop, $(itn.elapsed_time) min, circuity $(round(itn.circuity; digits=2))x")
    end
end
```

For finer control (reusing a store, customising constraints, or searching the same graph repeatedly), use the lower-level API directly:

```julia
using ItinerarySearch
using Dates

config = SearchConfig(max_stops=2)
store  = DuckDBStore()

ingest_newssim!(store, "data/demo/sample_newssim.csv.gz")
ingest_mct!(store, "data/demo/mct_demo.dat")

ctx = RuntimeContext(
    config=config, constraints=SearchConstraints(),
    itn_rules=build_itn_rules(config),
)

for target in [Date(2026, 2, 26)]
    graph = build_graph!(store, config, target; source=:newssim)
    for (origin, dest) in [(StationCode("ORD"), StationCode("LHR")),
                           (StationCode("DEN"), StationCode("LAX"))]
        itns = search_itineraries(graph.stations, origin, dest, target, ctx)
        println("$(origin)→$(dest) $(target): $(length(itns)) itineraries")
    end
end

close(store)
```

> **Note:** `search_itineraries` returns a reference to `ctx.results` — call `copy(itns)` if you need to retain results across multiple search calls.

### CLI

```bash
# Search
julia --project=. bin/itinsearch.jl search ORD LHR 2026-03-20
julia --project=. bin/itinsearch.jl search ORD,DEN LHR,LAX 2026-03-20 --compact

# Trip (round-trip with min stay)
julia --project=. bin/itinsearch.jl trip ORD LHR 2026-03-20 LHR ORD 2026-03-27 --min-stay 720

# Build graph only
julia --project=. bin/itinsearch.jl build --date 2026-03-20

# Info / Ingest
julia --project=. bin/itinsearch.jl info
julia --project=. bin/itinsearch.jl ingest

# With overrides
julia --project=. bin/itinsearch.jl search ORD LHR 2026-03-20 \
    --max-stops 3 --scope intl --output results.json --log-level debug

# NewSSIM CSV ingest (alternative to SSIM fixed-width)
julia --project=. bin/itinsearch.jl --newssim data/demo/sample_newssim.csv.gz \
    search ORD LHR 2026-06-15
julia --project=. bin/itinsearch.jl --newssim data/input/schedule.csv --delimiter '|' \
    build --date 2026-06-15
```

### REST API

```bash
# Start the server
julia --project=. bin/itinsearch.jl serve --date 2026-03-20 --port 8080

# Search
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{"origins":["ORD"],"destinations":["LHR"],"dates":["2026-03-20"]}'

# Trip search
curl -X POST http://localhost:8080/trip \
  -H "Content-Type: application/json" \
  -d '{"legs":[{"origin":"ORD","destination":"LHR","date":"2026-03-20"}]}'

# Station info / Health / Rebuild
curl http://localhost:8080/station/ORD
curl http://localhost:8080/health
curl -X POST http://localhost:8080/rebuild
curl -X POST http://localhost:8080/rebuild -d '{"source":"newssim"}'
```

## Architecture

```
SSIM file ──┐
NewSSIM CSV ┼──► DuckDB Store ──► FlightGraph ──► DFS Search ──► Output
MCT file  ──┤                                                     ├── JSON / CSV
Ref tables ─┘                                                     ├── CLI (stdout)
                                                                  └── REST API (HTTP)
```

See [docs/src/architecture.md](docs/src/architecture.md) for the full Mermaid diagram, type hierarchy, and design rationale.

## Key Types

**Record layer** (DuckDB bridge, immutable, `isbits`-friendly):

| Type | Purpose |
|------|---------|
| `LegRecord` | Full 41-field flight leg from SSIM ingest |
| `StationRecord` | Airport reference data (coordinates, timezone, region) |
| `SegmentRecord` | Precomputed segment-level aggregates |
| `MCTResult` | MCT cascade result with source, specificity, and matched_fields |
| `LegKey` | Compact cross-reference to a leg (row_number + flight identity + operating_date + departure_time) |
| `ItineraryRef` | Serializable itinerary summary with ordered `Vector{LegKey}` |

**Graph layer** (mutable, pointer-linked):

| Type | Purpose |
|------|---------|
| `GraphStation` | Airport node; holds `departures`, `arrivals`, `connections` |
| `GraphLeg` | Flight leg node; holds `connect_to` / `connect_from` edges + `nonstop_cp` |
| `GraphSegment` | Segment node grouping legs sharing the same flight identity |
| `GraphConnection` | Connection edge between two legs at a station, with MCT and status |
| `Itinerary` | Ordered sequence of `GraphConnection` edges |
| `Trip` | Multi-leg booking container grouping one or more `Itinerary` objects |
| `FlightGraph` | Top-level container with `geo_stats` geographic aggregation |

## Configuration

`SearchConfig` is immutable and JSON-loadable. Key parameters:

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_stops` | `2` | Maximum intermediate stops |
| `leading_days` | `2` | Days before target date in schedule window |
| `trailing_days` | `0` | Days after target date in schedule window |
| `max_connection_minutes` | `480` | Maximum connection time (8 hours) |
| `circuity_factor` | `2.5` | Maximum ratio of flown to great-circle distance |
| `scope` | `SCOPE_ALL` | `SCOPE_DOM`, `SCOPE_INTL`, or `SCOPE_ALL` |
| `interline` | `INTERLINE_CODESHARE` | `INTERLINE_ONLINE`, `INTERLINE_CODESHARE`, or `INTERLINE_ALL` |
| `distance_formula` | `:haversine` | `:haversine` or `:vincenty` |
| `mct_cache_enabled` | `true` | Cache MCT lookup results during connection build |
| `mct_serial_ascending` | `false` | MCT tiebreaker: `false` = higher serial (later record, matches production); `true` = lower serial (earlier record) |
| `mct_codeshare_mode` | `:both` | `:both`, `:marketing`, or `:operating` carrier lookup |
| `mct_schengen_mode` | `:sch_then_eur` | SCH/EUR region priority: `:sch_then_eur`, `:eur_then_sch`, `:sch_only`, `:eur_only` |
| `mct_suppressions_enabled` | `true` | Include MCT suppression records (`false` = ignore) |
| `maft_enabled` | `true` | Enable MAFT (Maximum Allowable Flying Time) rule |
| `interline_dcnx_enabled` | `true` | Enable interline double-connect pattern restriction |
| `crs_cnx_enabled` | `true` | Enable CRS distance-based max connection time rule |
| `log_level` | `:info` | Log level (`:debug`, `:info`, `:warn`, `:error`) |
| `log_json_path` | `""` | DynaTrace-compatible JSON log file (empty = disabled) |
| `event_log_enabled` | `false` | Structured event log with JSONL sink |

Constraints (numeric ranges, categorical allow/deny filters) are configured via `SearchConstraints` / `ParameterSet` and loaded from JSON via `load_constraints("config.json")`. See [Getting Started](docs/src/getting-started.md) for examples.

## Benchmarks

Measured on the full United Airlines / OA carrier schedule:

| Phase | Time | Notes |
|-------|------|-------|
| Graph build | ~5.6s | 823 stations, 25K legs, 1.9M connections |
| Connection pairs | 493 ns/pair | 11.4M pairs, Tuple-dispatched rules |
| MCT lookup | 105 ns | 77% cache hit rate |
| DFS search (ORD→LHR) | 43 ms | 1,903 itineraries |
| DFS search (DEN→LAX) | 14 ms | 798 itineraries |
| JSON output (5 ODs) | 125 ms | 8,610 itineraries |
| Test suite | 1,413 tests | ~1 min |

## Installation

```bash
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

Requires Julia 1.10+.

## Make Targets

```bash
make test           # run full test suite (1413 tests)
make demo           # full pipeline on demo data (3 days, 5 OD pairs, all outputs)
make demo-newssim   # NewSSIM CSV ingest demo
make bench          # benchmarks

# CLI
make cli-search ORG=ORD DST=LHR DATE=2026-03-20
make cli-trip LEGS="ORD LHR 2026-03-20 LHR ORD 2026-03-27"
make cli-build DATE=2026-03-20
make cli-ingest
make cli-info

# REST API
make serve DATE=2026-03-20 PORT=8080

# Compilation
make sysimage       # PackageCompiler sysimage (236MB, 0ms load)
make app            # standalone distributable app

# Visualizations and JSON
make search ORG=ORD DST=LHR DATE=2026-03-20
make viz DATE=2026-03-18
make json DATE=2026-03-18 DAYS=3
```

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

## Documentation

- [Architecture](docs/src/architecture.md) — pipeline, type hierarchy, design principles
- [Getting Started](docs/src/getting-started.md) — end-to-end tutorial with all major features
- [API Reference](docs/src/api/types.md) — types, functions, and configuration
- [Development Diary](docs/dev-diary.md) — session-by-session changelog

## License

See LICENSE file.

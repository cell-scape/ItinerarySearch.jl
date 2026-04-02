# Getting Started

This tutorial walks through the complete pipeline from loading a schedule to producing output. All examples assume the demo dataset is installed in `data/input/`.

## Prerequisites

```bash
# Julia 1.10+ required
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

Alternatively, run the full demo end-to-end:

```bash
make demo
```

## Step 1: Load the Schedule

```julia
using ItinerarySearch
using Dates

config = SearchConfig()    # defaults point to data/input/
store  = DuckDBStore()
load_schedule!(store, config)
```

`load_schedule!` runs the full ingest pipeline:

1. Streams the SSIM file into DuckDB (`ingest_ssim!`), expanding EDF date ranges and resolving DEI 50 codeshare supplements
2. Parses MCT records (`ingest_mct!`)
3. Loads airport, region, aircraft, and OA-control reference tables
4. Runs the SQL post-ingest pipeline inside DuckDB: segment building, codeshare joins, and market distance injection

The store is now ready to serve graph-build queries.

### Alternative: NewSSIM CSV Ingest

For denormalized CSV schedule files (comma, pipe, or tab-delimited; .gz supported), use the NewSSIM ingest path:

```julia
store = DuckDBStore()
ingest_newssim!(store, "data/demo/sample_newssim.csv.gz")
ingest_mct!(store, "data/input/MCTIMFILUA.DAT")  # MCT still required

target_date = Date(2026, 6, 15)
graph = build_graph!(store, config, target_date; source=:newssim)
```

Or from the CLI:

```bash
julia --project=. bin/itinsearch.jl --newssim data/demo/sample_newssim.csv.gz \
    search ORD LHR 2026-06-15
```

## Step 2: Build the Flight Graph

```julia
target_date = Date(2026, 3, 20)
graph = build_graph!(store, config, target_date)
```

`build_graph!` materializes the in-memory network:

- Queries schedule-level legs for the window `(target_date - leading_days)` to `(target_date + trailing_days)` (default: 2 days before, 0 days after)
- Creates `GraphStation`, `GraphLeg`, and `GraphSegment` nodes
- Gap-fills missing leg distances from the geodesic formula
- Materializes the MCT lookup from DuckDB
- Runs the O(n²) connection builder to create all `GraphConnection` edges

`graph.build_stats` contains instrumentation counts (total stations, legs, segments, connections, build time).

## Step 3: Create the Search Context

```julia
ctx = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
)
```

`RuntimeContext` holds all per-search mutable state: rule chains, great-circle distance cache, search statistics, and the results accumulator. Create one context per search thread.

## Step 4: Search

### Single O-D Pair

`itinerary_legs` returns a `Vector{ItineraryRef}` sorted by stops, then elapsed time, then distance. Duplicates (same leg sequence on multiple dates) are removed.

```julia
refs = itinerary_legs(
    graph.stations,
    StationCode("ORD"),
    StationCode("LHR"),
    target_date,
    ctx,
)
println("Found $(length(refs)) itineraries")

for (i, ref) in enumerate(refs[1:min(5, length(refs))])
    println("  $i. $(route_str(ref)) — $(ref.num_stops) stops, $(ref.elapsed_minutes) min")
end
```

Each `ItineraryRef` contains:

| Field / Accessor | Description |
|-------|-------------|
| `legs` | `Vector{LegKey}` — ordered leg references (each LegKey carries `operating_date` and `departure_time`) |
| `num_stops` | Number of intermediate stops (0 = nonstop) |
| `elapsed_minutes` | Total elapsed time (minutes, UTC-corrected) |
| `flight_minutes` | Total in-flight block time (minutes) |
| `layover_minutes` | Total ground time at connect points (minutes) |
| `distance_miles` | Total flown distance (statute miles) |
| `circuity` | Ratio of flown distance to great-circle distance |
| `flights_str(ref)` | Human-readable flight chain, e.g., `"UA 920"` or `"UA4247 -> UA 284"` |
| `route_str(ref)` | Station chain, e.g., `"ORD -> LHR"` or `"ORD -> EWR -> LHR"` |
| `origin(ref)` | Origin station code (first leg's org) |
| `destination(ref)` | Destination station code (last leg's dst) |

### Multiple O-D Pairs (Keyword Interface)

`itinerary_legs_multi` accepts single values or vectors for origins, destinations, and dates.

```julia
# Specific O-D pairs — paired by default (ORD→LHR, DEN→LAX)
result = itinerary_legs_multi(graph.stations, ctx;
    origins      = ["ORD", "DEN"],
    destinations = ["LHR", "LAX"],
    dates        = target_date,
)

# Access by date → origin → destination
refs_ord_lhr = result[target_date]["ORD"]["LHR"]

# Cross-product: every origin × every destination
result = itinerary_legs_multi(graph.stations, ctx;
    origins      = ["ORD", "DEN"],
    destinations = ["LHR", "LAX"],
    dates        = target_date,
    cross        = true,
)
```

### All Destinations from a Station

Omit `destinations` (or pass `nothing`) to search all reachable stations:

```julia
result = itinerary_legs_multi(graph.stations, ctx;
    origins = "ORD",
    dates   = target_date,
)
# result[target_date]["ORD"] contains all reachable destinations from ORD
```

### Multiple Dates

```julia
result = itinerary_legs_multi(graph.stations, ctx;
    origins      = "ORD",
    destinations = "LHR",
    dates        = [Date(2026, 3, 20), Date(2026, 3, 21), Date(2026, 3, 22)],
)
# result[Date(2026, 3, 20)]["ORD"]["LHR"]  → itineraries for March 20
# result[Date(2026, 3, 21)]["ORD"]["LHR"]  → itineraries for March 21
```

### JSON Output

`itinerary_legs_json` returns a JSON string keyed by date → origin → destination. The default (full) format includes the complete `legs` array per itinerary. The compact format includes only summary fields.

```julia
# Full JSON with leg arrays
json = itinerary_legs_json(graph.stations, ctx;
    origins      = ["ORD", "DEN"],
    destinations = ["LHR", "SFO"],
    dates        = target_date,
)
write("data/output/itineraries_$(target_date).json", json)

# Compact JSON (no leg arrays — faster, smaller)
compact_json = itinerary_legs_json(graph.stations, ctx;
    origins  = "ORD",
    dates    = target_date,
    compact  = true,
)
```

JSON structure:

```json
{
  "2026-03-20": {
    "ORD": {
      "LHR": [
        {
          "flights": "UA 920",
          "route": "ORD -> LHR",
          "origin": "ORD",
          "destination": "LHR",
          "stops": ["ORD", "LHR"],
          "num_stops": 0,
          "elapsed_minutes": 480,
          "flight_minutes": 465,
          "layover_minutes": 0,
          "distance_miles": 3958.0,
          "circuity": 1.0,
          "legs": [
            {
              "row_number": 8234,
              "carrier": "UA",
              "flight_number": 920,
              "departure_station": "ORD",
              "arrival_station": "LHR"
            }
          ]
        }
      ]
    }
  }
}
```

## Step 5: Resolve LegKey Back to Full Records

`LegKey` is a compact cross-reference. Use `resolve_leg` to recover the full `LegRecord` or `GraphLeg`:

```julia
ref = refs[1]
key = ref.legs[1]

# Resolve to GraphLeg (requires graph in memory)
graph_leg = resolve_leg(key, graph)
println(graph_leg.record.passenger_departure_time)   # scheduled departure (minutes since midnight)

# Resolve to LegRecord from DuckDB (works without graph)
leg_record = resolve_leg(key, store)
println(leg_record.aircraft_type)    # equipment type

# Resolve all legs in an ItineraryRef
all_legs = resolve_legs(ref, graph)   # Vector{Union{GraphLeg, Nothing}}
all_records = resolve_legs(ref, store) # Vector{Union{LegRecord, Nothing}}
```

## Step 6: Write CSV Files

```julia
outdir = "data/output/legs_index"
mkpath(outdir)

result = itinerary_legs_multi(graph.stations, ctx;
    origins      = ["ORD", "DEN", "IAH"],
    destinations = ["LHR", "SFO", "LAX"],
    dates        = target_date,
)

for (date, org_dict) in result
    for (org, dst_dict) in org_dict
        for (dst, itinerary_refs) in dst_dict
            fname = joinpath(outdir, "$(org)_$(dst)_$(date).csv")
            open(fname, "w") do io
                println(io, "itinerary,leg_pos,row_number,record_serial,carrier,flight_number,departure_station,arrival_station")
                for (itn_idx, ref) in enumerate(itinerary_refs)
                    for (leg_pos, key) in enumerate(ref.legs)
                        println(io, join([
                            itn_idx, leg_pos,
                            key.row_number, key.record_serial,
                            strip(String(key.carrier)), key.flight_number,
                            strip(String(key.departure_station)), strip(String(key.arrival_station)),
                        ], ","))
                    end
                end
            end
        end
    end
end
```

Alternatively, use `write_legs` and `write_itineraries` for the full CSV format with all schedule fields:

```julia
open("data/output/legs_$(target_date).csv", "w") do io
    n = write_legs(io, graph, target_date)
    println("Wrote $n legs")
end

itineraries = copy(search_itineraries(
    graph.stations,
    StationCode("ORD"),
    StationCode("LHR"),
    target_date,
    ctx,
))

open("data/output/itineraries_ord_lhr.csv", "w") do io
    n = write_itineraries(io, itineraries, graph, target_date)
    println("Wrote $n rows")
end
```

## Step 7: Trip Search

`search_trip` pairs multiple one-way itineraries into a multi-leg trip (e.g., round-trip), applies temporal pairing constraints, scores the combinations, and returns a ranked list.

```julia
legs = [
    TripLeg(
        origin      = StationCode("ORD"),
        destination = StationCode("LHR"),
        date        = Date(2026, 3, 20),
    ),
    TripLeg(
        origin      = StationCode("LHR"),
        destination = StationCode("ORD"),
        date        = Date(2026, 3, 27),
        min_stay    = 1440,   # at least 24 hours between arrival and next departure
    ),
]

trips = search_trip(store, graph, legs, ctx;
    weights     = TripScoringWeights(),
    max_per_leg = 100,
    max_trips   = 1000,
)

println("Found $(length(trips)) trips")
if !isempty(trips)
    best = trips[1]
    println("Best: score=$(round(best.score; digits=1)), $(best.trip_type), $(best.total_elapsed) min")
end
```

`TripScoringWeights` controls the relative importance of each criterion (all minimized, lower score = better):

| Weight field | Default | Criterion |
|-------------|---------|-----------|
| `stops` | 10.0 | Total intermediate stops across all legs |
| `eqp_changes` | 5.0 | Number of equipment type changes |
| `carrier_changes` | 5.0 | Number of marketing carrier changes |
| `flt_no_changes` | 2.0 | Number of flight number changes |
| `elapsed` | 1.0 | Total elapsed time (hours) |
| `block_time` | 0.5 | Total in-flight block time (hours) |
| `layover` | 0.5 | Total layover / connection time (hours) |
| `distance` | 0.1 | Total flown distance (thousands of miles) |
| `circuity` | 3.0 | Average circuity excess above 1.0 |

Write trip results:

```julia
open("data/output/trips_ord_lhr.csv", "w") do io
    n = write_trips(io, trips, graph, Date(2026, 3, 20))
    println("Wrote $n rows")
end
```

## Step 8: Visualizations

All three visualization functions write self-contained HTML files that open directly in a browser.

### Network Map

Renders station markers (sized by departure count) and leg arcs for an operating date. Pass itineraries to highlight specific paths as thick colored arcs.

```julia
viz_network_map(
    "data/viz/network_$(target_date).html",
    graph,
    target_date;
    itineraries = itineraries[1:min(5, length(itineraries))],
    map_mode    = :leaflet,   # or :offline for no tile dependency
)
```

### Itinerary Timeline

Gantt-style view of legs plotted in UTC minutes. Connection gaps appear as dashed rectangles; legs are colored by airline.

```julia
viz_timeline(
    "data/viz/timeline_ord_lhr.html",
    itineraries;
    max_display = 30,
)
```

### Trip Comparison

Stacked horizontal bar chart showing the weighted contribution of each scoring criterion per trip. Useful for understanding why one trip ranks above another.

```julia
viz_trip_comparison(
    "data/viz/trips_comparison.html",
    trips;
    weights = TripScoringWeights(),
    top_n   = 10,
)
```

## Step 9: Observability

### Structured JSON Logging (DynaTrace-Compatible)

Enable DynaTrace-compatible JSON logging by setting `log_json_path`:

```julia
config = SearchConfig(
    log_json_path = "data/output/app.log",  # JSON log file
    log_level     = :debug,                  # :debug, :info, :warn, :error
)
graph = build_graph!(store, config, target_date)
```

Every `@info`, `@debug`, `@warn`, and `@error` message is written as a JSON line:

```json
{"timestamp":"2026-03-23T14:30:00.123Z","severity":"INFO","content":"Built connections","service.name":"ItinerarySearch","attributes":{"total":1234}}
```

For container deployments, also send JSON to stdout:

```julia
config = SearchConfig(log_stdout_json = true)
```

The log level can also be set via environment variable (takes precedence over config):

```bash
ITINERARY_SEARCH_LOG_LEVEL=debug julia --project=. scripts/demo.jl
```

### Event Log (Typed Telemetry)

Enable the structured event log for phase timing and system metrics:

```julia
config = SearchConfig(
    event_log_enabled = true,
    event_log_path    = "data/output/events.jsonl",
)
graph = build_graph!(store, config, target_date)
```

The event log captures `PhaseEvent` (start/end of ingest, MCT materialization, connection build), `SystemMetricsEvent` (memory, GC stats), and `BuildSnapshotEvent` (connection build stats) as typed JSONL records.

## Step 10: CLI

The CLI wraps the entire pipeline in a single command:

```bash
# Search itineraries
julia --project=. bin/itinsearch.jl search ORD LHR 2026-03-20

# Multiple ODs, multiple dates
julia --project=. bin/itinsearch.jl search ORD,DEN LHR,LAX 2026-03-20 2026-03-21

# Round-trip search with scoring
julia --project=. bin/itinsearch.jl trip ORD LHR 2026-03-20 LHR ORD 2026-03-27 --min-stay 720

# Build graph only (warmup/validation)
julia --project=. bin/itinsearch.jl build --date 2026-03-20

# Show table stats
julia --project=. bin/itinsearch.jl info

# With parameter overrides
julia --project=. bin/itinsearch.jl search ORD LHR 2026-03-20 \
    --max-stops 3 --scope intl --compact --output results.json
```

Global flags: `--config`, `--log-level`, `--log-json`, `--quiet`, `--compact`, `--output`.

## Step 11: REST API Server

Start the service:

```bash
julia --project=. bin/itinsearch.jl serve --date 2026-03-20 --port 8080
# or: make serve DATE=2026-03-20
```

The server loads the schedule, builds the graph once, then serves requests concurrently. Each request gets its own `RuntimeContext`; the shared graph is read-only.

### Search

```bash
curl -X POST http://localhost:8080/search \
  -H "Content-Type: application/json" \
  -d '{
    "origins": ["ORD"],
    "destinations": ["LHR"],
    "dates": ["2026-03-20"],
    "max_stops": 2,
    "compact": true
  }'
```

Response: `{"status":"ok","data":{...}}` — same nested structure as `itinerary_legs_json`.

### Trip Search

```bash
curl -X POST http://localhost:8080/trip \
  -H "Content-Type: application/json" \
  -d '{
    "legs": [
      {"origin":"ORD","destination":"LHR","date":"2026-03-20"},
      {"origin":"LHR","destination":"ORD","date":"2026-03-27","min_stay":720}
    ],
    "max_trips": 50
  }'
```

### Other Endpoints

```bash
# Station info
curl http://localhost:8080/station/ORD

# Server health
curl http://localhost:8080/health

# Refresh graph (rebuild in background)
curl -X POST http://localhost:8080/rebuild
curl -X POST http://localhost:8080/rebuild -d '{"date":"2026-03-21"}'
```

### Per-Request Constraint Overrides

The `/search` endpoint accepts optional constraint fields in the request body:

| Field | Default | Maps to |
|-------|---------|---------|
| `max_stops` | 2 | `ParameterSet.max_stops` |
| `max_elapsed` | 1440 | `ParameterSet.max_elapsed` |
| `max_connection` | 480 | `ParameterSet.max_mct_override` |
| `circuity_factor` | 2.0 | `ParameterSet.circuity_factor` |

## Step 12: Compilation

### Sysimage (fast startup, ~0ms load)

```bash
make sysimage
julia --sysimage=build/ItinerarySearch.so --project=. bin/itinsearch.jl search ORD LHR 2026-03-20
```

The sysimage exercises the full pipeline during build, compiling all code paths to native code. Module load drops from ~400ms to 0ms.

### Standalone App

```bash
make app
build/app/bin/itinsearch search ORD LHR 2026-03-20
```

Produces a distributable directory with all dependencies bundled. No Julia installation required on the target machine.

## Complete Example Script

```julia
using ItinerarySearch
using Dates

# Load
config = SearchConfig()
store  = DuckDBStore()
load_schedule!(store, config)

# Build graph
target = Date(2026, 3, 20)
graph  = build_graph!(store, config, target)

# Search context
ctx = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
)

# Search all destinations from ORD and DEN, write JSON
json = itinerary_legs_json(graph.stations, ctx;
    origins = ["ORD", "DEN"],
    dates   = target,
)
mkpath("data/output")
write("data/output/all_itineraries_$(target).json", json)
println("Written $(length(json)) bytes")

# Visualize the network
viz_network_map("data/viz/network_$(target).html", graph, target)
println("Network map written")

close(store)
```

# Itinerary Leg Index

The itinerary leg index is the primary compact output format for search results. It returns a `Vector{ItineraryRef}` — one element per unique itinerary — where each `ItineraryRef` holds an ordered sequence of `LegKey` cross-references plus summary metrics.

## What It Does

- **Sorted** by number of stops (fewest first), then elapsed time, then total distance
- **Deduplicated** — identical leg sequences appear only once
- **Filtered** — only itineraries whose first leg actually operates on the requested date
- **Numbered** — each itinerary gets a sequential ID reflecting its rank

Each `LegKey` contains the fields needed to cross-reference with the full schedule or graph.

## Output Fields

### ItineraryRef Summary

| Field | Type | Description |
|-------|------|-------------|
| `flights` | String | Flight chain (e.g., `"UA 920"` or `"UA4247 -> UA 284 -> UA3612"`) |
| `route` | String | Station chain (e.g., `"ORD -> LHR"` or `"LFT -> IAH -> ORD -> YYZ"`) |
| `stops` | `Vector{String}` | Ordered station codes visited |
| `num_stops` | Int | Number of intermediate stops (0 = nonstop) |
| `origin` | String | First departure station |
| `destination` | String | Final arrival station |
| `elapsed_minutes` | Int32 | Total elapsed time (minutes, UTC-corrected) |
| `flight_minutes` | Int32 | Total in-flight block time (minutes) |
| `layover_minutes` | Int32 | Total ground / connection time (minutes) |
| `distance_miles` | Float32 | Total flown distance (miles) |
| `circuity` | Float32 | Ratio of flown distance to great-circle distance |

### LegKey Fields

| Field | Description |
|-------|-------------|
| `row_number` | Unique database row ID for this leg record |
| `record_serial` | SSIM record serial number (bytes 195-200) |
| `carrier` | Marketing carrier IATA code |
| `flight_number` | Marketing flight number |
| `operational_suffix` | SSIM operational suffix |
| `itinerary_var_id` | Itinerary variation number |
| `itinerary_var_overflow` | Itinerary variation overflow character |
| `leg_sequence_number` | SSIM leg sequence number within the flight |
| `service_type` | Service type code |
| `operating_carrier` | Operating carrier (same as `carrier` when operating) |
| `operating_flight_number` | Operating flight number (same as `flight_number` when operating) |
| `departure_station` | Departure station IATA code |
| `arrival_station` | Arrival station IATA code |
| `operating_date` | Operating date (packed YYYYMMDD) |
| `departure_time` | Scheduled departure time (minutes since midnight, local) |

## Functions

| Function | Returns | Use Case |
|----------|---------|----------|
| `itinerary_legs(stations, org, dst, date, ctx)` | `Vector{ItineraryRef}` | Single O-D pair, single date |
| `itinerary_legs_multi(stations, ctx; origins, destinations, dates)` | Nested Dict (date → origin → dest → refs) | Multiple O-Ds, flexible inputs |
| `itinerary_legs_json(stations, ctx; origins, destinations, dates, compact=false)` | `String` (JSON) | Same as multi, for external consumption |
| `viz_itinerary_refs(path, data; title="")` | `Nothing` (writes HTML) | Interactive sortable/filterable HTML table |

## Flexible Input Arguments

All three input arguments (`origins`, `destinations`, `dates`) accept single values or collections:

| Argument | Accepts | Examples |
|----------|---------|----------|
| `origins` | `String`, `StationCode`, or `Vector` of either | `"ORD"`, `StationCode("ORD")`, `["ORD", "DEN"]` |
| `destinations` | `String`, `StationCode`, `Vector`, or `nothing` | `"LHR"`, `["LHR", "SFO"]`, `nothing` (all reachable) |
| `dates` | `Date` or `Vector{Date}` | `Date(2026,3,20)`, `[Date(2026,3,20), Date(2026,3,21)]` |

When `destinations` is omitted or `nothing`, the search finds all reachable destinations from each origin — every station reachable via up to `max_stops` connections.

## Return Structure

`itinerary_legs_multi` returns a nested dictionary:

```
Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}}
```

Keyed by date → origin → destination:

```julia
result[Date(2026, 3, 20)]["ORD"]["LHR"]  # → Vector{ItineraryRef}
result[Date(2026, 3, 20)]["ORD"]["SFO"]  # → Vector{ItineraryRef}
```

`itinerary_legs_json` returns the same structure as a JSON string keyed by date string → origin → destination.

```json
{
  "2026-03-20": {
    "ORD": {
      "LHR": [
        {
          "flights": "UA 920",
          "route": "ORD -> LHR",
          "num_stops": 0,
          "elapsed_minutes": 480,
          "distance_miles": 3958.0,
          "circuity": 1.01,
          "legs": [
            {
              "row_number": 8234, "record_serial": 56789,
              "carrier": "UA", "flight_number": 920,
              "departure_station": "ORD", "arrival_station": "LHR"
            }
          ]
        }
      ]
    }
  }
}
```

## Tutorial

### Prerequisites

```bash
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

### Step 1: Load the Schedule

```julia
using ItinerarySearch
using Dates

config = SearchConfig()
store  = DuckDBStore()
load_schedule!(store, config)
```

### Step 2: Build the Flight Graph

```julia
target_date = Date(2026, 3, 20)
graph = build_graph!(store, config, target_date)
```

### Step 3: Create the Search Context

```julia
ctx = RuntimeContext(
    config      = config,
    constraints = SearchConstraints(),
    itn_rules   = build_itn_rules(config),
)
```

### Step 4: Search

#### Single O-D Pair

```julia
refs = itinerary_legs(
    graph.stations,
    StationCode("DEN"),
    StationCode("LAX"),
    target_date,
    ctx,
)
# Returns Vector{ItineraryRef} — sorted by stops → elapsed → distance
```

#### Multiple O-D Pairs (Keyword Interface)

```julia
# Specific destinations (paired by default)
result = itinerary_legs_multi(graph.stations, ctx;
    origins      = ["DEN", "ORD"],
    destinations = ["LAX", "SFO", "LHR"],
    dates        = Date(2026, 3, 20),
    cross        = true,    # search all origins × all destinations
)

# Access
result[Date(2026, 3, 20)]["DEN"]["LAX"]
```

#### All Destinations from a Station

```julia
result = itinerary_legs_multi(graph.stations, ctx;
    origins = "DEN",
    dates   = Date(2026, 3, 20),
)
# result[Date(2026, 3, 20)]["DEN"] contains all reachable destinations from DEN
```

#### Multiple Dates

```julia
result = itinerary_legs_multi(graph.stations, ctx;
    origins      = "ORD",
    destinations = "LHR",
    dates        = [Date(2026, 3, 20), Date(2026, 3, 21), Date(2026, 3, 22)],
)
```

#### JSON Output

```julia
json = itinerary_legs_json(graph.stations, ctx;
    origins      = ["DEN", "ORD"],
    destinations = ["LAX", "LHR"],
    dates        = Date(2026, 3, 20),
)
write("data/output/itineraries.json", json)

# Compact (no leg arrays — smaller, faster)
compact = itinerary_legs_json(graph.stations, ctx;
    origins  = "ORD",
    dates    = Date(2026, 3, 20),
    compact  = true,
)
```

### Step 5: Write CSV Files

```julia
outdir = "data/output/legs_index"
mkpath(outdir)

result = itinerary_legs_multi(graph.stations, ctx;
    origins      = ["DEN", "ORD", "IAH"],
    destinations = ["LAX", "SFO", "LHR", "EWR"],
    dates        = Date(2026, 3, 20),
)

header = join([
    "itinerary", "leg_pos", "row_number", "record_serial",
    "carrier", "flight_number", "operational_suffix", "itinerary_var_id",
    "leg_sequence_number", "service_type", "operating_carrier", "operating_flight_number",
    "departure_station", "arrival_station",
], ",")

for (date, org_dict) in result
    for (org, dst_dict) in org_dict
        for (dst, itinerary_refs) in dst_dict
            fname = joinpath(outdir, "$(org)_$(dst)_$(date).csv")
            open(fname, "w") do io
                println(io, header)
                for (itn_idx, ref) in enumerate(itinerary_refs)
                    for (leg_pos, key) in enumerate(ref.legs)
                        println(io, join([
                            itn_idx, leg_pos,
                            key.row_number, key.record_serial,
                            strip(String(key.carrier)), key.flight_number,
                            key.operational_suffix, key.itinerary_var_id,
                            key.leg_sequence_number, key.service_type,
                            strip(String(key.operating_carrier)),
                            key.operating_flight_number,
                            strip(String(key.departure_station)), strip(String(key.arrival_station)),
                        ], ","))
                    end
                end
            end
            println("$(org)->$(dst) $(date): $(length(itinerary_refs)) itineraries -> $(fname)")
        end
    end
end
```

### Complete Script

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

# Search all destinations from DEN and ORD, write JSON
json = itinerary_legs_json(graph.stations, ctx;
    origins = ["DEN", "ORD"],
    dates   = target,
)
mkpath("data/output")
write("data/output/all_itineraries_$(target).json", json)
println("Written $(length(json)) bytes")

close(store)
```

## Example Output

```
itinerary,leg_pos,row_number,record_serial,carrier,flight_number,departure_station,arrival_station
1,1,8234,56789,UA,774,DEN,LAX
2,1,8301,56812,UA,2240,DEN,LAX
3,1,8156,56734,UA,1013,DEN,LAX
...
9,1,9102,61234,UA,526,DEN,LAS
9,2,12456,78901,UA,1892,LAS,LAX
```

- Itineraries 1–8: nonstops (1 leg each) — DEN→LAX direct
- Itinerary 9+: 1-stop (2 legs) — DEN→LAS→LAX via Las Vegas
- Later itineraries: 2-stop (3 legs) — e.g., DEN→ORD→SFO→LAX

## Make Commands

| Command | Description |
|---------|-------------|
| `make search ORG=ORD DST=LHR DATE=2026-03-20` | Single O-D: CSV, JSON, HTML table, network map |
| `make search ORG=ORD DATE=2026-03-20` | All destinations from ORG |
| `make viz [DATE=2026-03-18]` | Regenerate HTML visualizations only |
| `make json [DATE=2026-03-18] [DAYS=3]` | Write JSON output only (full + compact) |
| `make demo` | Full end-to-end pipeline on demo dataset |
| `make demo-newssim` | NewSSIM CSV ingest demo |

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `config.max_stops` | `2` | Maximum intermediate stops (0=nonstop only, 1=up to 1-stop, 2=up to 2-stop) |
| `config.leading_days` | `2` | Days before target date to include in schedule window |
| `config.max_connection_minutes` | `480` | Maximum connection time (8 hours) |
| `config.circuity_factor` | `2.5` | Maximum ratio of flown distance to great-circle distance |
| `config.interline` | `INTERLINE_CODESHARE` | Carrier mode: `INTERLINE_ONLINE`, `INTERLINE_CODESHARE`, `INTERLINE_ALL` |
| `config.scope` | `SCOPE_ALL` | `SCOPE_DOM` (domestic only), `SCOPE_INTL` (international only), `SCOPE_ALL` |
| `config.distance_formula` | `:haversine` | `:haversine` or `:vincenty` for geodesic distance computation |

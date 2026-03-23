# Itinerary Leg Index — Quick Reference

## What It Does

The `itinerary_legs` function searches all valid itineraries between an origin and destination on a specific date, then returns a compact index of the legs in each itinerary. The output is:

- **Sorted** by number of stops (fewest first), then elapsed time, then total distance
- **Deduplicated** — identical leg sequences appear only once
- **Filtered** — only itineraries whose first leg actually operates on the requested date
- **Numbered** — each itinerary gets a sequential ID reflecting its rank

Each row contains the flight identity fields needed to cross-reference with the full schedule, plus the codeshare carrier and flight number.

### Output Fields

| Field | Description |
|-------|-------------|
| `itinerary` | Sequential itinerary number (1 = best by stops/duration/distance) |
| `leg_pos` | Position of this leg in the itinerary (1, 2, 3, ...) |
| `row_number` | Unique database row ID for this leg record |
| `record_serial` | SSIM record serial number (bytes 195-200) |
| `airline` | Marketing carrier IATA code |
| `flt_no` | Marketing flight number |
| `operational_suffix` | SSIM operational suffix |
| `itin_var` | Itinerary variation number |
| `leg_seq` | SSIM leg sequence number within the flight |
| `svc_type` | Service type code |
| `codeshare_airline` | Operating carrier (same as airline when operating) |
| `codeshare_flt_no` | Operating flight number (same as flt_no when operating) |
| `org` | Departure station IATA code |
| `dst` | Arrival station IATA code |

---

## End-to-End Tutorial

### Prerequisites

```bash
# Julia 1.10+ required
julia --project=. -e 'using Pkg; Pkg.instantiate()'
```

### Input Files

Place these in `data/input/`:

| File | Description |
|------|-------------|
| `uaoa_ssim.new.dat` | SSIM schedule file (Type 1-5 records) |
| `MCTIMFILUA.DAT` | MCT (Minimum Connecting Time) data |
| `mdstua.txt` | Airport reference table (MDSTUA format) |
| `REGIMFILUA.DAT` | Region-to-airport mapping |
| `aircraft.txt` | Aircraft type reference |
| `oa_control_table.csv` | OA carrier control table |

### Step 1: Load the Schedule

```julia
using ItinerarySearch
using Dates

config = SearchConfig()    # defaults point to data/input/
store = DuckDBStore()
load_schedule!(store, config)
```

This ingests the SSIM schedule, MCT data, and reference tables into DuckDB, then runs the post-ingest pipeline (EDF expansion, codeshare resolution, segment building, market distances).

### Step 2: Build the Flight Graph

```julia
target_date = Date(2026, 3, 20)
graph = build_graph!(store, config, target_date)
```

This queries the schedule for the date window (`target_date - leading_days` to `target_date + trailing_days`), creates stations and legs, gap-fills missing distances, materializes the MCT lookup, and builds all valid connections.

### Step 3: Create the Search Context

```julia
ctx = RuntimeContext(
    config = config,
    constraints = SearchConstraints(),
    itn_rules = build_itn_rules(config),
)
```

### Step 4: Search a Single O-D Pair

```julia
legs = itinerary_legs(
    graph.stations,
    StationCode("DEN"),
    StationCode("LAX"),
    target_date,
    ctx,
)
```

Returns a `Vector{NamedTuple}` — one row per leg per itinerary, sorted and deduplicated.

### Step 5: Write to a PSV File

```julia
mkpath("data/output/legs_index")
open("data/output/legs_index/DEN_LAX_2026-03-20.psv", "w") do io
    # Header
    println(io, join([
        "itinerary", "leg_pos", "row_number", "record_serial",
        "airline", "flt_no", "operational_suffix", "itin_var",
        "leg_seq", "svc_type", "codeshare_airline", "codeshare_flt_no",
        "org", "dst",
    ], "|"))
    # Data
    for r in legs
        println(io, join([
            r.itinerary, r.leg_pos, r.row_number, r.record_serial,
            r.airline, r.flt_no, r.operational_suffix, r.itin_var,
            r.leg_seq, r.svc_type, r.codeshare_airline, r.codeshare_flt_no,
            r.org, r.dst,
        ], "|"))
    end
end
```

### Step 6: Search Multiple O-D Pairs at Once

```julia
od_pairs = [
    (StationCode("DEN"), StationCode("LAX"), Date(2026, 3, 20)),
    (StationCode("ORD"), StationCode("SFO"), Date(2026, 3, 20)),
    (StationCode("ORD"), StationCode("LHR"), Date(2026, 3, 20)),
]

results = itinerary_legs_multi(graph.stations, od_pairs, ctx)

# results is a Dict keyed by (origin, dest, date)
for ((org, dst, date), legs) in results
    println("$(org)→$(dst) on $(date): $(length(legs)) leg rows")
end
```

### Complete Script

```julia
using ItinerarySearch
using Dates

# Load
config = SearchConfig()
store = DuckDBStore()
load_schedule!(store, config)

# Build graph
target = Date(2026, 3, 20)
graph = build_graph!(store, config, target)

# Search context
ctx = RuntimeContext(
    config = config,
    constraints = SearchConstraints(),
    itn_rules = build_itn_rules(config),
)

# Search and write per-OD files
markets = [
    (StationCode("DEN"), StationCode("LAX")),
    (StationCode("ORD"), StationCode("SFO")),
    (StationCode("IAH"), StationCode("EWR")),
    (StationCode("ORD"), StationCode("LHR")),
]

outdir = "data/output/legs_index"
mkpath(outdir)

for (org, dst) in markets
    legs = itinerary_legs(graph.stations, org, dst, target, ctx)
    isempty(legs) && continue

    fname = joinpath(outdir, "$(org)_$(dst)_$(target).psv")
    open(fname, "w") do io
        println(io, join([
            "itinerary", "leg_pos", "row_number", "record_serial",
            "airline", "flt_no", "operational_suffix", "itin_var",
            "leg_seq", "svc_type", "codeshare_airline", "codeshare_flt_no",
            "org", "dst",
        ], "|"))
        for r in legs
            println(io, join([
                r.itinerary, r.leg_pos, r.row_number, r.record_serial,
                r.airline, r.flt_no, r.operational_suffix, r.itin_var,
                r.leg_seq, r.svc_type, r.codeshare_airline, r.codeshare_flt_no,
                r.org, r.dst,
            ], "|"))
        end
    end
    println("$(org)→$(dst): $(length(legs)) rows → $(fname)")
end

close(store)
```

---

## Example Output

```
itinerary|leg_pos|row_number|record_serial|airline|flt_no|operational_suffix|itin_var|leg_seq|svc_type|codeshare_airline|codeshare_flt_no|org|dst
1|1|8234|56789|UA|774| |2|1|J|UA|774|DEN|LAX
2|1|8301|56812|UA|2240| |5|1|J|UA|2240|DEN|LAX
3|1|8156|56734|UA|1013| |3|1|J|UA|1013|DEN|LAX
...
9|1|9102|61234|UA|526| |4|1|J|UA|526|DEN|LAS
9|2|12456|78901|UA|1892| |1|1|J|UA|1892|LAS|LAX
```

- Itineraries 1-8: nonstops (1 leg each) — DEN→LAX direct
- Itinerary 9+: 1-stop (2 legs) — DEN→LAS→LAX via Las Vegas
- Later itineraries: 2-stop (3 legs) — e.g., DEN→ORD→SFO→LAX

---

## Key Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `config.max_stops` | 2 | Maximum intermediate stops (0=nonstop only, 1=up to 1-stop, 2=up to 2-stop) |
| `config.leading_days` | 2 | Days before target date to include in schedule window |
| `config.max_connection_minutes` | 480 | Maximum connection time (8 hours) |
| `config.circuity_factor` | 2.5 | Maximum ratio of flown distance to great-circle distance |
| `config.interline` | `INTERLINE_CODESHARE` | Carrier mode: `INTERLINE_ONLINE`, `INTERLINE_CODESHARE`, `INTERLINE_ALL` |
| `config.scope` | `SCOPE_ALL` | `SCOPE_DOM` (domestic only), `SCOPE_INTL` (international only), `SCOPE_ALL` |
| `config.distance_formula` | `:haversine` | `:haversine` or `:vincenty` for geodesic distance computation |

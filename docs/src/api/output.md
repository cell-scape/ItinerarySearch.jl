# Output and Formats

## Itinerary Leg Index

The primary output interface. Returns compact `ItineraryRef` vectors sorted by stops, elapsed time, and distance, deduplicated by leg-sequence fingerprint.

```@docs
itinerary_legs
itinerary_legs_multi
itinerary_legs_json
```

## Resolution Helpers

Resolve `LegKey` cross-references back to full records or graph nodes.

```@docs
resolve_leg
resolve_segment
resolve_legs
```

## Table Formats

Convert `Vector{Itinerary}` to `Vector{NamedTuple}` for tabular analysis.
Any `Vector{NamedTuple}` is a valid Tables.jl source, so the output flows
directly into `DataFrame`, `CSV.write`, `Arrow.write`, `Parquet.write`, or
any other Tables.jl sink.

```@docs
itinerary_long_format
itinerary_wide_format
```

## DataFrame Output

Thin convenience wrappers that construct `DataFrame`s directly from the
formats above. All three are tidy-data ready: one row per logical record,
typed columns, missing-aware for sparse fields.

```@docs
itinerary_legs_df
itinerary_summary_df
itinerary_pivot_df
```

`itinerary_pivot_df` emits one row per itinerary with `legN_*` and
`cnxN_*` column blocks, padded with `missing` for shorter itineraries.
Its `max_legs` keyword pins the schema — passing an itinerary with more
legs than `max_legs` throws `ArgumentError` rather than silently
truncating. Default `max_legs = 3` matches the project's default
`max_stops = 2` (3 legs = 2 stops + 1 origin leg).

## High-Level Convenience

```@docs
search_markets
```

## CSV File Writers

Write comma-delimited files with canonical column names. All writers return the number of rows written.

```@docs
write_legs
write_itineraries
write_trips
```

### Passthrough Columns

All three writers accept `store::DuckDBStore` and `passthrough_columns::Vector{String}` keyword arguments that append arbitrary columns from the original ingested schedule table to each output row. This is the mechanism for preserving input-CSV fields — business identifiers, operational metadata, anything in the source that isn't among the canonical columns — without plumbing them through the graph structs.

The writer issues one batched SQL query keyed by `row_number` against the source table the graph was built from:

| Graph source | Source table | Key column |
|---|---|---|
| `:newssim` (from `build_graph!(...; source=:newssim)`) | `newssim` | `row_number` |
| `:ssim` (default) | `legs_with_operating` | `row_id` |

Column names are user-supplied strings passed verbatim (case preserved, whitespace trimmed) and appended to the header in the order given. Validation and column-existence are checked before the header is written — an invalid kwarg or missing column raises before any output is produced.

```julia
using ItinerarySearch
using Dates

store = DuckDBStore()
ingest_newssim!(store, "data/demo/sample_newssim.csv.gz")
graph = build_graph!(store, SearchConfig(), Date(2026, 2, 26); source=:newssim)

# Write itineraries with two extra columns preserved from the NewSSIM input
open("itineraries.csv", "w") do io
    write_itineraries(io, itineraries, graph, Date(2026, 2, 26);
                      store = store,
                      passthrough_columns = ["prbd", "DEI_127"])
end
```

Empty `passthrough_columns` (the default) takes a fast path with no store access and produces byte-identical output to earlier versions of the writers.

Error semantics:

- `passthrough_columns` non-empty with `store === nothing` → `ArgumentError`.
- Duplicate or blank column names → `ArgumentError`.
- Column doesn't exist on the source table → DuckDB error propagates; no output is written.
- Row from the graph not found in the source table → cells render as empty strings (lenient fallback).

## Visualizations

All three functions write self-contained HTML files that open directly in a browser with no server required.

```@docs
viz_network_map
viz_timeline
viz_trip_comparison
viz_itinerary_refs
```

## Output Format Reference

### Long Format Fields (`itinerary_long_format`)

One row per leg per itinerary:

| Field | Type | Description |
|-------|------|-------------|
| `itinerary_id` | Int | Sequential itinerary number |
| `leg_seq` | Int | Position of this leg (1-based) |
| `carrier` | String | Marketing carrier IATA code |
| `flight_number` | Int | Marketing flight number |
| `flight_id` | String | Formatted flight identifier (e.g., `"UA 920"`) |
| `record_serial` | Int | SSIM record serial number |
| `segment_hash` | UInt64 | Segment identity hash |
| `departure_station` | String | Departure station IATA code |
| `arrival_station` | String | Arrival station IATA code |
| `passenger_departure_time` | Int | Scheduled passenger departure (minutes since midnight) |
| `passenger_arrival_time` | Int | Scheduled passenger arrival (minutes since midnight) |
| `aircraft_type` | String | Equipment type code |
| `body_type` | Char | Aircraft body type (`'W'` = widebody, `'N'` = narrowbody) |
| `distance` | Float64 | Flown distance (miles) |
| `is_through` | Bool | True when this is a through-service leg |
| `is_nonstop` | Bool | True when this connection is a nonstop self-connection |
| `cnx_time` | Int | Connection time at this leg's origin (minutes; 0 for first leg) |
| `mct` | Int | Minimum connecting time enforced at this connection (minutes; 0 for first leg) |
| `mct_matched_id` | Int | PK of the matched MCT table row for audit traceability (0 = global default or first leg) |
| `mct_matched_fields` | UInt32 | SSIM8 specificity bitmask — which fields the matched record required |
| `departure_terminal` | String | Departure terminal code |
| `arrival_terminal` | String | Arrival terminal code |

### Wide Format Fields (`itinerary_wide_format`)

One row per itinerary:

| Field | Type | Description |
|-------|------|-------------|
| `itinerary_id` | Int | Sequential itinerary number |
| `origin` | String | First departure station |
| `destination` | String | Final arrival station |
| `flights` | String | Flight chain joined with `/` |
| `record_serials` | String | SSIM record serials joined with `/` |
| `num_legs` | Int | Total number of legs |
| `num_stops` | Int | Number of intermediate stops |
| `num_eqp_changes` | Int | Number of equipment type changes |
| `elapsed_time` | Int | Total elapsed time (minutes) |
| `total_distance` | Float64 | Total flown distance (miles) |
| `market_distance` | Float64 | Great-circle O-D distance (miles) |
| `circuity` | Float64 | `total_distance / market_distance` |
| `is_international` | Bool | True when any leg crosses an international border |
| `has_interline` | Bool | True when carriers differ across legs |
| `has_codeshare` | Bool | True when any leg is a codeshare |
| `has_through` | Bool | True when any connection is a through-service |
| `num_metros` | Int | Distinct metro areas traversed |
| `num_countries` | Int | Distinct countries traversed |
| `num_regions` | Int | Distinct IATA regions traversed |

### CSV Itinerary Leg Columns (`write_itineraries`)

Comma-delimited, one row per leg per itinerary. Key columns beyond the long format:

| Column | Description |
|--------|-------------|
| `cnx_type` | `L` = single nonstop leg, `S` = through-segment, `C` = connection |
| `mct_id` | Primary key of the matched MCT rule (0 = global default) |
| `is_operating` | True for the physical operating flight; false for codeshare |
| `operating_carrier`, `operating_flight_number` | Operating carrier (from DEI 50) |
| `dei_10` | Commercial duplicate list |
| `dei_127` | Operating airline disclosure |
| `wet_lease` | True when operated under wet-lease |
| `aircraft_owner` | Aircraft owner IATA code |

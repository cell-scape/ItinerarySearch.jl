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

```@docs
itinerary_long_format
itinerary_wide_format
```

## CSV File Writers

Write comma-delimited files with canonical column names. All writers return the number of rows written.

```@docs
write_legs
write_itineraries
write_trips
```

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
| `mct` | Int | Minimum connecting time (minutes; 0 for first leg) |
| `dep_term` | String | Departure terminal code |
| `arr_term` | String | Arrival terminal code |

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
| `administrating_carrier`, `administrating_carrier_flight_number` | Operating carrier (from DEI 50) |
| `dei_10` | Commercial duplicate list |
| `dei_127` | Operating airline disclosure |
| `wet_lease` | True when operated under wet-lease |
| `aircraft_owner` | Aircraft owner IATA code |

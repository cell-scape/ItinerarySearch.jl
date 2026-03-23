# Ingest

The ingest layer streams raw airline schedule files into a `DuckDBStore`. All functions are designed for large files and use memory-mapped I/O where appropriate.

## Schedule Loading

```@docs
load_schedule!
```

## SSIM Ingest

```@docs
ingest_ssim!
```

## MCT Ingest

```@docs
ingest_mct!
```

## Reference Table Loaders

```@docs
load_airports!
load_regions!
load_aircrafts!
load_oa_control!
```

## Store Interface

```@docs
AbstractStore
DuckDBStore
JuliaStore
post_ingest_sql!
query_schedule_legs
query_schedule_segments
query_legs
query_station
query_mct
get_departures
get_arrivals
query_market_distance
query_segment
query_segment_stops
table_stats
```

## Configuration Loading

```@docs
load_config
```

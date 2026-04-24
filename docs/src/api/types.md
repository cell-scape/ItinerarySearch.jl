# Types

## Core Records

Records are immutable, `isbits`-friendly structs that bridge DuckDB query results and the graph layer. They can be stored in `Vector` without boxing and are safe to pass across threads.

```@docs
LegRecord
StationRecord
SegmentRecord
MCTResult
LegKey
ItineraryRef
```

## Graph Types

Graph types are mutable, pointer-linked structs forming the in-memory flight network. They are built by `build_graph!` and searched by `search_itineraries`.

```@docs
AbstractGraphNode
AbstractGraphEdge
GraphStation
GraphLeg
GraphSegment
GraphConnection
Itinerary
Trip
TripLeg
TripScoringWeights
FlightGraph
```

## Configuration

```@docs
SearchConfig
SearchConstraints
ParameterSet
MarketOverride
MCTAuditConfig
RuntimeContext
```

All five `@kwdef` config structs above have both the canonical keyword
constructor and an `AbstractDict` constructor form. The dict form accepts
`String` or `Symbol` keys, parses enum-valued fields from their string
representations (`"intl"` → `SCOPE_INTL`), recursively constructs nested
struct-typed fields (e.g. `MarketOverride.params` from a nested dict),
and throws `ArgumentError` on unknown keys. See [`config/README.md`](../../../config/README.md)
for the full field reference and JSON schema.

## Enums and Status Bits

```@docs
MCTStatus
MCTSource
Cabin
ScopeMode
InterlineMode
```

## Instrumentation

```@docs
StationStats
BuildStats
SearchStats
MCTSelectionRow
GeoStats
aggregate_geo_stats
merge_build_stats!
merge_station_stats!
```

## Observability: Event Log

```@docs
EventLog
emit!
checkpoint!
with_phase
collect_system_metrics
SystemMetricsEvent
PhaseEvent
BuildSnapshotEvent
SearchSnapshotEvent
CustomEvent
JsonlSink
stdout_sink
```

## Observability: Structured Logging

```@docs
setup_logger
```

The structured logger uses LoggingExtras `TeeLogger` to fan out Julia's standard `@info`/`@debug`/`@warn`/`@error` to both a `ConsoleLogger` (human-readable) and a `FormatLogger` (DynaTrace-compatible JSON). Configuration:

| SearchConfig field | Default | Description |
|-------------------|---------|-------------|
| `log_level` | `:info` | Minimum log level (`:debug`, `:info`, `:warn`, `:error`) |
| `log_json_path` | `""` | Path for JSON log file (empty = disabled) |
| `log_stdout_json` | `false` | Also write JSON to stdout |

Environment variable `ITINERARY_SEARCH_LOG_LEVEL` overrides `log_level`.

## Utility Functions

```@docs
pack_date
unpack_date
flight_id
segment_id
full_id
nonstop_connection
resolve_params
```

## Search Results

```@docs
MarketUniverse
MarketSearchFailure
is_failure
failed_markets
```

## Observability

Span and trace primitives emitted during searches. See the "Observability"
section of `docs/src/getting-started.md` for a walkthrough.

```@docs
SpanEvent
TraceContext
```

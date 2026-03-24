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
OneStopConnection
FlightGraph
```

## Configuration

```@docs
SearchConfig
SearchConstraints
ParameterSet
MarketOverride
RuntimeContext
```

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

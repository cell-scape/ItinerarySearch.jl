# Graph and Search

## Graph Construction

```@docs
build_graph!
build_connections!
build_connections_at_station!
```

## MCT Lookup

```@docs
MCTRecord
MCTLookup
materialize_mct_lookup
lookup_mct
```

## Connection Rules

Connection rules are `Function` values with signature `(from_leg, to_leg, station, ctx) -> Int`.
A positive return value indicates a pass; zero or negative indicates failure with a reason code.

```@docs
build_cnx_rules
MCTRule
MAFTRule
CircuityRule
check_cnx_roundtrip
check_cnx_scope
check_cnx_interline
check_cnx_opdays
check_cnx_suppcodes
check_cnx_trfrest
```

## Itinerary Rules

Itinerary rules are `Function` values with signature `(itn, ctx) -> Int`.

```@docs
build_itn_rules
check_itn_scope
check_itn_opdays
check_itn_circuity
check_itn_suppcodes
check_itn_maft
```

## DFS Search

```@docs
search_itineraries
search
```

## Trip Search

```@docs
search_trip
score_trip
```

## Layer 1 (One-Stop Pre-computation)

Layer 1 is optional. It pre-computes all two-stop (one-via) paths indexed by `(origin, destination)`, accelerating DFS for repeated searches over the same network.

```@docs
build_layer1!
export_layer1!
import_layer1!
export_layer1_parquet!
```

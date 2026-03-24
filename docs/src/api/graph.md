# Graph and Search

## Graph Construction

```@docs
build_graph!
build_connections!
build_connections_at_station!
```

## MCT Lookup

The MCT lookup implements the full SSIM8 Chapter 8 matching hierarchy. Records are matched against a 29-level specificity cascade covering carriers, codeshare indicators, flight number ranges, terminals, station pairs, state/country/region geography, aircraft types, body types, and date validity. Suppressions can be scoped by geographic region.

The lookup key is a `(arr_station, dep_station)` tuple, supporting both intra-station and inter-station (multi-airport city) connections.

```@docs
MCTRecord
MCTLookup
materialize_mct_lookup
lookup_mct
```

### MCT Bitmask Constants

Each `MCT_BIT_*` constant marks a matching field in `MCTRecord.specified`. A set bit means the field was explicitly specified and must match during lookup; an unset bit is a wildcard.

| Constant | Bit | Field |
|----------|-----|-------|
| `MCT_BIT_ARR_CARRIER` | 0 | Arriving carrier |
| `MCT_BIT_DEP_CARRIER` | 1 | Departing carrier |
| `MCT_BIT_ARR_TERM` | 2 | Arrival terminal |
| `MCT_BIT_DEP_TERM` | 3 | Departure terminal |
| `MCT_BIT_PRV_STN` | 4 | Origin of arriving flight |
| `MCT_BIT_NXT_STN` | 5 | Destination of departing flight |
| `MCT_BIT_PRV_COUNTRY` | 6 | Country of arriving flight's origin |
| `MCT_BIT_NXT_COUNTRY` | 7 | Country of departing flight's destination |
| `MCT_BIT_PRV_REGION` | 8 | IATA region of arriving flight's origin |
| `MCT_BIT_NXT_REGION` | 9 | IATA region of departing flight's destination |
| `MCT_BIT_DEP_BODY` | 10 | Departing aircraft body type |
| `MCT_BIT_ARR_BODY` | 11 | Arriving aircraft body type |
| `MCT_BIT_ARR_CS_IND` | 12 | Arriving codeshare indicator |
| `MCT_BIT_ARR_CS_OP` | 13 | Arriving codeshare operating carrier |
| `MCT_BIT_DEP_CS_IND` | 14 | Departing codeshare indicator |
| `MCT_BIT_DEP_CS_OP` | 15 | Departing codeshare operating carrier |
| `MCT_BIT_ARR_ACFT_TYPE` | 16 | Arriving aircraft IATA type code |
| `MCT_BIT_DEP_ACFT_TYPE` | 17 | Departing aircraft IATA type code |
| `MCT_BIT_ARR_FLT_RNG` | 18 | Arriving flight number range |
| `MCT_BIT_DEP_FLT_RNG` | 19 | Departing flight number range |
| `MCT_BIT_PRV_STATE` | 20 | State/province of arriving flight's origin |
| `MCT_BIT_NXT_STATE` | 21 | State/province of departing flight's destination |

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


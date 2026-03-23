# SSIM Service Types (Appendix C)

Service Type is a single alpha character in byte 14 of SSIM Record Type 3.

## Scheduled Services

| Code | Type of Operation | Description |
|------|-------------------|-------------|
| **J** | Passenger | Normal Scheduled Service |
| **S** | Passenger | Shuttle Mode |
| **U** | Passenger | Service operated by Surface Vehicle |
| **F** | Cargo/Mail | Loose loaded cargo and/or preloaded devices |
| **V** | Cargo/Mail | Service operated by Surface Vehicle |
| **M** | Cargo/Mail | Mail only |
| **Q** | Passenger/Cargo | Passenger/Cargo in Cabin (mixed configuration) |

## Additional Flights

| Code | Type of Operation | Description |
|------|-------------------|-------------|
| **G** | Passenger | Normal Service |
| **B** | Passenger | Shuttle Mode |
| **A** | Cargo/Mail | Cargo/Mail |
| **R** | Passenger/Cargo | Passenger/Cargo in Cabin (mixed configuration) |

## Charter

| Code | Type of Operation | Description |
|------|-------------------|-------------|
| **C** | Passenger | Passenger Only |
| **O** | Special Handling | Charter requiring special handling (e.g. Migrants) |
| **H** | Cargo/Mail | Cargo and/or Mail |
| **L** | Passenger/Cargo/Mail | Passenger and Cargo and/or Mail |

## Others

| Code | Type of Operation | Description |
|------|-------------------|-------------|
| **P** | Not specific | Non-revenue (Positioning/Ferry/Delivery/Demo) |
| **T** | Not specific | Technical Test |
| **K** | Not specific | Training (School/Crew check) |
| **D** | Not specific | General Aviation |
| **E** | Not specific | Special (FAA/Government) |
| **W** | Not specific | Military |
| **X** | Not specific | Technical Stop (Chapter 6 only) |
| **I** | Not specific | State/Diplomatic/Air Ambulance (Chapter 6 only) |
| **N** | Not specific | Business Aviation/Air Taxi |

## Passenger Service Types for Connection Building

The types relevant for passenger itinerary building are primarily: **J** (Normal Scheduled), **S** (Shuttle), **G** (Additional), **Q** (Mixed), and **U** (Surface Vehicle).

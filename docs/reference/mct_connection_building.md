# MCT Connection Building Rules

Extracted from the MCT Technical Guide v2.2 and SSIM Chapter 8 (2021).

## Overview

An MCT (Minimum Connecting Time) is the shortest time interval required to transfer a passenger and luggage from one flight to a connecting flight at a specific location. MCTs are processed through a hierarchy to determine whether a connection can be built and what minimum time applies.

## MCT Record Types

1. **Station Standard MCT**: No carrier specified. Published by IATA. Provides the default MCT for a station.
2. **MCT Exception**: At least one carrier specified. Filed bilaterally. Overrides station standards.
3. **MCT Suppression**: `Suppression Indicator = Y`. Blocks specific connections.

If no station standard exists, use the **global defaults**: DD=30min, DI=60min, ID=90min, II=90min, inter-station=4hrs.

## Connection Building Process

For each potential connection (arriving flight → departing flight at a station):

1. **Check Connection Building Filter** (Record Type 3, effective 01NOV22):
   - If the arriving carrier filed a partnership list, check if the departing carrier is on it
   - If the departing carrier filed a partnership list, check if the arriving carrier is on it
   - If BOTH filed lists and EITHER omitted the other, the connection **cannot be built**
   - If a carrier didn't file a list, it allows connections with everyone
   - Check both marketing AND operating carriers (DEI 50)

2. **Find applicable MCT records**: Match on arrival/departure station, carriers, status (DD/DI/ID/II)

3. **Apply hierarchy**: Select the most specific matching MCT using the 28-priority hierarchy

4. **Check suppression**: If the selected MCT is a suppression (`Suppression Indicator = Y`), the connection is blocked

5. **Compare elapsed time**: If `arrival_time + MCT_time <= departure_time`, the connection builds

## Codeshare Handling

Codeshare flights are identified by **DEI 50** on the flight schedule (Operating Airline Disclosure).

- MCTs are established by the **operating carrier** unless overwritten by the marketing carrier
- When processing a codeshare flight, first look for an MCT matching the **marketing carrier** with `Codeshare Indicator = Y`
- If no marketing MCT exists, use the **operating carrier** MCT (identified by DEI 50)
- A marketing (Y) MCT **overrides** an operating MCT
- A marketing MCT is not necessary unless the marketing carrier wants a **longer** MCT than the operating carrier

## Suppression Rules

- A record is a suppression **if and only if** `Suppression Indicator = Y`
- Valid suppression indicator values: `Y` or `N` (blanks converted to `N` by aggregators)
- A suppression is **global** when arrival station, departure station, suppression region, suppression country, and suppression state are ALL empty
- Suppressions can have effective from/to dates
- International/Domestic status is **mandatory** for suppressions
- Suppression overrides (N) at specific stations can reverse a global suppression (Y)

## Flight Number Range Rules

- Flight numbers must be 4 digits with leading zeros
- For a single flight, Range Start = Range End
- If both Carrier and Codeshare Operating Carrier are defined, flight number applies to the **Carrier**
- If only Codeshare Operating Carrier is defined, flight number applies to it
- **Subset ranges** take priority over larger ranges
- **Overlapping ranges** across arrival/departure sides are NOT allowed

## Effective Date Rules

- Format: DDMMMYY (e.g., `03MAR18`, `26OCT17`) — 7 chars, uppercase
- Dates are **local time** at the connection airport
- An MCT with dates takes priority over one without dates (hierarchy levels 26-27)
- Duplicate detection: Records are duplicates only if all hierarchy fields match; any overlap/subset of effective dates on otherwise identical records is rejected

## Aircraft Body Type Rules

- `W` = Widebody, `N` = Narrowbody
- Aircraft Type and Aircraft Body are **mutually exclusive** (cannot both be specified)
- If no body type exists for an aircraft (e.g., TRN), use the aircraft type instead
- Only `W` and `N` are valid body type values

## Connection Building Filter (Effective 01NOV22)

Record Type 3 in the MCT file contains carrier partnership lists.

- Each airline **can but is not required** to publish a list
- **Online connections** (same carrier) are always allowed for the submitting carrier
- The filter is checked **per connection** — each connection in a multi-segment itinerary is checked separately
- The filter **supersedes all other MCT records** when a carrier has filed one
- Both **marketing AND operating carriers** (identified via DEI 50) must pass the filter check

### Multi-segment itinerary example
`JFK (AA) – LHR (BA) – MAD (VY) - BCN`

Each connection checked separately:
- AA→BA: Check AA's list and BA's list
- BA→VY: Check BA's list and VY's list
- VY doesn't file a list, so all carriers allowed for VY's side

## Key Definitions

- **Online connection**: Both flights use the same Airline Designator
- **Interline connection**: The flights have different Airline Designators
- **International connection**: Flights connect between stations in different countries
- **Metropolitan Area**: Traffic restrictions treat all airports within a metro area as equivalent (e.g., JFK and EWR are both NYC)
- **Station Standard (Station Default/Airport Standard)**: MCT with no carrier specified

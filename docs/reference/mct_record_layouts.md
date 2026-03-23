# MCT Record Layouts (SSIM Chapter 8)

All records are 200 bytes. Three record types (four as of 01NOV22 with Connection Building Filter).

## Global Default MCTs
When no station standard or exception MCT is found:
- **DD** (Domestic-Domestic): 0030 (30 min)
- **DI** (Domestic-International): 0100 (60 min)
- **ID** (International-Domestic): 0130 (90 min)
- **II** (International-International): 0130 (90 min)
- **Inter-station** (all status types): 0400 (4 hours)

## Record Type 1 — Header Record (Mandatory)

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `1` |
| 2-31 | Title of Contents | M | Always `MINIMUM CONNECT TIME DATA SET` |
| 32-66 | Creator Reference | M | Free format |
| 67-73 | Creation Date (UTC) | M | DDMMMYY |
| 74-77 | Creation Time (UTC) | M | HHMM |
| 78 | Content Indicator | M | `F`=full file replacement, `U`=updates (adds/deletes) |
| 79-194 | (Spare) | M | Blank fill |
| 195-200 | Record Serial Number | M | Always `000001` |

## Record Type 2 — MCT Record (Mandatory)

The main MCT data record. Each record represents one MCT rule (station standard, exception, or suppression).

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `2` |
| 2-4 | Arrival Station | C | 3-char IATA. Blank for global suppression |
| 5-8 | Time (HHMM) | C | Hours and minutes. Blank for suppression |
| 9-10 | International/Domestic Status | M | Two-char: DD, DI, ID, or II |
| 11-13 | Departure Station | C | 3-char IATA. Blank for global suppression |
| 14-15 | Arrival Carrier | C | 2-char IATA |
| 16 | Arrival Codeshare Indicator | O | `Y` or blank |
| 17-18 | Arrival Codeshare Operating Carrier | C | 2-char IATA |
| 19-20 | Departure Carrier | C | 2-char IATA |
| 21 | Departure Codeshare Indicator | O | `Y` or blank |
| 22-23 | Departure Codeshare Operating Carrier | C | 2-char IATA |
| 24-26 | Arrival Aircraft Type | O | 3-char IATA. Cannot combine with Arrival Aircraft Body |
| 27 | Arrival Aircraft Body | O | `W`=widebody, `N`=narrowbody. Cannot combine with Arrival Aircraft Type |
| 28-30 | Departure Aircraft Type | O | 3-char IATA. Cannot combine with Departure Aircraft Body |
| 31 | Departure Aircraft Body | O | `W` or `N`. Cannot combine with Departure Aircraft Type |
| 32-33 | Arrival Terminal | O | Alphanumeric, left justify |
| 34-35 | Departure Terminal | O | Alphanumeric, left justify |
| 36-37 | Previous Country | C | 2-char ISO. Required when using Previous State. Cannot use with Previous Station/Region |
| 38-40 | Previous Station | O | 3-char IATA. Cannot use with Previous Country/State/Region |
| 41-42 | Next Country | C | 2-char ISO. Required when using Next State. Cannot use with Next Station/Region |
| 43-45 | Next Station | O | 3-char IATA. Cannot use with Next Country/State/Region |
| 46-49 | Arrival Flight Number Range Start | O | 4-digit, leading zeros |
| 50-53 | Arrival Flight Number Range End | C | 4-digit, leading zeros. Same as Start for single flight |
| 54-57 | Departure Flight Number Range Start | O | 4-digit, leading zeros |
| 58-61 | Departure Flight Number Range End | C | 4-digit, leading zeros |
| 62-63 | Previous State | O | 2-char IATA. Country must be present. Cannot use with Previous Station/Region |
| 64-65 | Next State | O | 2-char IATA. Country must be present. Cannot use with Next Station/Region |
| 66-68 | Previous Region | O | 3-char IATA. Cannot use with Previous Country/State/Station |
| 69-71 | Next Region | O | 3-char IATA. Cannot use with Next Country/State/Station |
| 72-78 | Effective From Date (Local) | O | DDMMMYY or blank |
| 79-85 | Effective To Date (Local) | O | DDMMMYY or blank |
| 86 | (Spare) | M | Blank fill |
| 87 | Suppression Indicator | C | `Y` or `N`. Default `N`. Blank when Station Standard |
| 88-90 | Suppression Region | O | IATA region. Blank = global suppression |
| 91-92 | Suppression Country | C | IATA country |
| 93-94 | Suppression State | O | IATA state. Suppression Country must be populated |
| 95-96 | Submitting Carrier Identifier | C | 2-char. Populated by data aggregators. Blank for station standard |
| 97-103 | Filing Date (Local) | C | DDMMMYY. Populated by data aggregators. Informational |
| 104 | Action Indicator | C | `A`=Add, `D`=Delete. Blank for full replacement file |
| 105-194 | (Spare) | M | Blank fill |
| 195-200 | Record Serial Number | M | Sequential |

## Record Type 3 — Connection Building Filter [Effective 01NOV22]

Partnership list for Connection Building Filter. Multiple records per carrier allowed.

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `3` |
| 2-4 | Submitting Airline Designator | M | 2-char, left justified |
| 5-194 | Allow Airline Designators | C | Space for 95 x 2-char airline codes |
| 195-200 | Record Serial Number | M | |

## Record Type 4 — Trailer Record [Effective 01NOV22]

(Was Record Type 3 prior to 01NOV22)

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `4` (was `3` pre-01NOV22) |
| 2-193 | (Spare) | M | Blank fill |
| 194 | End Code | M | Always `E` |
| 195-200 | Serial Number Check Reference | M | Equals previous record's serial number |

## MCT Hierarchy (Priority Order)

Priority is ascending (1 = most specific/highest priority). When hierarchy can't decide between two records, they're considered duplicates.

| Priority | Data Element | Notes |
|----------|-------------|-------|
| 1 | International/Domestic Status | DD, DI, ID, II |
| 2 | Departure Codeshare Indicator | `Y` means codeshare MCT |
| 3 | Departure Carrier | |
| 4 | Departure Codeshare Operating Carrier | |
| 5 | Arrival Codeshare Indicator | |
| 6 | Arrival Carrier | |
| 7 | Arrival Codeshare Operating Carrier | |
| 8 | Departure Flight Number Range Start | Subset overrides larger range |
| 9 | Departure Flight Number Range End | |
| 10 | Arrival Flight Number Range Start | Subset overrides larger range |
| 11 | Arrival Flight Number Range End | |
| 12 | Departure Terminal | |
| 13 | Arrival Terminal | |
| 14 | Next Station | |
| 15 | Previous Station | |
| 16 | Next State | |
| 17 | Previous State | |
| 18 | Next Country | |
| 19 | Previous Country | |
| 20 | Next Region | |
| 21 | Previous Region | |
| 22 | Departure Aircraft Type | |
| 23 | Arrival Aircraft Type | |
| 24 | Departure Aircraft Body | W/N |
| 25 | Arrival Aircraft Body | W/N |
| 26 | Effective From Date | MCT with date takes priority over one without |
| 27 | Effective To Date | |
| 28 | Departure Station | |
| 29 | Arrival Station | |
| # | Suppression Indicator | Not part of hierarchy numbering |
| # | Suppression State/Country/Region | |
| # | Time (HHMM) | |
| # | Filing Date, Submitting Carrier, Action Indicator | Informational |

## MCT Types

1. **Station Standard MCT**: No arrival/departure carrier, no operating carrier. Published by IATA. Overrides global defaults.
2. **MCT Exception**: Has at least one carrier specified. Filed bilaterally between carriers. Overrides station standards.
3. **MCT Suppression**: `Suppression Indicator = Y`. Blocks connections. Can be station-specific or geographic (global).

## Key MCT Application Rules

- **Departure Carrier takes priority** over Arrival Carrier (when both filed)
- **Marketing carrier (codeshare Y)** overrides operating carrier MCT
- **Codeshare flights** are identified by presence of DEI 50 on the flight schedule
- **Subset flight ranges** take priority over larger ranges
- **SCH (Schengen) region** overrides EUR region in tiebreakers
- **Connection Building Filter** (Record Type 3) supersedes all other MCT records when filed
- **Effective dates** are local time at the connection airport
- **Overlapping flight ranges** across arrival/departure sides are not allowed

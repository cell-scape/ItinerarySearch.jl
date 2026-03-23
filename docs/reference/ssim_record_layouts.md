# SSIM Record Layouts (Chapter 7)

All records are 200 bytes, blocked in 5s (1000-byte blocks). Data expressed in EBCDIC or ASCII.

## Record Type 1 — Header Record (Mandatory)

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `1` |
| 2-35 | Title of Contents | M | Always `AIRLINE STANDARD SCHEDULE DATA SET` |
| 36-40 | (Spare) | M | Blank fill |
| 41 | Number of Seasons | O | |
| 42-191 | (Spare) | M | Blank fill |
| 192-194 | Data Set Serial Number | M | |
| 195-200 | Record Serial Number | M | Always `000001` |

## Record Type 2 — Carrier Record (Mandatory)

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `2` |
| 2 | Time Mode | M | `U`=UTC, `L`=Local |
| 3-5 | Airline Designator | M | IATA code, left justified |
| 6-10 | (Spare) | M | Blank fill |
| 11-13 | Season | O | |
| 14 | (Spare) | M | Blank fill |
| 15-28 | Period of Schedule Validity | M | From (15-21) and To (22-28), DDMMMYY |
| 29-35 | Creation Date | M | DDMMMYY |
| 36-64 | Title of Data | O | Free format |
| 65-71 | Release (Sell) Date | O | |
| 72 | Schedule Status | M | `P` or `C` |
| 73-107 | Creator Reference | O | |
| 108 | Duplicate Airline Designator Marker | C | |
| 109-168 | General Information | O | |
| 169 | Secure Flight Indicator | O | `S` if subject to regulations |
| 170-188 | In-Flight Service Information defaults | O | |
| 189-190 | Electronic Ticketing Information | O | `EN`=Not E-Ticket, `ET`=E-Ticket |
| 191-194 | Creation Time | M | HHMM |
| 195-200 | Record Serial Number | M | |

## Record Type 3 — Flight Leg Record (Mandatory)

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `3` |
| 2 | Operational Suffix | C | Blank fill |
| 3-5 | Airline Designator | M | Left justified |
| 6-9 | Flight Number | M | Right justified, blank fill |
| 10-11 | Itinerary Variation Identifier | M | 01-99 |
| 12-13 | Leg Sequence Number | M | 01-99 |
| 14 | Service Type | M | Alpha (see Service Types) |
| 15-21 | Period of Operation From | M | DDMMMYY |
| 22-28 | Period of Operation To | M | DDMMMYY |
| 29-35 | Day(s) of Operation | M | 7-char frequency bitmap |
| 36 | Frequency Rate | C | Blank fill |
| 37-39 | Departure Station | M | 3-char IATA code |
| 40-43 | Passenger STD | M | HHMM |
| 44-47 | Aircraft STD | M | HHMM |
| 48-52 | UTC/Local Variation (Departure) | M | Hours+Minutes from UTC |
| 53-54 | Passenger Terminal (Departure) | C | Alphanumeric, left justify |
| 55-57 | Arrival Station | M | 3-char IATA code |
| 58-61 | Aircraft STA | M | HHMM |
| 62-65 | Passenger STA | M | HHMM |
| 66-70 | UTC/Local Variation (Arrival) | M | Hours+Minutes from UTC |
| 71-72 | Passenger Terminal (Arrival) | C | Alphanumeric, left justify |
| 73-75 | Aircraft Type | M | IATA 3-char code |
| 76-95 | PRBD (Passenger Reservations Booking Designator) | C | Either this or ACV (173-192) is mandatory |
| 96-100 | PRBM (Passenger Reservations Booking Modifier) | C | |
| 101-110 | Meal Service Note | O | |
| 111-119 | Joint Operation Airline Designators | C | Up to 3 carriers |
| 120-121 | MCT International/Domestic Status | O | Position 120=departure, 121=arrival (D or I) |
| 122 | Secure Flight Indicator | O | `S` if subject |
| 123-127 | (Spare) | M | Blank fill |
| 128 | Itinerary Variation Identifier Overflow | C | |
| 129-131 | Aircraft Owner | C | Left justify |
| 132-134 | Cockpit Crew Employer | C | |
| 135-137 | Cabin Crew Employer | C | |
| 138-140 | Onward Flight Airline Designator | O | |
| 141-144 | Onward Flight Number | M | Right justify |
| 145 | Aircraft Rotation Layover | C | |
| 146 | Operational Suffix | C | |
| 147 | (Spare) | C | |
| 148 | Flight Transit Layover | C | |
| 149 | Operating Airline Disclosure | C | |
| 150-160 | Traffic Restriction Code | M | See TRC table |
| 161 | Traffic Restriction Code Leg Overflow Indicator | C | |
| 162-172 | (Spare) | O | |
| 173-192 | Aircraft Configuration/Version | O | Either this or PRBD (76-95) is mandatory |
| 193-194 | Date Variation | O | Departure/Arrival day offset |
| 195-200 | Record Serial Number | M | Sequential |

### Date Variation codes (byte 193-194)
- `0` = same day
- `1` = next day
- `2` = two days later
- `A` = previous day

First digit = departure variation, second digit = arrival variation.

## Record Type 4 — Segment Data Record (Conditional/Optional)

DEI (Data Element Identifier) supplement records following a Type 3 leg record.

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `4` |
| 2 | Operational Suffix | C | |
| 3-5 | Airline Designator | M | |
| 6-9 | Flight Number | M | |
| 10-11 | Itinerary Variation Identifier | M | |
| 12-13 | Leg Sequence Number | M | |
| 14 | Service Type | M | |
| 15-27 | (Spare) | M | Blank fill |
| 28 | Itinerary Variation Identifier Overflow | C | |
| 29 | Board Point Indicator | M | Alpha |
| 30 | Off Point Indicator | M | Alpha |
| 31-33 | Data Element Identifier (DEI) | M | Right justify, zero fill |
| 34-36 | Board Point | M | 3-char IATA code |
| 37-39 | Off Point | M | 3-char IATA code |
| 40-194 | Data (DEI-specific) | C | Format per Chapter 2 |
| 195-200 | Record Serial Number | M | |

### Key DEI Numbers
- **DEI 2**: Operating Airline Disclosure (Code Share)
- **DEI 9**: Shared Airline/Wet Lease Designation
- **DEI 10**: Code Share: Commercial Duplicate
- **DEI 50**: Operating Carrier information (critical for codeshare MCT processing)
- **DEI 170-173**: Traffic Restriction Code qualifiers
- **DEI 220**: MCT International/Domestic Status override
- **DEI 503**: In-flight service codes

## Record Type 5 — Trailer Record (Mandatory)

| Bytes | Field | Status | Notes |
|-------|-------|--------|-------|
| 1 | Record Type | M | Always `5` |
| 2 | (Spare) | M | |
| 3-5 | Airline Designator | M | |
| 6-12 | Release (Sell) Date | O | |
| 13-187 | (Spare) | M | Blank fill |
| 188-193 | Serial Number Check Reference | M | Equals previous record's serial number |
| 194 | Continuation/End Code | M | `C` or `E` |
| 195-200 | Record Serial Number | M | |

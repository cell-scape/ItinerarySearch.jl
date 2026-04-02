# SSIM & MCT Glossary

Structured definitions extracted from:
- **SSIM Chapter 1 — Definitions** (IATA Standard Schedules Information Manual, 23rd Ed., March 2013)
- **SSIM Chapter 8 §8.8 — MCT Data Elements Glossary** (2021 edition)

Organized by relevance to the ItinerarySearch system and the `newssim.csv` column names.

---

## Flight Identity & Structure

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Administrating Carrier** | The airline that has the financial and commercial responsibility of a flight and that may or may not be the Operating Carrier. | `administrating_carrier`, `administrating_carrier_flight_number` | Distinct from both Marketing Carrier and Operating Carrier |
| **Operating Carrier** | The carrier that holds the Air Operator's Certificate for the aircraft used for that flight. | `aircraft_owner` | The actual operator of the aircraft |
| **Marketing Carrier** | The carrier that sells with its own code as part of a code-share agreement on a flight actually operated by another carrier. | `carrier` | The airline code shown on the ticket |
| **Flight** | The operation of one or more legs with the same Flight Designator. | `carrier` + `flight_number` | A flight may have multiple legs |
| **Leg** | The operation between a departure station and the next arrival station. | `leg_or_seg`, `leg_sequence_number` | The fundamental unit of schedule data |
| **Segment** | (Sometimes referred to as CITY PAIR) The operation between board point and any subsequent off point within the same flight. | `leg_or_seg`, `num_of_legs_in_seg` | A segment may span multiple legs |
| **Itinerary** | A single flight or a series of identical flights defined by a continuous Period and Days of Operation, each consisting of one or more contiguous legs which describe the complete routing of that flight. | — | Core concept for the search system |
| **Operational Leg** | A flight leg which is physically operated and identified by its Airline Designator and Flight Number. Any other Airline Designators and/or Flight Numbers associated with the same flight leg are considered to be non-operational flight legs. | — | Distinguishes physical operations from codeshare duplicates |
| **Duplicate Leg** | A single, non-operational leg of a flight that, for commercial/technical reasons, is displayed under more than one Flight Number by the operating carrier, or is displayed by a different Airline Designator/Flight Number by an airline other than the operating carrier. | — | Important for deduplication during graph building |

## Code Sharing & Partnerships

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Code Share** (Operating Airline Disclosure) | A flight where the operating airline allows seats/space to be sold by one or more other airlines under their own Flight Designator. More than one Flight Designator is used for a single operating flight. | `carrier`, `aircraft_owner` | When `carrier` != `aircraft_owner` |
| **Wet Lease** (Shared Airline or Wet Lease Designation) | A flight designated by a Flight Designator of one airline but operated by another airline on its behalf, as part of a commercial agreement (e.g., franchise/commuter style operations). Only the Airline Designator of the first (non-operating) airline is used in the Flight Designator(s) of the operating flight. | `wet_lease` field (derived) | Uses crew (cabin or cockpit) not employed by the administrating carrier |
| **Commercial Duplicate** | Refer to Operating Airline Disclosure — Code Share. | `DEI_127` | Listed in DEI 10 |

## Station & Geography

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Station** | A place to which a Location Identifier has been assigned. | `departure_station`, `arrival_station` | 3-letter IATA code |
| **Board Point** | Station of embarkation. | `departure_station` | Where passengers board |
| **Off Point** | Station of disembarkation. | `arrival_station` | Where passengers deplane |
| **Transit Station/Airport** | A scheduled en route stopping station on a flight. | — | Intermediate stops on multi-leg segments |
| **Departure Country / Arrival Country** | ISO 2-letter country code for the station. | `departure_country`, `arrival_country` | |
| **Departure State / Arrival State** | 2-letter state/province code. | `departure_state`, `arrival_state` | US states, Canadian provinces, etc. |
| **Departure Region / Arrival Region** | IATA region code (3-letter). | `departure_region`, `arrival_region` | See SSIM Appendix I for region codes |
| **Departure City / Arrival City** | Metropolitan area / city code. | `departure_city`, `arrival_city` | Multi-airport cities share a metro code |

## Time & Schedule

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Basic Schedule** | The planned regularly operated flights of an airline. | — | The undated frequency-based schedule |
| **Ad Hoc Schedule** | A variation, addition or cancellation from the basic schedule on single dates. | — | Single-date exceptions |
| **Passenger Departure/Arrival Time** | Scheduled time for passenger operations. | `passenger_departure_time`, `passenger_arrival_time` | May differ from aircraft times for ground handling |
| **Departure/Arrival DateTime** | Full local datetime of departure/arrival. | `departure_datetime`, `arrival_datetime` | ISO format in newssim CSV |
| **Departure/Arrival DateTime UTC** | Full UTC datetime of departure/arrival. | `departure_datetime_utc`, `arrival_datetime_utc` | ISO format in newssim CSV |
| **Block Time** | Total elapsed time from departure to arrival. | `blocktime` | In minutes |
| **Transit Time** | The time an aircraft remains in transit at the station in question. | — | Ground time at intermediate stops |

## Equipment & Configuration

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Aircraft** | A transport vehicle certified as airworthy. As used in SSIM, includes surface vehicles and traffic handling booked in similar manner to aircraft. | `aircraft_type` | 3-character IATA code (e.g., 78P, 739) |
| **Aircraft Configuration** | Planned utilisation layout of aircraft interior space. | `aircraft_configuration` | e.g., "J48O21Y188" |
| **Aircraft Body** | Wide body (W) or Narrow body (N) classification. | `body_type` | Used in MCT matching (priority 23/24) |
| **Fleet** | Grouping of aircraft by type/subfleet. | `fleet` | Airline-specific classification |

## Booking & Cabin

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Cabin** | A compartment where passenger seats are installed. | `cabin_pattern`, `cabin_count` | e.g., J/O/Y for First/Business/Economy |
| **Class** | Seating of passengers based on fare paid or facilities and services offered. | `class_count`, `prbd` | PRBD = Passenger Reservation Booking Designator |
| **Reservation** | (Equivalent to BOOKING) The allotment in advance of seating or sleeping accommodation for a passenger. | — | |

## MCT & Connection Building

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **MCT** (Minimum Connecting Time) | The shortest time interval required to transfer a passenger and luggage from one flight to a connecting flight, at a specific location. | — | Defined per station, carrier pair, and status |
| **Connection** (also TRANSFER) | The ability to transfer passengers, baggage, cargo or mail from one flight to another within a reasonable time period. Online connections concern transfers between flights of the same airline designator; interline connections between flights of different airline designators. | — | Core concept for graph edge building |
| **International/Domestic Status** | Identification of the international/domestic status on each flight leg to control the correct generation of flight connections. Values: DD (Domestic-Domestic), DI (Domestic-International), ID (International-Domestic), II (International-International). | `departure_international_domestic_status`, `arrival_international_domestic_status` | Determines which MCT applies; maps to `dep_intl_dom`/`arr_intl_dom` in structs |
| **Arrival/Departure Station** (MCT context) | The station where the MCT applies — the connecting point. | — | MCT filed per station pair |
| **Arrival/Departure Carrier** (MCT context) | The 2-letter Airline Designator of the delivering/receiving carrier at the specified station. | — | Used in MCT hierarchy matching |
| **Arrival/Departure Terminal** (MCT context) | The physical terminal a passenger arrives in / departs from. | `departure_terminal`, `arrival_terminal` | MCT hierarchy priority 11 (dep) and 12 (arr) |
| **Arrival/Departure Aircraft Type** (MCT context) | IATA 3-character aircraft code at the station. | `aircraft_type` | MCT hierarchy priority 21/22 |
| **Arrival/Departure Aircraft Body** (MCT context) | Wide (W) or Narrow (N) body classification. | `body_type` | MCT hierarchy priority 23/24 |
| **Codeshare Indicator** (MCT context) | "Y" when the MCT applies specifically to codeshare flights (determined by DEI 50 presence). A marketing (Y) flight MCT overrides an operating MCT. | — | Applied at both arrival and departure side |
| **Suppression Indicator** | Y = suppress connections, N/blank = allow. When Y, MCT time field is blank. | — | Can target by region, country, or state |
| **Next/Previous Station** (MCT context) | The next station of the departing flight / previous station of the arriving flight. Enables MCT to be applied based on routing. | — | MCT hierarchy priority 13/14 |
| **Next/Previous Country** (MCT context) | ISO country code of the next/previous station. | — | MCT hierarchy priority 17/18 |
| **Next/Previous State** (MCT context) | State code of the next/previous station. Next Country must also be provided. | — | MCT hierarchy priority 15/16 |
| **Next/Previous Region** (MCT context) | IATA region code. Cannot be used with Country, State, or Station. | — | MCT hierarchy priority 19/20 |
| **Time** (MCT context) | The minimum connecting time in HHMM format (e.g., 0130 = 1 hour 30 minutes). Blank when Suppression Indicator is Y. | — | The actual MCT value in minutes |

## Traffic Restrictions

| Term | Definition | Related CSV Column(s) | Notes |
|------|-----------|----------------------|-------|
| **Traffic Restriction** | Codes that restrict which traffic may be carried on a flight leg. | `traffic_restriction_for_leg` | See SSIM Appendix G for full code table |
| **Qualifier** | A data element whose value, extracted from a code list, gives specific meaning to the function of another data element or a segment. | — | TRC qualifiers 710-712 refine restriction codes |

## MCT Data Element Hierarchy (Priority Order)

The MCT matching system uses a priority hierarchy to select the most specific applicable MCT.
Higher priority (lower number) = more specific = takes precedence.

| Priority | Data Element | Description |
|----------|-------------|-------------|
| # | International/Domestic Status | Always considered; determines DD/DI/ID/II |
| 1 | Departure Codeshare Indicator | Codeshare-specific MCT for departing flight |
| 2 | Departure Carrier | Departing airline at the connection point |
| 3 | Departure Codeshare Operating Carrier | Operating carrier of departing codeshare |
| 4 | Arrival Codeshare Indicator | Codeshare-specific MCT for arriving flight |
| 5 | Arrival Carrier | Arriving airline at the connection point |
| 6 | Arrival Codeshare Operating Carrier | Operating carrier of arriving codeshare |
| 7 | Departure Flight Number Range Start | Specific departing flight or range |
| 8 | Departure Flight Number Range End | End of departing flight range |
| 9 | Arrival Flight Number Range Start | Specific arriving flight or range |
| 10 | Arrival Flight Number Range End | End of arriving flight range |
| 11 | Departure Terminal | Terminal of departing flight |
| 12 | Arrival Terminal | Terminal of arriving flight |
| 13 | Next Station | Next station on departing routing |
| 14 | Previous Station | Previous station on arriving routing |
| 15 | Next State | State of next station (requires Next Country) |
| 16 | Previous State | State of previous station (requires Previous Country) |
| 17 | Next Country | Country of next station |
| 18 | Previous Country | Country of previous station |
| 19 | Next Region | Region of next station (exclusive with Country/State/Station) |
| 20 | Previous Region | Region of previous station (exclusive with Country/State/Station) |
| 21 | Departure Aircraft Type | Aircraft type of departing flight |
| 22 | Arrival Aircraft Type | Aircraft type of arriving flight |
| 23 | Departure Aircraft Body | W/N body type of departing flight |
| 24 | Arrival Aircraft Body | W/N body type of arriving flight |

---

*Source: IATA SSIM 23rd Edition (March 2013) Chapter 1; SSIM Chapter 8 (March 2021) §8.6-8.8*

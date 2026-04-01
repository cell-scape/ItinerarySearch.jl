# SSIM Data Element Glossary

> Source: IATA Standard Schedules Information Manual (SSIM), 23rd Edition, March 2013, Chapter 2, Section 2.6
>
> This document contains glossary entries relevant to flight schedule data processing and itinerary building. Messaging-only elements (Action Codes, Message Serial Numbers, etc.) are omitted.

---

## Aircraft Configuration/Version (ACV)

**DEI:** ---

Identification of the physical cabin layout of an aircraft.

**Format:**

| Application | Format | Condition | Example |
|---|---|---|---|
| Chapters 3,4,5,7 | a(x)(x)(x)...... | Passenger codes | FY |
| | | Passenger codes and values | F10J10Y200 |
| | | Cargo codes | LLPP |
| | | Cargo codes and quantities | PP10, FYPP |
| | | Combinations | F32Y200K93PP20, F014Y119VVT3M33, PPVVT3M33 |

**Use:**
- Can only be used for legs.
- Intended as a physical description of the cabin layout; may not necessarily specify the classes used for publication/reservation purposes.
- Seat values are optional, but when provided the total seats must equal the seating capacity of the aircraft. May optionally include leading zeroes. Must not exceed four characters per value.

**Passenger Codes (Compartment/Class of Service):**

| Code | Compartment |
|---|---|
| P | First Class Premium |
| F | First Class |
| A | First Class Discounted |
| J | Business Class Premium |
| C | Business Class |
| D, I, Z | Business Class Discounted |
| W | Economy/Coach Premium |
| S, Y | Economy/Coach |
| B, H, K, L, M, N, Q, T, V, X | Economy/Coach Discounted |
| G | Conditional Reservation |
| U | Shuttle Service--No reservation needed--Seat guaranteed |
| E | Shuttle Service--No reservation allowed--Seat to be confirmed at check-in / Passenger Service--Reservations permitted |
| O, R | Use varies by Airline |

**Cargo Codes:**

| Code | Description |
|---|---|
| LL | Unit Load Devices (containers) |
| PP | Pallets |

**Chapter 7 Application:**
- If the ACV cannot be expressed within the available 20-character field (bytes 173-192), then "XX" will be stated in bytes 173/174 and 175 to 192 will be left blank, indicating reference to DEI 108 for the full specification.

---

## Aircraft Owner

**DEI:** 3

Information provided to whomever it may concern that the flight(s) will be operated with an aircraft not belonging to the fleet of the Administrating Carrier.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | xx(a) or X | BA or BAF or X |
| Chapter 7 | xx(a) or X%% | BA or X%% |

*DEI 3 is only applicable to Chapters 4 and 5.*

**Default:** When not stated, the aircraft belongs to the carrier stated in the airline designator. The aircraft owner field is left blank, no data supplied.

**Use:**
- When there is a legal requirement to disclose the Aircraft Owner, and the default does not apply, the use of this data element is mandatory.

**Chapters 4, 5 and 7 Applications:**
- The Data Element Identifier is always the digit "3" (not applicable in Chapter 7).
- Followed by the Airline Designator for the carrier to whose fleet the aircraft belongs.
- When the aircraft owner has no Airline Designator, the letter "X" indicates that its name in plain text will be found under DEI 113 (Aircraft Owner Specification).

---

## Aircraft Type

**DEI:** ---

The IATA standard 3-character code that normally covers the manufacturer and main model of a commercial aircraft.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,5,6,7 | xxx | D92 |

**Use:**
- For timetable publication purposes, the Aircraft Type can be overridden with the objective of consolidating otherwise equal itineraries (see Aircraft Type Publication Override DEI 121).

**Values:** Refer to SSIM Appendix A.

**Note:** When there is a plane change en-route without Aircraft Type change, this information must be provided using DEI 210 (Plane Change without Aircraft Type Change).

---

## Airline Designator

**DEI:** ---

The 2-character code assigned to a carrier by IATA and published in the IATA Airline Coding Directory or the 3-alphabetic codes assigned to a carrier by ICAO.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,5,6,7 | xx(a) | ABC |

**Use:**
- Carriers not assigned IATA 2-character codes may use the ICAO 3-letter codes.
- For publication and reservations purposes, 3-letter codes must currently not be used as some computer systems would be unable to read them.
- The data element format provides for 3-character designators. The present official format is effectively 'xx' but in practice is 'xa' or 'ax' to avoid confusion with the Flight Number.

**Values:** Refer to the IATA Airline Coding Directory.

---

## Board Point Indicator

**DEI:** ---

A single alpha character to indicate the departure station of a segment (Board Point) to which a data element associated with a Data Element Identifier applies.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | a | A |

**Values:**
- The departure station (board point) on the first leg of a flight is indicated by "A", the departure station on the second leg is indicated by "B", and so on.

---

## Date Variation

**DEI:** ---

The relationship between Day(s)/Period of Operation of the flight origin station and the Scheduled Time of Aircraft Departure/Arrival in the same time mode.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 4 | (M)n | 2 |
| Chapter 7 | Nn | 01 |

**Chapter 4 Application Values:**

| Code | Description |
|---|---|
| 0 | Arrival/departure on the same day (optional) |
| 1 | Arrival/departure on the next day |
| 2 | Arrival/departure two days later |
| 3 | Arrival/Departure three days later |
| 4 | Arrival/Departure four days later |
| 5 | Arrival/Departure five days later |
| 6 | Arrival/departure six days later |
| 7 | Arrival/departure seven days later |
| M1 | Arrival/departure on the previous day |

**Chapter 7 Application Values:**

| Code | Description |
|---|---|
| 0 | Arrival/departure on the same day |
| 1 | Arrival/departure on the next day |
| 2 | Arrival/departure two days later |
| 3 | Arrival/Departure three days later |
| 4 | Arrival/Departure four days later |
| 5 | Arrival/Departure five days later |
| 6 | Arrival/departure six days later |
| 7 | Arrival/departure seven days later |
| A | Arrival/departure is previous day |

**Key Notes:**
- The first indicator in the Chapter 7 format applies to the Departure Variation and the second indicator applies to the Arrival Variation.

---

## Day(s) of Operation / Frequency Rate

### Day(s) of Operation

**DEI:** ---

The day(s) of the week when a flight is operated.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 3 | nnnnnnn (n may be substituted by full stop/period) | 1.3.5.7 |
| Chapter 4 | n(n)(n)(n)(n)(n) | 1357 |
| Chapter 6 | nnnnnnn | 1030507 |
| Chapter 7 | (n)(n)(n)(n)(n)(n)(n) | 10305b7 |

**Use:**
- Days of Operation are stated as numbers 1 through 7, where Monday is Day 1.
- Ascending order is mandatory.
- Days of Operation should be compatible with Period of Operation.
- The day(s) always relate to the Scheduled Time of Aircraft Departure (STD) -- not the Passenger STD.

**Non-operative days fill:**

| Application | Fill |
|---|---|
| Chapter 3 | Insert full stops/periods |
| Chapter 4 | no fill |
| Chapter 6 | zero (0) fill |
| Chapter 7 | blank fill |

**Key Notes (Chapters 4 and 7):**
- Downline legs of a flight having an STD on the next (or previous) day(s) shall have the Day(s) of Operation adjusted correspondingly in relation to the Day(s) of Operation on the first leg.

### Frequency Rate

**DEI:** ---

An indication that a flight operates at fortnightly intervals (every 2 weeks) on the day(s) of the week stated under Day(s) of Operation.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 4 | /an | /W2 |
| Chapters 6,7 | 2 | 2 |

**Default:** When not stated, the flight operates at weekly intervals on the day(s) of the week stated under Day(s) of Operation.

**Use:**
- When Frequency Rate is used, the start date of the Period of Operation must be the first date on which the flight operates, and the end date must be the last date on which the flight operates.
- The start and end dates may **not** be expressed as "00XXX00" or "00XXX".

---

## Flight Number

**DEI:** ---

A multi-purpose reference assigned by a carrier in connection with the planning and control of the operation of flights.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 3 | n(n)(n)(n) | 83 |
| Chapters 4,5,6 | nnn(n) | 123 |
| Chapter 7 | (n)(n)(n)n | %%% 2 |

**Use:**
- The Flight Number shall identify a flight or series of similar flights.
- The Flight Number shall be assigned such that it applies to only one scheduled departure from origin station per day (UTC and local).
- May consist of up to 4 numeric digits. In Chapters 4, 5, and 6, a minimum of 3 digits, zero filled as necessary, is mandatory.
- The Flight Number must never appear on its own but must always form part of the Flight Designator.

**Key Notes:**
- This field is fixed formatted, right justified and zero and/or blank filled in respect of Chapter 7 Schedule Data Set formats.
- Leading zeros do not create a different Flight Number (e.g., 123 and 0123 are the same).

---

## Itinerary Variation Identifier (IVI)

**DEI:** ---

A number used to differentiate between itineraries having the same Flight Designator (without regard to Operational Suffixes, if any).

An **Itinerary** is a single flight or a series of identical flights defined by a continuous Period and Day(s) of Operation (and Frequency Rate if applicable), each of which consists of one or more contiguous legs which, taken together, describe a complete routing of that flight.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | nn | 02 |

**Format:** A number between 01 and 99.

**Use:**
- IVIs shall be assigned such that the itinerary with the earliest effective date shall be assigned IVI "01", the next earliest "02", etc.
- Where two or more itineraries have equal effective dates, the one with the earliest discontinue date is assigned the smallest IVI.
- When more than 99 IVIs are required for the same Flight Designator, use the Itinerary Variation Identifier Overflow data element.

---

## Leg Sequence Number

**DEI:** ---

The sequence number of the leg for the flight and itinerary variation being specified within each Itinerary Variation Identifier.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | nn | 03 |

**Format:** 2 numeric bytes; recommended maximum of 20 legs.

---

## Minimum Connecting Time International/Domestic Status

**DEI:** ---

Identification of the international/domestic status on each flight leg to control the correct generation of flight connections between two flights.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | aa | DD |

**Default:**
- Country codes of origin and destination stations on the flight leg are compared. When the countries are the same, the leg status is "DD" (domestic). When different, the leg status is "II" (international).
- A leg status of "DI" or "ID" is possible when an exception applies.

**Use:**
- Only used in Chapter 7.
- The first character specifies the departure status of either "D" (domestic) or "I" (international), and the second character specifies the arrival status ("D" or "I") of the specified leg.
- Functional use requires the arrival status of one flight leg and the departure status of the connecting flight leg to be combined. This combined status ("DD", "II", "DI", or "ID") identifies the connection status for MCT application.
- It is very important to correctly identify the connection status in order to find the accurate Minimum Connect Time data.

---

## MCT International/Domestic Status Override (DEI 220)

**DEI:** 220

Information required to control the correct generation of flight connections.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5,7 | a/a | D/I |

**Chapters 4, 5 and 7 Applications:**
- The following codes are used: **D** (Domestic), **I** (International).
- The first indicator applies to the Board Point and the second (preceded by a slash) to the Off Point. Both indicators must be provided to avoid ambiguity.

**Use:**
- Used when the status (Domestic or International) of the flight leg or segment cannot be interpreted unambiguously for Minimum Connecting Time (MCT) application.
- May also be applied to override the status normally derived from analyzing the routing.
- The default interpretation is: same country = domestic, different countries = international.

**Example:** Flight XY123 operates SYD-HNL-LAX. Use `SYDLAX 220/I/D` to uniquely define the MCT Status for SYD-LAX passengers arriving at LAX as Domestic.

---

## Off Point Indicator

**DEI:** ---

A single alpha character to indicate the arrival station of a segment (Off Point) to which a data element associated with a Data Element Identifier applies.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | a | C |

**Values:**
- The arrival station (off point) on the first leg of a flight is indicated by "B"; the arrival station on the second leg is indicated by "C", and so on.

---

## Operating Airline Disclosure (DEI 127)

**DEI:** 127

To state the operator of the flight in a code share, shared airline designation or wet lease situation.

**Format:**

| Application | Condition | Format | Example |
|---|---|---|---|
| Chapters 4,5 | Airline Designator | xx(a) | BA or AAL |
| | Airline Designator and Name | xx(a)/x(x)... | BA/BRITISH AIRWAYS or CPB/CORPORATE EXPRESS AIRLINES |
| | Name--text only | /x(x)... | /LOGANAIR or /BRIT AIR DBA AIR FRANCE |
| Chapter 7 | Airline Designator | xx(a) | BA or AAL |
| | Airline Designator and Name | xx(a)/x(x)... | BA%/BRITISH AIRWAYS |
| | Name--text only | /x(x)... | /LOGANAIR |

**Use:**
- Information that states the actual operator of the flight, when the operator is different from both the Administrating Carrier and the Aircraft Owner.
- The use of this data element is mandatory when there is a legal requirement to disclose the operator of a service.
- If the operator has its own Airline Designator, the code is submitted in the first two or three bytes.
- If the operator has no airline designator (or chooses not to use it), the full company name or other text is supplied as free text.

**Chapter 7 Application:**
- DEI 127 is used when either 'X' or 'Z' has been specified in byte 149:
  - **'X'**: Operating Airline Disclosure--Shared Airline or Wet Lease Designation
  - **'Z'**: Operating Airline Disclosure--Code Share

---

## Operating Airline Disclosure--Code Share (DEI 2)

**DEI:** 2

To state the carrier actually operating a flight, or flight leg(s) in a commercial duplicate code share operation.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | xx(a) or X | AB or 3B or 6X or AGL or X |
| Chapter 7 | a | L |

*DEI 2 is only applicable to Chapters 4 and 5.*

**Use:**
- Information supplied on a flight/flight leg providing details of the Carrier who is operating a flight/flight leg on behalf of the carrier in the flight designator.
- The use of this Data Element is mandatory when there is a legal requirement to disclose the Actual Operator.
- Code Share details consist of Data Element Identifier 2 followed by either the Airline Designator specifying the operator, or the letter "X" (indicating no Airline Designator; full details via DEI 127).

**Chapter 7 Application:**
- Code Share details are supplied in record type 3 by supplying a letter 'L' or 'Z' in byte 149:
  - **'L'**: the operator is the Airline Designator specified in the Aircraft Owner field byte 129-131.
  - **'Z'**: the carrier has no Airline Designator; full details are specified via DEI 127.

---

## Operating Airline Disclosure--Shared Airline or Wet Lease Designation (DEI 9)

**DEI:** 9

To state the carrier actually operating a flight, or flight legs on behalf of the Carrier specified by the Airline Designator in the Flight Designator.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | xx(a) or X | AB or 3B or 6X or AGL or 9/X |
| Chapter 7 | a | S |

*DEI 9 is only applicable to Chapters 4 and 5.*

**Use:**
- Information supplied on a flight/flight leg providing details of the carrier who is operating the flight/flight leg on behalf of the carrier in the flight designator.
- The use of this data element is mandatory when there is a legal requirement to disclose the Actual Operator, and this is different from both the Administrating Carrier and the Aircraft Owner.

**Chapter 7 Application:**
- Code Share details are supplied in record type 3 by supplying a letter 'S' or 'X' in byte 149:
  - **'S'**: the operator is the Airline Designator specified in the Aircraft Owner field byte 129-131.
  - **'X'**: the carrier has no Airline Designator; full details are specified via DEI 127.

---

## Operational Suffix

**DEI:** ---

A code assigned by the administrating carrier for operational purposes.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5,6,7 | a | B |

**Use:**
- An optional one alphabetic character that immediately follows the Flight Number.
- The use and meaning of the suffix will be defined by the Administrating Carrier.
- When supplying Operational Suffix details for multi-leg flights, the suffix will apply to all legs of the itinerary.
- It is recommended that Suffix Z be reserved for use in connection with UTC day/date Flight Designator duplications.
- The Operational Suffix must not be considered as part of the Flight Number for publication and reservations purposes.

**Chapter 7 Application:** The Operational Suffix is specified byte 2 of Record Types 3 and 4.

---

## Passenger Reservations Booking Designator (PRBD)

**DEI:** ---

A leg oriented data element specifying the codes to describe the reservations classes provided, and optionally the number of seats allocated for each class or group of classes.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,5 | a(x)(x)(x) .... | PFCYBV |
| Chapter 7 | a(x)(x)(x) .... (20 char.) | F008C038BQV145%%%%% |

**Use:**
- Used for publication, reservations and other public information purposes. May differ from the physical aircraft layout defined in the Aircraft Configuration/Version.
- A string of characters consisting of single alphabetic codes from the ACV table and/or AIRIMP Section 7.1.1.
- Optionally, all codes may be followed by a numeric value to indicate the number of seats for each code. Each numeric specification must not exceed three digits.
- The codes can be stated in any sequence. Receiving systems unable to process all codes will normally process them in the order presented.
- When it is not possible to express the PRBD within the available field, "XX" will be stated in the first two positions, indicating reference to DEI 106 for the full specification.

**Key Notes:**
- While specification of number of seats is optional, when a value is quoted the total seats must equal the saleable seating capacity of the aircraft.
- For segments where the classes are not identical on each of the legs making up the segment, use DEI 101 (Segment Override).

---

## Passenger Reservations Booking Modifier (PRBM)

**DEI:** ---

A modifying code applicable to the appropriate Passenger Reservations Booking Designator Code.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | aa(aa)(aa)..... | FNYN |
| Chapter 7 | (a)(a)(a)(a)(a) | %N%%% |

**Use:**
- The relevant Passenger Reservations Booking Designator Code is stated before the modifier.
- When it is not possible to express the PRBM within the available line length, "XX" will be stated in the first two positions, referencing DEI 107 for the full specification.
- The modifier must be a single, non-blank, alphabetic character that is different from the Passenger Reservations Booking Designator Code which it modifies.
- Non-applicable and non-existent classes are to be blank-filled.

**Key Notes:**
- Modifiers shall apply to multi-leg segments of a flight only when the PRBD and the PRBM are equal on each of the legs making up the segment.
- When classes and/or modifiers are different over a multi-leg segment, the override facility (DEI 101/102) must be used.

---

## Passenger Terminal / Terminal Identifiers

### Passenger Terminal

**DEI:** ---

The physical terminal used by a passenger at any airport where more than one terminal exists.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,7 | x(x) | 2A |

**Use:**
- If the terminal at an airport included in SSIM Appendix D is not pre-determined, the Passenger Terminal shall be stated as "0" (zero).
- If the terminal varies by segment, report the terminal that pertains to the departure/arrival leg in the appropriate Passenger Terminal field.
- Any terminal information that differs by segment shall be supplied using DEI 198 (Arrival) or DEI 199 (Departure).

**Format (Chapters 3 and 7):** A two byte field.

**Values:** Refer to SSIM Appendix D.

### Passenger Terminal Identifier--Arrival (DEI 98)

**DEI:** 98

The passenger arrival terminal.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | x(x) | 2W |
| Chapter 6 | TA.x(x) | TA.M |

*DEI 98 is only applicable to Chapters 4 and 5.*

**Use:**
- The Passenger Terminal Identifier always refers to the Off Point of the stated leg.

### Passenger Terminal Identifier--Departure (DEI 99)

**DEI:** 99

The passenger departure terminal.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | x(x) | 2W |
| Chapter 6 | TD.x(x) | TD.D |

*DEI 99 is only applicable to Chapters 4 and 5.*

**Use:**
- The Passenger Terminal Identifier always refers to the Board Point of the stated leg.

### Passenger Terminal Segment Override--Arrival (DEI 198)

**DEI:** 198

The Passenger Terminal for deplaning passengers that may not apply leg by leg but over a segment.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5,7 | x(x) | I |

**Use:** Always refers to the Off Point of the stated segment.

### Passenger Terminal Segment Override--Departure (DEI 199)

**DEI:** 199

The Passenger Terminal for enplaning passengers that may not apply leg by leg but over a segment.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5,7 | x(x) | I |

**Use:** Always refers to the Board Point of the stated Segment.

---

## Period of Operation

**DEI:** ---

The date limits for the first and last operation of a flight.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4 | nnaaa(nn)->nnaaa(nn) | 01JUN 00XXX |
| Chapter 6 | nnaaannaaa | 27APR27SEP |
| Chapter 7 | nnaaannnnaaann | 10APR0112MAY01 |

**Use:**
- Dates always relate to the Scheduled Time of Aircraft Departure (STD) -- not the Passenger STD.
- In Chapter 7, downline legs departing on the next (or previous) day(s) shall have the Period of Operation adjusted correspondingly.

**Key Notes:**
- The date shall be expressed as the first two numerics for the date and first three alphabetic characters (in English spelling) for the month. Year is optionally the two last numerics.
- The year may be omitted in Chapters 3 and 4 only if the first and last operations are within 11 months from the current date, or are indefinite.
- Either date can be stated as "00XXX00" (last two characters being optional). This indicates indefinite validity.
- When the first date is so specified, data is effective immediately (Chapter 7: on the first date in the Period of Schedule Validity applied to the first leg of the itinerary).
- When the second date is so specified, it is effective indefinitely (Chapter 7: until the last date in the Period of Schedule Validity applied to the first leg of the itinerary).

---

## Record Serial Number

**DEI:** ---

The number of the record in computerized schedule formats.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | nnnnnn | 001049 |

**Format:** A 6 byte numeric field occurring in all records on each physical data set, irrespective of type, numbered sequentially beginning with "000001".

**Use:**
- Enables a check to be made for possible errors and enables records to be unambiguously identified.
- When the number of records exceeds "999999", re-numbering starts at "000002" since "000001" is reserved for Record Type 1.

---

## Record Type

**DEI:** ---

The type of records in the computerized schedules formats for Chapter 7.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | n | 1 |

**Values:**

| Code | Description |
|---|---|
| 1 | Header Record |
| 2 | Carrier Record |
| 3 | Flight Leg Record |
| 4 | Segment Data Record |
| 5 | Trailer Record |

---

## Scheduled Time of Aircraft Arrival (Aircraft STA)

**DEI:** ---

The scheduled arrival time of an aircraft at the terminal or arrival gate/position at an airport.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,6,7 | nnnn | 2400 |
| Chapter 5 | (nn)nnnn | 301900 |

**Use:**
- STA shall always be expressed by four digits indicating the 24 hours clock timing and be in the range of 0001 through 2400.
- Arrivals at midnight (end of the day) are always stated as 2400.
- STA always refers to the on-block time of the aircraft.
- STA can be expressed in local time in Chapters 3, 4, 5 and 7.
- The 24 hour clock format is hhmm. 'hh' does not exceed 24 and 'mm' does not exceed 59. The only valid value in the hour 24 is minutes 00.

---

## Scheduled Time of Aircraft Departure (Aircraft STD)

**DEI:** ---

The scheduled departure time of an aircraft from the terminal or departure gate/position at an airport.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,6,7 | nnnn | 0000 |
| Chapter 5 | (nn)nnnn | 010145 |

**Use:**
- STD shall always be expressed by four digits indicating the 24 hours clock timing and be in the range of 0000 through 2359.
- Departures at midnight (beginning of the new day) are always stated as 0000.
- STD always refers to the off-block time of the aircraft.
- STD can be expressed in local time in Chapters 3, 4, 5 and 7.
- The 24 hour clock format is hhmm. 'hh' does not exceed 23 and 'mm' does not exceed 59.

---

## Scheduled Time of Passenger Arrival (Passenger STA)

**DEI:** ---

The Scheduled Time of Arrival of the passenger at the terminal or arrival gate at an airport.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5,7 | nnnn | 1540 |

**Default:** If not stated, the Passenger STA will be the same as the Aircraft STA. Note that there is no default for Chapter 7, since the Passenger STA is a mandatory field on Record Type 3.

**Use:**
- It is only different from the Aircraft STA when a transfer is effected between aircraft and terminal/gate by another transport mode (e.g. mobile lounge) for which a different arrival time is scheduled.
- Range: 0001 through 2400. Arrivals at midnight are always stated as 2400.
- hhmm format: 'hh' does not exceed 24 and 'mm' does not exceed 59. The only valid value in the hour 24 is minutes 00.

---

## Scheduled Time of Passenger Departure (Passenger STD)

**DEI:** ---

The Scheduled Time of Departure of the passenger at the terminal or departure gate at an airport.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5,7 | nnnn | 1255 |

**Default:** If not stated, the Passenger STD will be the same as the Aircraft STD. Note that there is no default for Chapter 7, since the Passenger STD is a mandatory field on Record Type 3.

**Use:**
- It is only different from the Aircraft STD when a transfer is effected between terminal/gate and aircraft by another transport mode for which a different departure time is scheduled.
- Range: 0000 through 2359. Departures at midnight are always stated as 0000.
- hhmm format: 'hh' does not exceed 23 and 'mm' does not exceed 59.

---

## Service Type

**DEI:** ---

Classification of or flight or flight leg as well as the type of service provided.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,5,6,7 | a | J |

**Use:**
- The Service Type is a leg oriented data element.
- For multi-leg flights where the Service Type differs by leg, no assumption can be made about multi-leg segments.
- The Service Type is **not** a substitute for the Aircraft Configuration/Version.

**Values:** Refer to SSIM Appendix C. Common values include:

| Code | Description |
|---|---|
| J | Normal/Scheduled Passenger Service |
| S | Scheduled Passenger/Cargo in Cabin Service |
| G | Passenger Charter |
| A | Passenger Service (Shuttle) |
| B | Passenger Service (Supplement) |
| U | Shuttle Service |
| C | Passenger/Cargo (Combi) |
| F | Freight/Cargo |
| H | Cargo Charter |
| V | Surface Service (Bus/Train) |
| P | Non-revenue (positioning/ferry/delivery) |
| Q | Passenger/Cargo in Cabin (Combi) |
| R | Additional Flights -- Passenger/Cargo |

**Note:** For segment AAA-CCC carrying Charter traffic only (not to be sold in reservations systems), Traffic Restriction 'A' should be used for that segment.

---

## Station

**DEI:** ---

Identification of an airport for airline purposes.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 3,4,5,6,7 | aaa | JFK |

**Values:**
- The 3-letter Location Identifiers for airports are assigned by IATA in accordance with IATA Resolution 763 and published in the IATA Airline Coding Directory.

**Fictitious Points:**
- The following Location Identifiers have been reserved as "fictitious points" for schedule construction:
  - **QZX** -- Fictitious Country ZZ 1, UTC Variation: UTC
  - **QPX** -- Fictitious Country ZZ 2, UTC Variation: UTC + 7
  - **QMX** -- Fictitious Country ZZ 3, UTC Variation: UTC - 7
  - **QPY** -- Fictitious Country ZZ 4, UTC Variation: UTC + 14
  - **QMY** -- Fictitious Country ZZ 5, UTC Variation: UTC - 14

Used to: (a) overcome day duplication problems; (b) describe legs of elapsed times covering more than 23:59 hours.

---

## Traffic Restriction Code

**DEI:** ---

Information provided by a carrier to specify restrictions to carry traffic or specify limitations on the carriage of traffic.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | (a)(a)(a)(a)(a)(a)(a)(a)(a)(a)(a) | %%A%Z%%%%%%%% |

**Default:** In the absence of any information to the contrary, it is assumed that any Traffic Restriction stated applies to all forms of traffic (passenger, cargo, mail) at Board and/or Off Point.

**General Traffic Restriction Information:**
- A Traffic Restriction Code allows a carrier to specify:
  - (a) any restriction on the carriers right to carry traffic, and
  - (b) any limitations on the actual carriage of traffic on a segment

**Use of Traffic Restriction Overflow Indicator 'Z' (Chapter 7 only):**
- 'Z' is used instead of a valid Traffic Restriction Code when:
  - (a) A different Traffic Restriction applies to passenger, cargo or mail
  - (b) A Traffic Restriction applies to one or two categories of service only but not to all three
  - (c) A Traffic Restriction is required on the 12th leg of a flight (leg sequence number >11)
- When 'Z' is placed, the actual Traffic Restriction code details must be supplied with the appropriate DEI 170-173 in the Segment Data Record (type 4 record).

**Additional Traffic Restriction Code Information (Chapter 7 only):**
- DEI 710 -- Traffic Restriction Code Qualifier at Board Point
- DEI 711 -- Traffic Restriction Code Qualifier at Off Point
- DEI 712 -- Traffic Restriction Code Qualifier at Board and Off Points

**Chapter 7 Application:**
- The Traffic Restriction code is input in the 11 byte field in the SSIM Flight Leg Record (record type 3) starting at byte 150 through and including byte 160.
- Each byte from 150 to 160 relates sequentially to the **Off Points** in the routing, and these bytes can therefore accommodate a flight with 11 non-stop legs.
- When the Traffic Restriction applies to all categories of traffic, the code is placed in the byte that matches the off point on that leg.
- When the Traffic Restriction is not applicable to all categories of service, 'Z' is placed and the actual code is supplied with DEI 170-173.

**Values:** Refer to Appendix G for the Traffic Restriction Codes Table. Common codes relevant to passenger itinerary building include:

| Code | Description |
|---|---|
| A | No local traffic -- Passenger must have a connecting flight at Board AND Off Point |
| B | Local and behind traffic only -- No online connections at Off Point |
| C | Local traffic only |
| D | Qualify the Code Share/Operating Airline Disclosure DEI 2 or 9 |
| E | Qualify the Code Share DEI 2 |
| G | Board Point Restriction -- Applies at Board Point only |
| H | Off Point Restriction -- Applies at Off Point only |
| I | No international stopover at Board or Off Point |
| K | Connection required -- at Board and/or Off Point (see DEI 710/711/712) |
| L | Board Point local and connection |
| M | Restriction applies to Online selling and Code Share |
| N | Restriction applies to Online selling, Code Share and Interline |
| O | Applicable to Online selling only |
| P | Reserved for 1st and/or Business class |
| Q | No online or interline connection at Off Point |
| T | Online connection only |
| V | Connecting traffic only -- At Board and Off Point |
| W | No local traffic -- International/Domestic |
| X | Code Share traffic only |
| Y | Restriction for Cargo/Mail only |

**Traffic Restriction Code Qualifiers:**

| DEI | Name | Effect |
|---|---|---|
| 710 | Qualifier at Board Point | Restriction requirements must be met at the Board Point; no restrictions at Off Point |
| 711 | Qualifier at Off Point | Restriction requirements must be met at the Off Point; no restrictions at Board Point |
| 712 | Qualifier at Board and Off Points | Restriction requirements must be met at both Board and Off Points |

---

## UTC/Local Time Variation

**DEI:** ---

Indication of the difference in hours and minutes between UTC and local time.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapter 7 | +/-nnnn | +0100 |

**Format:**
- UTC is to be expressed as +0000 (Chapter 7).
- A plus or minus sign, followed by four numerics where the two first express the 'hour' and the two last express the 'minutes'.

**Use:**
- The difference will be negative if UTC is later than the local time.
- The sign difference is always applied to UTC in order to obtain local time.

**Chapters 4 and 5 Applications:**
- Specification is achieved by using DEI 97 (UTC/Local Time Variation Specification).

**Values:** Refer to SSIM Appendix F.

### UTC/Local Time Variation Specification (DEI 97)

**DEI:** 97

Identification of a UTC/Local Time Variation where the originator of an SSM/ASM wants to override a UTC/Local Time Variation held in the recipient's systems.

**Format:**

| Application | Format | Example |
|---|---|---|
| Chapters 4,5 | aaa/xnnnn | ABC/P0200 |

**Format:**
- The 'x' represents either "M" (minus) or "P" (plus).
- UTC is to be represented as P0000.

**Chapters 4 and 5 Applications:**
- The UTC/Local Time Variation Specification always refers to the Station stated within its format. If this Station equals the Board Point of the stated Segment, it refers to the departure time from that Board Point; if it equals the Off Point, it refers to the arrival time at that Off Point.
- This data element need not be stated if the UTC/local time variation is in agreement with SSIM Appendix F.

# Profit Manager Constraint Tables

Example data from a profit manager workset for building global itinerary superset.

# Overview

Constraint rules and fine-grained overrides for building itineraries. Typically delimited files, CSV or TSV. Tend to have a similar structure, commonly market level overrides for certain rules, or more configurable rules where fields can be set at certain levels like Region, Entity, Country, Airport, or for carriers, Airline or Alliance.

# General Terminology

- Online Connection: A connection between two legs with the same marketing carrier, but the same or different operating carriers. Can be a host connection or a codeshare connection.
- Host Connection: A connection where the marketing and operating carriers are the same on both legs. E.g. A UA flight operated by UA -> UA/UA 
- Codeshare Connection: A connection between two legs with the same marketing carrier but different operating carriers. 
- Interline Connection: A connection between two flight legs with different marketing carriers, and the same or different operating carriers. Can be two host flights from two different airlines, or two codeshare flights from different airlines, or a combination.
- Alliance Connection: An interline connection where both marketing airlines are in an alliance. E.g. UA -> AC, UA and AC are both in the alliance STAR-ALLIANCE.
- Single Connection: One stop connection
- Double Connection: Two stop connection
- Entity: A designator for a collection of geographic locations. Could be multiple regions, countries, states, metro areas, or other entities, but fundamentally will be resolved as a set of airports. E.g. MEX,AFR => NA-AFR. Mexico and Africa, both part of the Entity "North America-Africa", or all the airports in both NA and AFR. 
 

## Region Level Constraint Exceptions 

Example at data/demo/RgnLvlCnstExc.dat. CSV format file, empty cell means wildcard.
Prohibits certain connections from being built based on their origin, destination, and connection point.

### Header
    1. ORIGIN_LEVEL: Determines type of geography to use for origin. E.g. C => Country, A => Airport
    2. ORIGIN: Origin value. E.g. CA => Canada, ORD => O'Hare
    3. DESTINATION_LEVEL: Type of geography for destination.
    4. DESTINATION: Destination value
    5. AIRLINE1: Leg 1 (arrival) airline
    6. AIRLINE2: Leg 2 (departure) airline
    7. CONNECTIONPOINT_LEVEL: Type of geography for connection point
    8. CONNECTIONPOINT: Connectionpoint value
    9. ROUND_TRIP: Include round trips. Boolean, Y => True, N => False
    10. TYPE: Exception type. E.g. P => Prohibit

### Example Records

C,CA,C,CA,,,C,US,Y,P

- This record means that any flight involving any carrier whose origin and destination stations are both in Canada may not build a connection at a station in the US.

C,MX,C,MX,,,C,US,Y,P

- This record means that any flight involving any carrier whose origin and destination stations are both in Mexico may not build a connection at a station in the US.

A,ORD,C,GB,UA,AA,R,ASI,Y,P

- This hypothetical record means that a UA flight from ORD -> Any station in Great Britain cannot connect to an AA flight from Any station in Great Britain -> Any station in Asia.

## Connection Flags

Example data at data/demo/cnctFlags.dat. Delimited file. Determines what types of conections are allowed to be built at the Market or Entity level. ORG,DEST,ENTNM determine station, remaining fields are all boolean fields allowing or prohibiting different types of connections at the market level. 

### Header
1. ORG: Origin station
2. DEST: Destination station
3. ENTNM: Entity Name (set of stations)
4. SNGL_ONLN: Single online connections (1 == Permitted, 0 == Prohibited)
5. DUBL_ONLN: Double online connections
6. DUBL_INTER: Double interline connections
7. SNGL_ALLNCE: Single alliance interline connections
8. DUBL_ALLNCE_INTER: Double alliance interline connections

### Example Records 

CLE,ACC,*,1,0,1,1,0,0

- For the market CLE->ACC, allow any single online, double online, or double interline connection, but prohibit single interline, single alliance, and double alliance connections. 

DUS,CVG,*,1,1,1,1,1,1

- For the market DUS -> CVG, permit any type of single or double connection

ORD,DEN,*,0,0,0,0,0,0

- For the market ORD -> DEN, prohibit any kind of single or double connection.


## Circuity Override

Table of default circuities based on distance (cirOvrdDflt.dat), or market level circuity overrides (cirOvrd.dat). Default table is a map of the maximum distance to the default circuity, e.g 250,2.4 means connections within 0-250 miles have a maximum permissible circuity ratio of 2.4.

More fine grained circuity overrides take precedence over more generic circuity rules. If there is a market level circuity ratio, it overrides that of the global default circuity, or the tiered distance defaults.

Generally, circuity around hubs is higher, and circuities over greater distances are lower.

### Header
1. ORG: Origin station
2. DEST: Destination station
3. ENTNM: Entity name
4. CRTY: Maximum Circuity Ratio

### Example Records

ATL,YYZ,*,2.7 

Any connection between ATL and YYZ has a maximum permissible circuity raio of 2.7, which overrides the default circuities based on distance, or global default of 2.0.


## Alliances

data/demo/alliance.dat. A CSV file, a mapping of airlines to the alliances to which they belong. An airline may belong to more than one alliance, and airlines that are connnecting with alliance connections may both belong to multiple airlines. If the connecting airlines are both in any alliance together, they may connect for alliance single or double connections. 

Alliance connections are considered interline connections, and may or may not involve codesharing on either leg.

### Header
1. ALLNCENM: Alliance Name
2. ALNCD: Airline Code
3. ADJUSTPOO: Y or N, references another constraint table.

### Example Records

VALUE-ALLIANCE,5J,N

The airline 5J belongs to the alliance VALUE-ALLIANCE.

UA-UK,UA,N
UA-UK,UK,N

The airlines UA and UK belong to the alliance UA-UK. An alliance with exactly two members is an exclusive joint venture and should be prioritized over a larger alliance.

STAR-ALLIANCE,UA,N
STAR-ALLIANCE,LH,N

The airlines UA and LH belong to the alliance STAR-ALLIANCE.

## Airline Country

Table mapping Airline codes to their home country.
If there are records mapping an airline code to multiple different countries, the most recent timestamp should be taken.

## Header
1. ALNCD: Airline code
2. CTRY: Home country code
3. USRID: A userid (ignore)
4. TMST: Timestamp

### Example Record
UA,US,u000000,2000-01-01 00:00:00.0

UA is headquartered in the US. DD/DI/ID/II relationship to UA host flights is determined by home country.

## Market Flags

Rules governing connection building at market level.
ORG,DEST,ENTNM determines market, the other fields are boolean, 1 == permitted, 0 == prohibited.

### Header
1. ORG: Origin station
2. DEST: Destination station
3. ENTNM: Entity name
4. ONLN: Online connections
5. PUREONLN_ONLN: Pure online-online connections (host flights)
6. PUREONLN_INTER: Pure Online -> Interline Connections
7. INTERLINE: Interline connections
8. PUREINTER_ONLN: PUre interline -> Online connections
9. PUREINTER_INTER: PUre interline -> Interline connections

### Example Records

*,*,*,1,1,1,0,0,0

Global default (any station or entity). Online, pure online online, pure online interline connections are permitted, interline, pure interline online, pure interline interline connections are prohibited.

## Entity List

Mapping of entity ties to their origin and destination regions. data/demo/entList.dat.

### Header
1. ORGREGNCD: Origin Region code
2. DESTRGNCD: Destination Region code
3. ENTNM: Entity name

### Example Records

AFR,AFR,AFR-AFR

- All markets departing and arriving in Africa.

MEX,AFR,NA-AFR

- All markets departing from Mexico and arriving in Africa belong to the entity NA-AFR (all markets from NA -> AFR). Subset of a larger entity.

## Itinerary Level Constraint Exceptions

data/demo/ItinLvlCnstExc.dat. Disallows certain itineraries from being built between geographies and carriers. 

Z is a wildcard value for level, an empty field is a wildcard in the geography value,
and ** is a wildcard for carrier. 

The fields that are relevant are determined by the number of connections, for example, a one stop connection will not have a third carrier. 

FREQUENCY is the same format as the SSIM table operating day of week field 1 == Monday, .. , 7 == Sunday, a blank instead of a digit means not on that day of the week.

### Header
1. ORIGIN_LEVEL: Origin Geography type
2. ORIGIN: Origin value
3. DESTINATION_LEVEL: Destination geography type
4. DESTINATION: Destination value
5. CNCTNUMBER: Number of connection points
6. CARRIER1: First leg carrier
7. CNCT1_LEVEL: Connection point 1 geography type
8. CNCT1: Connection point 1 value
9. CARRIER2: Second leg carrier
10. CNCT2_LEVEL: Connection point 2 geography type
11. CNCT2: Connection point 2 value
12. CARRIER3: Third leg carrier
13. FREQUENCY: Day of week
14. TYPE: Exception type

### Example Records

Z,,Z,1,AA,Z,,AC,Z,,**,1234567,P

- Prohibit any one stop connection betwen AA and AC anywhere (*_LEVEL Z is wildcard, empty field is wildcard, ** is wildcard) on any day of the week

Z,,Z,,2,UA,Z,,**,Z,,DL,1234567,P

- Prohibit any 2 stop connection where the first flight leg is UA and the final destination leg is DL any day of the week, at any location, with any second carrier.

## Alliance Preference

data/demo/alliancePref.dat. Specify alliance and geography to select some value from this table. **This is currently not understood**.

### Header
1. ALLNCENM: Alliance Name
2. ORGLVL: Origin Level
3. ORG: Origin
4. DESTLVL: Destination level
5. DEST: Destination
6. ENTNM: Entity
7. LONSTOP: Unknown
8. HRNSTOP; Unknown
9. LRNSTOP: Unknown
10. HOCONN: Unknown
11. LOCONN: Unknown
12. HRCONN: Unknown
13. LRCONN: Unknown
14. HOINTR: Unknown
15. LOINTR: Unknown
16. HRINTR: Unknown
17. LRINTR: Unknown

### Example Records

UA-UK,Z,*,Z,*,*,70,70,70,70,70,70,70,70,70,70,70

The alliance UA-UK at any origin or destination station has a value of 70 for all of the remaining fields. **These values are not currently understood.**
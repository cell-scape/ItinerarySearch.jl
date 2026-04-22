# src/types/status.jl — StatusBits constants, sentinels, and query helpers

"""
    StatusBits constants — day-of-week bitmasks

Seven single-bit constants covering ISO weekday positions 0–6 (bit 0 = Monday,
bit 6 = Sunday).  Stored in bits 0–6 of a `StatusBits` (`UInt16`) value so they
never overlap with the classification constants at bits 7–12.

- `DOW_MON = 0x0001` — Monday
- `DOW_TUE = 0x0002` — Tuesday
- `DOW_WED = 0x0004` — Wednesday
- `DOW_THU = 0x0008` — Thursday
- `DOW_FRI = 0x0010` — Friday
- `DOW_SAT = 0x0020` — Saturday
- `DOW_SUN = 0x0040` — Sunday
- `DOW_MASK = 0x007f` — All seven day bits set (used to extract or clear DOW)
"""
const DOW_MON = StatusBits(1 << 0)    # 0x0001
const DOW_TUE = StatusBits(1 << 1)    # 0x0002
const DOW_WED = StatusBits(1 << 2)    # 0x0004
const DOW_THU = StatusBits(1 << 3)    # 0x0008
const DOW_FRI = StatusBits(1 << 4)    # 0x0010
const DOW_SAT = StatusBits(1 << 5)    # 0x0020
const DOW_SUN = StatusBits(1 << 6)    # 0x0040
const DOW_MASK = StatusBits(0x007f)

"""
    StatusBits constants — connection / leg classification bitmasks

Single-bit constants at positions 7–13.  Each bit independently marks a
property of a leg or connection; bits may be combined freely via `|`.

- `STATUS_INTERNATIONAL = 0x0080` — At least one endpoint is outside the domestic zone
- `STATUS_INTERLINE     = 0x0100` — Connection-level: marketing carriers differ
  across the connection's two legs (online ↔ interline distinction).  An
  itinerary-level `is_interline` rolls up "any cnx had a marketing change".
- `STATUS_ROUNDTRIP     = 0x0200` — Part of a round-trip itinerary
- `STATUS_CODESHARE     = 0x0400` — Leg-level (rolled up): at least one leg
  in scope has marketing carrier+flight ≠ operating carrier+flight on its
  own record.  Independent of `STATUS_INTERLINE` — a leg can be a codeshare
  inside an online itinerary, and an interline cnx can be made of host
  flights with no codeshares.
- `STATUS_THROUGH       = 0x0800` — Through-service / hidden stop (not a true connection)
- `STATUS_WETLEASE      = 0x1000` — Aircraft and crew wet-leased from another operator
- `STATUS_CNX_OP_THROUGH = 0x2000` — Connection-level qualifier on
  STATUS_INTERLINE: this interline cnx is mediated by a single operator
  carrying through despite the marketing brand change (the airline-industry
  "codeshare connection" concept).  Used by the rule chain to distinguish
  `INTERLINE_CODESHARE` (allow op-through interlines) from `INTERLINE_ALL`
  (allow all interlines).
"""
const STATUS_INTERNATIONAL  = StatusBits(1 << 7)    # 0x0080
const STATUS_INTERLINE      = StatusBits(1 << 8)    # 0x0100
const STATUS_ROUNDTRIP      = StatusBits(1 << 9)    # 0x0200
const STATUS_CODESHARE      = StatusBits(1 << 10)   # 0x0400
const STATUS_THROUGH        = StatusBits(1 << 11)   # 0x0800
const STATUS_WETLEASE       = StatusBits(1 << 12)   # 0x1000
const STATUS_CNX_OP_THROUGH = StatusBits(1 << 13)   # 0x2000

"""
    `is_international(s::StatusBits)::Bool`
---

# Description
- Returns `true` when the `STATUS_INTERNATIONAL` bit is set in `s`

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if at least one endpoint is international
"""
@inline is_international(s::StatusBits) = (s & STATUS_INTERNATIONAL) != StatusBits(0)

"""
    `is_interline(s::StatusBits)::Bool`
---

# Description
- Connection-level: `true` when the marketing carriers differ across the
  connection's two legs (regardless of whether operating carriers also
  differ — for that distinction, see `is_cnx_op_through`).
- Itinerary-level (rolled up): `true` when at least one connection had a
  marketing-carrier change.

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if marketing carrier changes at this connection (or anywhere in this itinerary)
"""
@inline is_interline(s::StatusBits) = (s & STATUS_INTERLINE) != StatusBits(0)

"""
    `is_codeshare(s::StatusBits)::Bool`
---

# Description
- Leg-level (rolled up): `true` when at least one leg in this scope has
  marketing carrier+flight differing from operating carrier+flight on its
  own record (the per-leg codeshare property).
- Independent of `is_interline`: a host-only itinerary (no marketing-carrier
  change) can still be a codeshare itinerary (an individual leg is a
  codeshare alias of another carrier's flight).

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if any leg in scope is a codeshare
"""
@inline is_codeshare(s::StatusBits) = (s & STATUS_CODESHARE) != StatusBits(0)

"""
    `is_cnx_op_through(s::StatusBits)::Bool`
---

# Description
- Connection-level qualifier on `STATUS_INTERLINE`: `true` when this is an
  interline connection mediated by a single operator carrying through
  despite the marketing brand change (the airline-industry "codeshare
  connection" concept — same physical aircraft, different marketed flights).
- Used by the rule chain's `INTERLINE_CODESHARE` filter mode to admit these
  while rejecting connections where the operating carrier also changes.
- Always implies `is_interline(s) == true` (it's only set on interline cnxs).

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if this interline cnx has the same operating carrier on both sides
"""
@inline is_cnx_op_through(s::StatusBits) = (s & STATUS_CNX_OP_THROUGH) != StatusBits(0)

"""
    `is_roundtrip(s::StatusBits)::Bool`
---

# Description
- Returns `true` when the `STATUS_ROUNDTRIP` bit is set in `s`

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if this leg or connection is part of a round-trip itinerary
"""
@inline is_roundtrip(s::StatusBits) = (s & STATUS_ROUNDTRIP) != StatusBits(0)

"""
    `is_through(s::StatusBits)::Bool`
---

# Description
- Returns `true` when the `STATUS_THROUGH` bit is set in `s`

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if this is a through-service / hidden-stop leg
"""
@inline is_through(s::StatusBits) = (s & STATUS_THROUGH) != StatusBits(0)

"""
    `is_wetlease(s::StatusBits)::Bool`
---

# Description
- Returns `true` when the `STATUS_WETLEASE` bit is set in `s`

# Arguments
1. `s::StatusBits`: status bitmask to test

# Returns
- `::Bool`: `true` if the aircraft and crew are wet-leased from another operator
"""
@inline is_wetlease(s::StatusBits) = (s & STATUS_WETLEASE) != StatusBits(0)

"""
    `dow_bit(iso_day::Int)::StatusBits`
---

# Description
- Converts an ISO weekday number to the corresponding `DOW_*` bitmask constant
- ISO numbering: 1 = Monday, 7 = Sunday (matches Julia `Dates.dayofweek`)

# Arguments
1. `iso_day::Int`: ISO weekday number in the range 1–7

# Returns
- `::StatusBits`: single-bit mask for the given weekday

# Examples
```julia
julia> dow_bit(1) == DOW_MON
true
julia> dow_bit(7) == DOW_SUN
true
```
"""
@inline dow_bit(iso_day::Int) = StatusBits(1 << (iso_day - 1))

"""
    Wildcard sentinel constants

Used as match-anything criteria in MCT lookup and connection-rule chains.
A field set to one of these values signals "accept any value in this position".

- `WILDCARD_STATION  = StationCode("*")` — matches any station code
- `WILDCARD_AIRLINE  = AirlineCode("*")` — matches any airline code
- `WILDCARD_COUNTRY  = InlineString3("*")` — matches any country code
- `WILDCARD_REGION   = InlineString3("*")` — matches any region code
- `WILDCARD_FLIGHTNO = FlightNumber(-1)` — matches any flight number
"""
const WILDCARD_STATION  = StationCode("*")
const WILDCARD_AIRLINE  = AirlineCode("*")
const WILDCARD_COUNTRY  = InlineString3("*")
const WILDCARD_REGION   = InlineString3("*")
const WILDCARD_FLIGHTNO = FlightNumber(-1)

"""
    Empty / null sentinel constants

Used to represent "not set" or "no value" in optional fields of isbits structs
(where `nothing` and `missing` are not available without boxing).

- `NO_STATION  = StationCode("")`  — empty station (field not populated)
- `NO_AIRLINE  = AirlineCode("")`  — empty airline (field not populated)
- `NO_MINUTES  = Minutes(-1)`      — invalid/missing time value
- `NO_DISTANCE = Distance(-1.0f0)` — invalid/missing distance value
- `NO_FLIGHTNO = FlightNumber(0)`  — unset flight number (0 is never a valid flight number)
"""
const NO_STATION  = StationCode("")
const NO_AIRLINE  = AirlineCode("")
const NO_MINUTES  = Minutes(-1)
const NO_DISTANCE = Distance(-1.0f0)
const NO_FLIGHTNO = FlightNumber(0)

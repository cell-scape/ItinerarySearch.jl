# src/ingest/schemas.jl — Parsing helper functions for SSIM and MCT fixed-width records
# FixedWidthParsers provides the canonical SSIM_SCHEMA, MCT_SCHEMA, AIRPORT_SCHEMA,
# and REGIONAL_SCHEMA constants used by the post-ingest pipeline and reference loaders.

using Dates
using FixedWidthParsers: SSIM_SCHEMA, MCT_SCHEMA, AIRPORT_SCHEMA, REGIONAL_SCHEMA

# ── Parsing helpers ──────────────────────────────────────────────

const MONTH_MAP = Dict{String,Int}(
    "JAN" => 1, "FEB" => 2, "MAR" => 3, "APR" => 4,
    "MAY" => 5, "JUN" => 6, "JUL" => 7, "AUG" => 8,
    "SEP" => 9, "OCT" => 10, "NOV" => 11, "DEC" => 12,
)

"""
    `parse_ddmonyy(s::AbstractString)::Date`
---

# Description
- Parse DDMONYY date format (e.g. "01JAN26" → Date(2026,1,1))
- SSIM standard date format
- Years 00-79 → 2000s, 80-99 → 1900s

# Arguments
1. `s::AbstractString`: 7-character date string in DDMONYY format

# Returns
- `::Date`: parsed date, or Date(1900,1,1) on parse failure
"""
function parse_ddmonyy(s::AbstractString)::Date
    s = strip(String(s))
    length(s) < 7 && return Date(1900, 1, 1)
    d = parse(Int, s[1:2])
    m = get(MONTH_MAP, uppercase(s[3:5]), 0)
    m == 0 && return Date(1900, 1, 1)
    y = parse(Int, s[6:7])
    year = y < 80 ? 2000 + y : 1900 + y
    Date(year, m, d)
end

"""
    `parse_hhmm(s::AbstractString)::Int16`
---

# Description
- Parse HHMM time string to minutes since midnight
- "2400" → 0 (same as midnight per SSIM convention)
- Blank or whitespace-only → 0

# Arguments
1. `s::AbstractString`: 4-character time string in HHMM format

# Returns
- `::Int16`: minutes since midnight (0–1439)
"""
function parse_hhmm(s::AbstractString)::Int16
    s = strip(String(s))
    isempty(s) && return Int16(0)
    length(s) < 4 && return Int16(0)
    h = parse(Int, s[1:2])
    m = parse(Int, s[3:4])
    h == 24 && m == 0 && return Int16(0)
    Int16(h * 60 + m)
end

"""
    `parse_frequency_bitmask(s::AbstractString)::UInt8`
---

# Description
- Parse 7-character SSIM frequency string to a day-of-week bitmask
- Position 1=Monday (bit 0) through 7=Sunday (bit 6)
- Digit at position i means the flight operates; space means it does not

# Arguments
1. `s::AbstractString`: 7-character frequency string (e.g. "1234567", "1 3 5 7")

# Returns
- `::UInt8`: bitmask with bit i-1 set if position i is non-space

# Examples
```julia
julia> parse_frequency_bitmask("1234567")
0x7f
julia> parse_frequency_bitmask("1 3 5 7")
0x55
```
"""
function parse_frequency_bitmask(s::AbstractString)::UInt8
    s = String(s)
    result = UInt8(0)
    for i in 1:min(7, length(s))
        if s[i] != ' '
            result |= UInt8(1) << (i - 1)
        end
    end
    result
end

"""
    `parse_date_var(s::AbstractString)::Int8`
---

# Description
- Parse SSIM date variation character to an integer offset
- '0' or blank → 0, '1' → +1, '2' → +2, 'A' → -1 (previous day)

# Arguments
1. `s::AbstractString`: single-character date variation field

# Returns
- `::Int8`: day offset (-1, 0, 1, or 2)
"""
function parse_date_var(s::AbstractString)::Int8
    c = length(strip(String(s))) > 0 ? strip(String(s))[1] : ' '
    c == 'A' && return Int8(-1)
    c == '1' && return Int8(1)
    c == '2' && return Int8(2)
    Int8(0)
end

"""
    `parse_utc_offset(s::AbstractString)::Int16`
---

# Description
- Parse a UTC offset field (5 characters: ±HHMM) to minutes
- Example: "+0530" → 330, "-0600" → -360

# Arguments
1. `s::AbstractString`: 5-character offset string in ±HHMM format

# Returns
- `::Int16`: offset in minutes (negative for west of UTC)
"""
function parse_utc_offset(s::AbstractString)::Int16
    s = String(s)
    length(s) < 5 && return Int16(0)
    sign = s[1] == '-' ? -1 : 1
    h_str = s[2:3]
    m_str = s[4:5]
    h = tryparse(Int, strip(h_str))
    m = tryparse(Int, strip(m_str))
    h === nothing && (h = 0)
    m === nothing && (m = 0)
    Int16(sign * (h * 60 + m))
end

"""
    `parse_serial(s::AbstractString)::UInt32`
---

# Description
- Parse a 6-digit right-justified record serial number to UInt32
- Blank or unparseable → 0

# Arguments
1. `s::AbstractString`: up to 6-character numeric string

# Returns
- `::UInt32`: serial number, or 0 if blank/invalid
"""
function parse_serial(s::AbstractString)::UInt32
    n = tryparse(UInt32, strip(String(s)))
    n === nothing ? UInt32(0) : n
end

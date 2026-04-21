# src/types/aliases.jl — Domain value type aliases
# All types are isbits (stack-allocated, no GC pressure).
# Changing a type here propagates everywhere.

"""
    StationCode

3-character IATA airport code (e.g. `"ORD"`, `"LAX"`). Alias for `InlineString3`
— `isbits`, stack-allocated, and safe to store in concretely typed vectors.
"""
const StationCode   = InlineString3

"""
    AirlineCode

2-character IATA carrier code (e.g. `"UA"`, `"NH"`). Alias for `InlineString3`
so the trailing space padding from SSIM source data round-trips safely.
"""
const AirlineCode   = InlineString3

"""
    FlightNumber

Up to 4-digit flight number. `Int16` range (−32_768..32_767) accommodates
values well beyond the 4-digit IATA maximum.
"""
const FlightNumber  = Int16

"""
    Minutes

Clock-time or duration in minutes (departure, arrival, MCT, elapsed). `Int16`
range supports up to ~22 days of durations; single-date times fit within 0–1439.
"""
const Minutes       = Int16

"""
    Distance

Distance in statute miles (or nautical miles in some reference tables). `Float32`
chosen for compact isbits layout — precision is ample for miles-level values.
"""
const Distance      = Float32

"""
    StatusBits

16-bit bitmask for connection/leg classification flags (international, interline,
codeshare, through, DOW bits, etc.). See `src/types/status.jl` for bit positions.
"""
const StatusBits    = UInt16

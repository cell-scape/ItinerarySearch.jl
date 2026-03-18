# src/types/aliases.jl — Domain value type aliases
# All types are isbits (stack-allocated, no GC pressure).
# Changing a type here propagates everywhere.

const StationCode   = InlineString7     # 3-char IATA airport (ORD, LAX)
const AirlineCode   = InlineString3     # 2-char IATA carrier (UA, NH)
const FlightNumber  = Int16             # 4-digit flight number
const Minutes       = Int16             # time in minutes (departure, arrival, MCT)
const Distance      = Float32           # distance in miles or nautical miles
const StatusBits    = UInt16            # bitmask for connection/leg status flags

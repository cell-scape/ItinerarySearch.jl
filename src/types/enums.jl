# src/types/enums.jl — CEnum types carried forward from TripBuilder

"""
    @cenum MCTStatus::Int8

Minimum Connecting Time domestic/international status.
1-indexed to match TripBuilder and SSIM8 array indices.

- `MCT_DD = 1` — Domestic arrival → Domestic departure
- `MCT_DI = 2` — Domestic arrival → International departure
- `MCT_ID = 3` — International arrival → Domestic departure
- `MCT_II = 4` — International arrival → International departure
"""
@cenum MCTStatus::Int8 MCT_DD=1 MCT_DI=2 MCT_ID=3 MCT_II=4

"""
    @cenum Cabin::Int8

Airline cabin class categories.

- `CABIN_J = 0` — First/Premium
- `CABIN_O = 1` — Business
- `CABIN_Y = 2` — Coach/Economy
"""
@cenum Cabin::Int8 CABIN_J=0 CABIN_O=1 CABIN_Y=2

"""
    @cenum ScopeMode::Int8

Connection/itinerary scope filter.

- `SCOPE_ALL = 0` — All connections
- `SCOPE_DOM = 1` — Domestic only
- `SCOPE_INTL = 2` — International only
"""
@cenum ScopeMode::Int8 SCOPE_ALL=0 SCOPE_DOM=1 SCOPE_INTL=2

"""
    @cenum InterlineMode::Int8

Interline connection filter.

- `INTERLINE_ONLINE = 0` — Same carrier only
- `INTERLINE_CODESHARE = 1` — Same carrier + codeshare partners
- `INTERLINE_ALL = 2` — Any carrier combination
"""
@cenum InterlineMode::Int8 INTERLINE_ONLINE=0 INTERLINE_CODESHARE=1 INTERLINE_ALL=2

"""
    @cenum MCTSource::UInt8

Source of an MCT lookup result. Isbits-compatible (unlike Symbol).

- `SOURCE_EXCEPTION = 0` — Carrier/equipment-specific exception record
- `SOURCE_STATION_STANDARD = 1` — Station standard (no carrier specificity)
- `SOURCE_GLOBAL_DEFAULT = 2` — Global default (no matching MCT record)
"""
@cenum MCTSource::UInt8 SOURCE_EXCEPTION=0 SOURCE_STATION_STANDARD=1 SOURCE_GLOBAL_DEFAULT=2

"""
    `parse_mct_status(s::AbstractString)::MCTStatus`

Convert 2-character MCT status string to MCTStatus enum.
"""
function parse_mct_status(s::AbstractString)::MCTStatus
    s == "DD" && return MCT_DD
    s == "DI" && return MCT_DI
    s == "ID" && return MCT_ID
    s == "II" && return MCT_II
    error("Unknown MCT status: $s")
end

# Default MCT times (minutes) per status — used when no station-specific MCT found
const MCT_DEFAULTS = Dict{MCTStatus, Minutes}(
    MCT_DD => Int16(30),
    MCT_DI => Int16(60),
    MCT_ID => Int16(90),
    MCT_II => Int16(90),
)

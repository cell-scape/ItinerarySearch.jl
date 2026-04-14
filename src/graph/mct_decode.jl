# src/graph/mct_decode.jl — Human-readable MCT bitmask decoding

# Ordered mapping from MCT_BIT_* constants to human-readable names.
# Order matches bit position (0 → 21) for consistent output.
const _MCT_BIT_NAMES = (
    (MCT_BIT_ARR_CARRIER,   "ARR_CARRIER"),
    (MCT_BIT_DEP_CARRIER,   "DEP_CARRIER"),
    (MCT_BIT_ARR_TERM,      "ARR_TERM"),
    (MCT_BIT_DEP_TERM,      "DEP_TERM"),
    (MCT_BIT_PRV_STN,       "PRV_STN"),
    (MCT_BIT_NXT_STN,       "NXT_STN"),
    (MCT_BIT_PRV_COUNTRY,   "PRV_COUNTRY"),
    (MCT_BIT_NXT_COUNTRY,   "NXT_COUNTRY"),
    (MCT_BIT_PRV_REGION,    "PRV_REGION"),
    (MCT_BIT_NXT_REGION,    "NXT_REGION"),
    (MCT_BIT_DEP_BODY,      "DEP_BODY"),
    (MCT_BIT_ARR_BODY,      "ARR_BODY"),
    (MCT_BIT_ARR_CS_IND,    "ARR_CS_IND"),
    (MCT_BIT_ARR_CS_OP,     "ARR_CS_OP"),
    (MCT_BIT_DEP_CS_IND,    "DEP_CS_IND"),
    (MCT_BIT_DEP_CS_OP,     "DEP_CS_OP"),
    (MCT_BIT_ARR_ACFT_TYPE, "ARR_ACFT_TYPE"),
    (MCT_BIT_DEP_ACFT_TYPE, "DEP_ACFT_TYPE"),
    (MCT_BIT_ARR_FLT_RNG,   "ARR_FLT_RNG"),
    (MCT_BIT_DEP_FLT_RNG,   "DEP_FLT_RNG"),
    (MCT_BIT_PRV_STATE,     "PRV_STATE"),
    (MCT_BIT_NXT_STATE,     "NXT_STATE"),
)

"""
    `decode_matched_fields(bitmask::UInt32)::String`
---

# Description
- Decode an MCT `matched_fields` or `specified` bitmask into a comma-separated
  list of human-readable field names
- Returns empty string for zero bitmask

# Arguments
1. `bitmask::UInt32`: the bitmask to decode

# Returns
- `::String`: comma-separated field names (e.g. "ARR_CARRIER,DEP_CARRIER,ARR_TERM")
"""
function decode_matched_fields(bitmask::UInt32)::String
    bitmask == UInt32(0) && return ""
    parts = String[]
    for (bit, name) in _MCT_BIT_NAMES
        (bitmask & bit) != 0 && push!(parts, name)
    end
    join(parts, ",")
end

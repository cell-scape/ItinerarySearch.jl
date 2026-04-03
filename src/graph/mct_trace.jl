# src/graph/mct_trace.jl — Traced MCT lookup wrapper

"""
    `function lookup_mct_traced(lookup::MCTLookup, arr_carrier, dep_carrier, arr_station, dep_station, status; kwargs...)::MCTTrace`
---

# Description
- Convenience wrapper around `lookup_mct` that captures the full cascade trace
- Allocates a `Vector{MCTCandidateTrace}` and passes it as the `trace` kwarg
- Returns an `MCTTrace` with all input parameters, candidates, and result

# Arguments
Same as `lookup_mct`

# Returns
- `::MCTTrace`: full trace of the lookup
"""
function lookup_mct_traced(
    lookup::MCTLookup,
    arr_carrier::AirlineCode,
    dep_carrier::AirlineCode,
    arr_station::StationCode,
    dep_station::StationCode,
    status::MCTStatus;
    arr_body::Char = ' ',
    dep_body::Char = ' ',
    prv_stn::StationCode = NO_STATION,
    nxt_stn::StationCode = NO_STATION,
    arr_term::InlineString3 = InlineString3(""),
    dep_term::InlineString3 = InlineString3(""),
    arr_op_carrier::AirlineCode = NO_AIRLINE,
    dep_op_carrier::AirlineCode = NO_AIRLINE,
    arr_is_codeshare::Bool = false,
    dep_is_codeshare::Bool = false,
    arr_acft_type::InlineString7 = InlineString7(""),
    dep_acft_type::InlineString7 = InlineString7(""),
    arr_flt_no::FlightNumber = FlightNumber(0),
    dep_flt_no::FlightNumber = FlightNumber(0),
    prv_country::InlineString3 = InlineString3(""),
    nxt_country::InlineString3 = InlineString3(""),
    prv_state::InlineString3 = InlineString3(""),
    nxt_state::InlineString3 = InlineString3(""),
    prv_region::InlineString3 = InlineString3(""),
    nxt_region::InlineString3 = InlineString3(""),
    target_date::UInt32 = UInt32(0),
)::MCTTrace
    candidates = MCTCandidateTrace[]
    result = lookup_mct(
        lookup, arr_carrier, dep_carrier, arr_station, dep_station, status;
        arr_body, dep_body, prv_stn, nxt_stn, arr_term, dep_term,
        arr_op_carrier, dep_op_carrier, arr_is_codeshare, dep_is_codeshare,
        arr_acft_type, dep_acft_type, arr_flt_no, dep_flt_no,
        prv_country, nxt_country, prv_state, nxt_state,
        prv_region, nxt_region, target_date,
        trace = candidates,
    )
    MCTTrace(;
        arr_carrier, dep_carrier, arr_station, dep_station, status,
        arr_body, dep_body, prv_stn, nxt_stn, arr_term, dep_term,
        arr_op_carrier, dep_op_carrier, arr_is_codeshare, dep_is_codeshare,
        arr_acft_type, dep_acft_type, arr_flt_no, dep_flt_no,
        prv_country, nxt_country, prv_state, nxt_state,
        prv_region, nxt_region, target_date,
        candidates, result,
    )
end

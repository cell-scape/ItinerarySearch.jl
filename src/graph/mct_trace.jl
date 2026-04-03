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

"""
    `function lookup_mct_codeshare_traced(lookup, arr_carrier, dep_carrier, ...; arr_op_carrier, dep_op_carrier, arr_op_flt_no, dep_op_flt_no, arr_is_codeshare, dep_is_codeshare, ...)::MCTTrace`
---

# Description
- Codeshare-aware traced MCT lookup matching production's `_mct_codeshare_resolve`
- For codeshare flights, performs two lookups:
  1. Marketing: marketing carriers + codeshare flags → finds codeshare-specific MCTs
  2. Operating: operating carriers, no codeshare flags → finds operating carrier MCTs
- Picks the result with highest specificity (marketing wins ties)
- For non-codeshare flights, performs a single lookup (no overhead)
- Both lookups' candidates are merged into the trace

# Returns
- `::MCTTrace`: with `marketing_result`, `operating_result`, and `codeshare_mode` populated
"""
function lookup_mct_codeshare_traced(
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
    arr_op_flt_no::FlightNumber = FlightNumber(0),
    dep_op_flt_no::FlightNumber = FlightNumber(0),
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
    # Common kwargs for both lookups
    common = (;
        arr_body, dep_body, prv_stn, nxt_stn, arr_term, dep_term,
        arr_acft_type, dep_acft_type,
        prv_country, nxt_country, prv_state, nxt_state,
        prv_region, nxt_region, target_date,
    )

    # ── Marketing lookup: marketing carriers + flight numbers + codeshare context
    mkt_candidates = MCTCandidateTrace[]
    marketing_result = lookup_mct(
        lookup, arr_carrier, dep_carrier, arr_station, dep_station, status;
        common...,
        arr_op_carrier, dep_op_carrier,
        arr_is_codeshare, dep_is_codeshare,
        arr_flt_no, dep_flt_no,
        trace = mkt_candidates,
    )

    # If neither leg is a codeshare, single lookup is sufficient
    if !arr_is_codeshare && !dep_is_codeshare
        return MCTTrace(;
            arr_carrier, dep_carrier, arr_station, dep_station, status,
            common...,
            arr_op_carrier, dep_op_carrier,
            arr_is_codeshare, dep_is_codeshare,
            arr_flt_no, dep_flt_no,
            candidates = mkt_candidates,
            result = marketing_result,
            marketing_result,
            codeshare_mode = :none,
        )
    end

    # ── Operating lookup: operating carriers + flight numbers, no codeshare flags
    op_arr_carrier = arr_is_codeshare ? arr_op_carrier : arr_carrier
    op_dep_carrier = dep_is_codeshare ? dep_op_carrier : dep_carrier
    op_arr_flt = arr_is_codeshare ? arr_op_flt_no : arr_flt_no
    op_dep_flt = dep_is_codeshare ? dep_op_flt_no : dep_flt_no

    op_candidates = MCTCandidateTrace[]
    operating_result = lookup_mct(
        lookup, op_arr_carrier, op_dep_carrier, arr_station, dep_station, status;
        common...,
        arr_op_carrier = NO_AIRLINE,
        dep_op_carrier = NO_AIRLINE,
        arr_is_codeshare = false,
        dep_is_codeshare = false,
        arr_flt_no = op_arr_flt,
        dep_flt_no = op_dep_flt,
        trace = op_candidates,
    )

    # Higher specificity wins; marketing preferred at equal specificity
    if operating_result.specificity > marketing_result.specificity
        winner = operating_result
        mode = :operating
    else
        winner = marketing_result
        mode = :marketing
    end

    # Merge candidates from both lookups
    all_candidates = vcat(mkt_candidates, op_candidates)

    MCTTrace(;
        arr_carrier, dep_carrier, arr_station, dep_station, status,
        common...,
        arr_op_carrier, dep_op_carrier,
        arr_is_codeshare, dep_is_codeshare,
        arr_flt_no, dep_flt_no,
        candidates = all_candidates,
        result = winner,
        marketing_result,
        operating_result,
        codeshare_mode = mode,
    )
end

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
- For codeshare flights, performs up to four lookups (YY, YN, NY, NN):
  1. YY — Marketing: marketing carriers + both codeshare flags
  2. YN — Dep-CS-only: marketing dep + operating arr, dep CS flag (both-CS only)
  3. NY — Arr-CS-only: operating dep + marketing arr, arr CS flag (both-CS only)
  4. NN — Operating: operating carriers, no codeshare flags
- Mixed lookups (YN, NY) only fire when both legs are codeshare
- NN establishes the time floor; codeshare results must have time >= operating time
- Picks the result with highest specificity subject to the time floor constraint
- All lookups' candidates are merged into the trace

# Returns
- `::MCTTrace`: with `marketing_result`, `operating_result`, `dep_cs_result`, `arr_cs_result`, and `codeshare_mode` populated
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
    # Common kwargs for all lookups
    common = (;
        arr_body, dep_body, prv_stn, nxt_stn, arr_term, dep_term,
        arr_acft_type, dep_acft_type,
        prv_country, nxt_country, prv_state, nxt_state,
        prv_region, nxt_region, target_date,
    )

    # ── YY: Marketing lookup — marketing carriers + codeshare context (both sides)
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

    op_arr_carrier_resolved = arr_is_codeshare ? arr_op_carrier : arr_carrier
    op_dep_carrier_resolved = dep_is_codeshare ? dep_op_carrier : dep_carrier
    op_arr_flt = arr_is_codeshare ? arr_op_flt_no : arr_flt_no
    op_dep_flt = dep_is_codeshare ? dep_op_flt_no : dep_flt_no

    # ── NN: Operating lookup first — establishes the time floor.
    # Per SSIM Ch. 8: a marketing carrier can only request a longer MCT
    # than the operating carrier.
    op_candidates = MCTCandidateTrace[]
    operating_result = lookup_mct(
        lookup, op_arr_carrier_resolved, op_dep_carrier_resolved, arr_station, dep_station, status;
        common...,
        arr_op_carrier = NO_AIRLINE,
        dep_op_carrier = NO_AIRLINE,
        arr_is_codeshare = false,
        dep_is_codeshare = false,
        arr_flt_no = op_arr_flt,
        dep_flt_no = op_dep_flt,
        trace = op_candidates,
    )

    # Start with operating as baseline; codeshare results must have higher
    # specificity AND time >= operating time to override
    best = operating_result
    mode = :operating
    all_candidates = vcat(mkt_candidates, op_candidates)
    dep_cs_result = EMPTY_MCT_RESULT
    arr_cs_result = EMPTY_MCT_RESULT

    if marketing_result.specificity > best.specificity &&
       marketing_result.time >= operating_result.time
        best = marketing_result
        mode = :marketing
    end

    # ── Mixed lookups: only needed when both legs are codeshare ───────────
    if arr_is_codeshare && dep_is_codeshare
        # YN: dep CS only — marketing dep carrier + operating arr carrier
        yn_candidates = MCTCandidateTrace[]
        dep_cs_result = lookup_mct(
            lookup, op_arr_carrier_resolved, dep_carrier, arr_station, dep_station, status;
            common...,
            arr_op_carrier = NO_AIRLINE,
            dep_op_carrier,
            arr_is_codeshare = false,
            dep_is_codeshare,
            arr_flt_no = op_arr_flt,
            dep_flt_no,
            trace = yn_candidates,
        )
        append!(all_candidates, yn_candidates)
        if dep_cs_result.specificity > best.specificity &&
           dep_cs_result.time >= operating_result.time
            best = dep_cs_result
            mode = :dep_cs
        end

        # NY: arr CS only — operating dep carrier + marketing arr carrier
        ny_candidates = MCTCandidateTrace[]
        arr_cs_result = lookup_mct(
            lookup, arr_carrier, op_dep_carrier_resolved, arr_station, dep_station, status;
            common...,
            arr_op_carrier,
            dep_op_carrier = NO_AIRLINE,
            arr_is_codeshare,
            dep_is_codeshare = false,
            arr_flt_no,
            dep_flt_no = op_dep_flt,
            trace = ny_candidates,
        )
        append!(all_candidates, ny_candidates)
        if arr_cs_result.specificity > best.specificity &&
           arr_cs_result.time >= operating_result.time
            best = arr_cs_result
            mode = :arr_cs
        end
    end

    MCTTrace(;
        arr_carrier, dep_carrier, arr_station, dep_station, status,
        common...,
        arr_op_carrier, dep_op_carrier,
        arr_is_codeshare, dep_is_codeshare,
        arr_flt_no, dep_flt_no,
        candidates = all_candidates,
        result = best,
        marketing_result,
        operating_result,
        dep_cs_result,
        arr_cs_result,
        codeshare_mode = mode,
    )
end

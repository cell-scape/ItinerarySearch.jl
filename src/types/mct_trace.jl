# src/types/mct_trace.jl — MCT audit trace types
#
# Provides:
#   - EMPTY_MCT_RESULT  — sentinel constant for "no MCT result"
#   - MCTTrace          — full trace of a single MCT lookup
#
# Note: MCTAuditConfig is defined in types/mct_audit_config.jl (included before
#       config.jl so that SearchConfig can embed it as a field).
# Note: MCTCandidateTrace is defined in graph/mct_lookup.jl (after MCTRecord)

"""
    `EMPTY_MCT_RESULT`

Sentinel `MCTResult` representing "no result." All fields zeroed/defaulted.
Test via `r === EMPTY_MCT_RESULT` (isbits bitwise comparison).
"""
const EMPTY_MCT_RESULT = MCTResult(
    time           = Minutes(0),
    queried_status = MCT_DD,
    matched_status = MCT_DD,
    suppressed     = false,
    source         = SOURCE_GLOBAL_DEFAULT,
    specificity    = UInt32(0),
    mct_id         = Int32(0),
    matched_fields = UInt32(0),
)

# MCTCandidateTrace is defined in graph/mct_lookup.jl (after MCTRecord,
# before lookup_mct) so that the trace kwarg type annotation resolves.

"""
    @kwdef struct MCTTrace

Full trace of a single MCT lookup: input parameters, all candidates evaluated
(in cascade order), the winning result, and codeshare resolution details.
"""
@kwdef struct MCTTrace
    # ── Input parameters ──
    arr_carrier::AirlineCode = NO_AIRLINE
    dep_carrier::AirlineCode = NO_AIRLINE
    arr_station::StationCode = NO_STATION
    dep_station::StationCode = NO_STATION
    status::MCTStatus = MCT_DD
    arr_body::Char = ' '
    dep_body::Char = ' '
    prv_stn::StationCode = NO_STATION
    nxt_stn::StationCode = NO_STATION
    arr_term::InlineString3 = InlineString3("")
    dep_term::InlineString3 = InlineString3("")
    arr_op_carrier::AirlineCode = NO_AIRLINE
    dep_op_carrier::AirlineCode = NO_AIRLINE
    arr_is_codeshare::Bool = false
    dep_is_codeshare::Bool = false
    arr_acft_type::InlineString7 = InlineString7("")
    dep_acft_type::InlineString7 = InlineString7("")
    arr_flt_no::FlightNumber = FlightNumber(0)
    dep_flt_no::FlightNumber = FlightNumber(0)
    prv_country::InlineString3 = InlineString3("")
    nxt_country::InlineString3 = InlineString3("")
    prv_state::InlineString3 = InlineString3("")
    nxt_state::InlineString3 = InlineString3("")
    prv_region::InlineString3 = InlineString3("")
    nxt_region::InlineString3 = InlineString3("")
    target_date::UInt32 = UInt32(0)

    # ── Connection station geography (for suppression scope) ──
    cnx_country::InlineString3 = InlineString3("")
    cnx_state::InlineString3 = InlineString3("")
    cnx_region::InlineString3 = InlineString3("")

    # ── Cascade output ──
    candidates::Vector{MCTCandidateTrace} = MCTCandidateTrace[]
    result::MCTResult = EMPTY_MCT_RESULT

    # ── Codeshare resolution ──
    marketing_result::MCTResult = EMPTY_MCT_RESULT
    operating_result::MCTResult = EMPTY_MCT_RESULT
    dep_cs_result::MCTResult = EMPTY_MCT_RESULT    # YN: dep CS only (both-CS connections)
    arr_cs_result::MCTResult = EMPTY_MCT_RESULT    # NY: arr CS only (both-CS connections)
    codeshare_mode::Symbol = :none                  # :none, :marketing, :operating, :dep_cs, :arr_cs
end

# MCTAuditConfig is defined in types/mct_audit_config.jl (included before config.jl).

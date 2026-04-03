# src/types/mct_trace.jl — MCT audit trace types
#
# Provides:
#   - EMPTY_MCT_RESULT  — sentinel constant for "no MCT result"
#   - MCTCandidateTrace — one candidate record's evaluation in the cascade
#   - MCTTrace          — full trace of a single MCT lookup
#   - MCTAuditConfig    — configuration for MCT audit logging

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

"""
    struct MCTCandidateTrace

Captures one candidate MCT record's evaluation during the cascade lookup.

# Fields
- `record::MCTRecord` — the candidate record
- `matched::Bool` — did all specified fields match?
- `skip_reason::Symbol` — `:none`, `:date_expired`, `:field_mismatch`, `:station_standard_skip`, `:supp_scope_miss`
- `pass::Symbol` — `:exception`, `:global_suppression`, `:station_standard`, `:global_default`
"""
struct MCTCandidateTrace
    record::MCTRecord
    matched::Bool
    skip_reason::Symbol
    pass::Symbol
end

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

    # ── Cascade output ──
    candidates::Vector{MCTCandidateTrace} = MCTCandidateTrace[]
    result::MCTResult = EMPTY_MCT_RESULT

    # ── Codeshare resolution ──
    marketing_result::MCTResult = EMPTY_MCT_RESULT
    operating_result::MCTResult = EMPTY_MCT_RESULT
    codeshare_mode::Symbol = :none
end

"""
    @kwdef struct MCTAuditConfig

Configuration for MCT audit logging during connection building.
Disabled by default.

# Fields
- `enabled::Bool` — enable audit logging (default `false`)
- `detail::Symbol` — `:summary` (CSV) or `:detailed` (JSONL)
- `output_path::String` — file path; empty string means stdout
- `max_connections::Int` — stop after N connections (0 = unlimited)
- `max_candidates::Int` — top N candidates in detailed mode (default 10)
"""
@kwdef struct MCTAuditConfig
    enabled::Bool = false
    detail::Symbol = :summary
    output_path::String = ""
    max_connections::Int = 0
    max_candidates::Int = 10
end

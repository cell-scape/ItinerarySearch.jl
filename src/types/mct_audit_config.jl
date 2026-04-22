# src/types/mct_audit_config.jl — MCTAuditConfig type definition
#
# Defined here (before config.jl) so that SearchConfig can embed it as a field.
# The remaining MCT trace types (EMPTY_MCT_RESULT, MCTTrace) live in
# types/mct_trace.jl, which is included after graph/mct_lookup.jl.

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

"""
    `MCTAuditConfig(d::AbstractDict)::MCTAuditConfig`

Construct an `MCTAuditConfig` from an `AbstractDict`. Accepts `String` or
`Symbol` keys; `detail` may be a `String` or `Symbol`. Unknown keys throw
`ArgumentError`.
"""
function MCTAuditConfig(d::AbstractDict)::MCTAuditConfig
    kw = _normalize_dict_keys(d)
    _validate_known_fields(kw, MCTAuditConfig)
    if haskey(kw, :detail) && kw[:detail] isa AbstractString
        kw[:detail] = Symbol(kw[:detail])
    end
    return MCTAuditConfig(; kw...)
end

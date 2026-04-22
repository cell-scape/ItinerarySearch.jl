# src/types/dict_ctor_helpers.jl — Shared helpers for AbstractDict-form
# constructors on @kwdef structs across the public API.
#
# Included early in the type-loading sequence (after aliases/enums/records/
# status/stats) so that later struct files (`constraints.jl`,
# `mct_audit_config.jl`) and `config.jl` can all use these helpers to build
# their own `Type(::AbstractDict)` constructors consistently.

"""
    `_normalize_dict_keys(d::AbstractDict)::Dict{Symbol,Any}`

Return a new `Dict{Symbol,Any}` whose keys come from `d` with
`AbstractString` keys converted via `Symbol`.  `Symbol` keys are copied
as-is.  Used by dict-form constructors to accept both JSON-loaded
(Symbol-keyed) and YAML/TOML-loaded (String-keyed) input shapes without
forcing callers to pre-normalize.
"""
function _normalize_dict_keys(d::AbstractDict)::Dict{Symbol,Any}
    out = Dict{Symbol,Any}()
    for (k, v) in d
        out[k isa Symbol ? k : Symbol(String(k))] = v
    end
    return out
end

"""
    `_validate_known_fields(kw::Dict{Symbol,Any}, ::Type{T}) where T`

Throw `ArgumentError` if any key in `kw` is not a `fieldname` of `T`.
Matches the error behaviour of `@kwdef`'s kwarg constructor — unknown
fields fail loud rather than silently pass through.  The error message
includes the full list of valid fields for quick diagnosis.
"""
function _validate_known_fields(kw::Dict{Symbol,Any}, ::Type{T}) where {T}
    valid = fieldnames(T)
    for k in keys(kw)
        k in valid || throw(ArgumentError(
            "unknown $(nameof(T)) field: `$k`. Valid fields: $(valid)",
        ))
    end
end

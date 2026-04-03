# src/config.jl — SearchConfig and JSON loading

"""
    `_default_path(filename::String)::String`
---

# Description
- Resolve a default data path relative to the package's `data/demo/` directory
- Falls back to a path relative to the current working directory if the package
  directory cannot be determined (e.g. during development without a package env)

# Arguments
1. `filename::String`: basename of the file under `data/demo/`

# Returns
- `::String`: absolute or relative path to the file
"""
function _default_path(filename::String)::String
    dir = pkgdir(@__MODULE__)
    if dir === nothing
        return joinpath("data", "input", filename)
    end
    joinpath(dir, "data", "input", filename)
end

"""
    `_parse_scope(s::AbstractString)::ScopeMode`

Parse a scope string from JSON config to ScopeMode enum.
"""
function _parse_scope(s::AbstractString)::ScopeMode
    s = lowercase(String(s))
    s == "all" && return SCOPE_ALL
    s == "dom" && return SCOPE_DOM
    s == "intl" && return SCOPE_INTL
    error("Unknown scope mode: $s. Expected: all, dom, intl")
end

"""
    `_parse_interline(s::AbstractString)::InterlineMode`

Parse an interline string from JSON config to InterlineMode enum.
"""
function _parse_interline(s::AbstractString)::InterlineMode
    s = lowercase(String(s))
    s == "online" && return INTERLINE_ONLINE
    s == "codeshare" && return INTERLINE_CODESHARE
    s == "all" && return INTERLINE_ALL
    error("Unknown interline mode: $s. Expected: online, codeshare, all")
end

"""
    @kwdef struct SearchConfig

Immutable configuration for ItinerarySearch. Every field has a sensible default.
Runtime changes via REST API construct a new SearchConfig and atomically swap
the reference — no locking, no mutation.

# Construction
- `SearchConfig()` — all defaults, uses demo data
- `SearchConfig(max_stops=3)` — override individual fields
- `load_config("path/to/config.json")` — load from JSON file

# Key Fields
- `allow_roundtrips::Bool` — when `false` (default), itineraries whose final
  destination equals their origin are rejected; when `true`, they are split at
  the farthest point from the origin and the two halves are committed separately
"""
@kwdef struct SearchConfig
    backend::String = "duckdb"
    db_path::String = ":memory:"
    max_stops::Int = 2
    max_connection_minutes::Int = 480
    max_elapsed_minutes::Int = 1440
    circuity_factor::Float64 = 2.5
    circuity_extra_miles::Float64 = 500.0
    scope::ScopeMode = SCOPE_ALL
    interline::InterlineMode = INTERLINE_CODESHARE
    max_days::Int = 1
    trailing_days::Int = 0
    ssim_path::String = _default_path("uaoa_ssim.new.dat")
    mct_path::String = _default_path("MCTIMFILUA.DAT")
    airports_path::String = _default_path("mdstua.txt")
    regions_path::String = _default_path("REGIMFILUA.DAT")
    aircrafts_path::String = _default_path("aircraft.txt")
    seats_path::String = _default_path("seats_ua.txt.DAT")
    classmap_path::String = _default_path("classmaptable.txt")
    serviceclass_path::String = _default_path("servclasstable.dat")
    oa_control_path::String = _default_path("oa_control_table.csv")
    leading_days::Int = 2
    metrics_level::Symbol = :full        # :basic, :aircraft, :full
    graph_export_path::String = "data/output"
    graph_import_path::String = "data/output"
    constraints_path::String = "data/output"
    event_log_enabled::Bool = false
    event_log_path::String = "data/output/events.jsonl"
    log_level::Symbol = :info
    log_json_path::String = ""
    log_stdout_json::Bool = false
    output_formats::Vector{Symbol} = [:json, :yaml, :csv]
    distance_formula::Symbol = :haversine  # :haversine or :vincenty
    allow_roundtrips::Bool = false
    mct_cache_enabled::Bool = true         # cache MCT lookup results during connection build
    mct_serial_ascending::Bool = true      # tiebreaker: true = lower serial wins (earlier record), false = higher serial wins (later record)
    mct_codeshare_mode::Symbol = :both     # :both = marketing+operating (default), :marketing = marketing only, :operating = operating only
    mct_schengen_mode::Symbol = :sch_then_eur  # :sch_then_eur (default), :eur_then_sch, :sch_only, :eur_only
    mct_suppressions_enabled::Bool = true    # false = ignore all suppression records in MCT lookup
    maft_enabled::Bool = true              # enable MAFT rule (both connection and itinerary level)
    interline_dcnx_enabled::Bool = true    # enable interline double-connect restriction
    crs_cnx_enabled::Bool = true           # enable CRS distance-based connection time rule
    mct_audit::MCTAuditConfig = MCTAuditConfig()    # MCT audit logging configuration
end

# ── JSON3 field extraction helpers ────────────────────────────────────────────
# JSON3 object field access returns a wide union type. These helpers narrow
# the return type so JET can verify correctness.

"""
    `_json_str(obj::JSON3.Object, key::Symbol)::Union{String, Nothing}`

Return the string value of `obj[key]` if it is a `String`, otherwise `nothing`.
"""
function _json_str(obj::JSON3.Object, key::Symbol)::Union{String,Nothing}
    haskey(obj, key) || return nothing
    val = obj[key]
    val isa String ? val : nothing
end

"""
    `_json_int(obj::JSON3.Object, key::Symbol)::Union{Int, Nothing}`

Return the integer value of `obj[key]` if it is an `Int64`, otherwise `nothing`.
"""
function _json_int(obj::JSON3.Object, key::Symbol)::Union{Int,Nothing}
    haskey(obj, key) || return nothing
    val = obj[key]
    val isa Int64 ? Int(val) : nothing
end

"""
    `_json_float(obj::JSON3.Object, key::Symbol)::Union{Float64, Nothing}`

Return the float value of `obj[key]` coerced from `Float64` or `Int64`,
otherwise `nothing`.
"""
function _json_float(obj::JSON3.Object, key::Symbol)::Union{Float64,Nothing}
    haskey(obj, key) || return nothing
    val = obj[key]
    val isa Float64 && return val
    val isa Int64 && return Float64(val)
    nothing
end

"""
    `_json_obj(raw::JSON3.Object, key::Symbol)::Union{JSON3.Object, Nothing}`

Return the nested object value of `raw[key]` if it is a `JSON3.Object`,
otherwise `nothing`.
"""
function _json_obj(raw::JSON3.Object, key::Symbol)::Union{JSON3.Object,Nothing}
    haskey(raw, key) || return nothing
    val = raw[key]
    val isa JSON3.Object ? val : nothing
end

"""
    `_json_bool(obj::JSON3.Object, key::Symbol)::Union{Bool, Nothing}`

Extract a boolean field from a JSON3 Object, returning `nothing` if the key
is absent or the value is not a `Bool`.
"""
function _json_bool(obj::JSON3.Object, key::Symbol)::Union{Bool,Nothing}
    haskey(obj, key) || return nothing
    v = obj[key]
    v isa Bool ? v : nothing
end

"""
    `_parse_json_set!(kwargs, obj, json_key, param_key, ::Type{T})`

Parse a JSON array at `obj[json_key]` into a `Set{T}` and store it in `kwargs[param_key]`.
Each element must be a `String`; non-string elements are skipped. Does nothing if the key
is absent, the value is not a `JSON3.Array`, or the resulting set is empty.
"""
function _parse_json_set!(
    kwargs::Dict{Symbol,Any}, obj, json_key::Symbol, param_key::Symbol, ::Type{T}
) where {T}
    haskey(obj, json_key) || return
    val = obj[json_key]
    val isa JSON3.Array || return
    s = Set{T}(T(String(v)) for v in val if v isa String)
    isempty(s) || (kwargs[param_key] = s)
end

"""
    `_parse_json_char_set!(kwargs, obj, json_key, param_key)`

Parse a JSON array at `obj[json_key]` into a `Set{Char}` and store it in `kwargs[param_key]`.
Each element must be a non-empty `String`; the first character is used. Does nothing if the
key is absent, the value is not a `JSON3.Array`, or the resulting set is empty.
"""
function _parse_json_char_set!(
    kwargs::Dict{Symbol,Any}, obj, json_key::Symbol, param_key::Symbol
)
    haskey(obj, json_key) || return
    val = obj[json_key]
    val isa JSON3.Array || return
    s = Set{Char}(first(String(v)) for v in val if v isa String && !isempty(String(v)))
    isempty(s) || (kwargs[param_key] = s)
end

"""
    `_parse_constraints_params(cstr::JSON3.Object)::Union{ParameterSet, Nothing}`
---

# Description
- Parse a JSON constraints object into a `ParameterSet`
- Returns `nothing` if no recognized fields are present
- Silently skips absent or wrongly-typed fields

# Arguments
1. `cstr::JSON3.Object`: the parsed `"constraints"` JSON section

# Returns
- `::Union{ParameterSet, Nothing}`: populated `ParameterSet`, or `nothing` if empty
"""
function _parse_constraints_params(cstr::JSON3.Object)::Union{ParameterSet,Nothing}
    ck = Dict{Symbol,Any}()

    # Minutes fields (Int16)
    for (jk, pk) in [
        (:min_connection_time, :min_connection_time),
        (:max_connection_time, :max_connection_time),
    ]
        v = _json_int(cstr, jk)
        v !== nothing && (ck[pk] = Minutes(v))
    end

    # Stop count fields (Int16)
    for (jk, pk) in [(:min_stops, :min_stops), (:max_stops, :max_stops)]
        v = _json_int(cstr, jk)
        v !== nothing && (ck[pk] = Int16(v))
    end

    # Int32 time fields
    for (jk, pk) in [
        (:min_elapsed, :min_elapsed),
        (:max_elapsed, :max_elapsed),
        (:min_flight_time, :min_flight_time),
        (:max_flight_time, :max_flight_time),
        (:min_layover_time, :min_layover_time),
        (:max_layover_time, :max_layover_time),
    ]
        v = _json_int(cstr, jk)
        v !== nothing && (ck[pk] = Int32(v))
    end

    # Distance fields (Float32) — accept float or integer JSON values
    for (jk, pk) in [
        (:min_total_distance, :min_total_distance),
        (:max_total_distance, :max_total_distance),
        (:min_leg_distance, :min_leg_distance),
        (:max_leg_distance, :max_leg_distance),
    ]
        f = _json_float(cstr, jk)
        if f !== nothing
            ck[pk] = Distance(f)
        else
            v = _json_int(cstr, jk)
            v !== nothing && (ck[pk] = Distance(Float64(v)))
        end
    end

    # Float64 fields
    for (jk, pk) in [
        (:min_circuity, :min_circuity),
        (:max_circuity, :max_circuity),
        (:circuity_factor, :circuity_factor),
        (:domestic_circuity_extra_miles, :domestic_circuity_extra_miles),
        (:international_circuity_extra_miles, :international_circuity_extra_miles),
    ]
        f = _json_float(cstr, jk)
        f !== nothing && (ck[pk] = f)
    end

    # Carrier sets (AirlineCode = InlineString3)
    _parse_json_set!(ck, cstr, :deny_carriers, :deny_carriers, AirlineCode)
    _parse_json_set!(ck, cstr, :allow_carriers, :allow_carriers, AirlineCode)
    _parse_json_set!(ck, cstr, :deny_operating_carriers, :deny_operating_carriers, AirlineCode)
    _parse_json_set!(ck, cstr, :allow_operating_carriers, :allow_operating_carriers, AirlineCode)

    # Geographic sets (InlineString3)
    _parse_json_set!(ck, cstr, :deny_countries, :deny_countries, InlineString3)
    _parse_json_set!(ck, cstr, :allow_countries, :allow_countries, InlineString3)
    _parse_json_set!(ck, cstr, :deny_regions, :deny_regions, InlineString3)
    _parse_json_set!(ck, cstr, :allow_regions, :allow_regions, InlineString3)
    _parse_json_set!(ck, cstr, :deny_states, :deny_states, InlineString3)
    _parse_json_set!(ck, cstr, :allow_states, :allow_states, InlineString3)

    # Station sets (StationCode = InlineString3)
    _parse_json_set!(ck, cstr, :deny_stations, :deny_stations, StationCode)
    _parse_json_set!(ck, cstr, :allow_stations, :allow_stations, StationCode)

    # Aircraft type sets (InlineString7)
    _parse_json_set!(ck, cstr, :deny_aircraft_types, :deny_aircraft_types, InlineString7)
    _parse_json_set!(ck, cstr, :allow_aircraft_types, :allow_aircraft_types, InlineString7)

    # Char sets (service type, body type)
    _parse_json_char_set!(ck, cstr, :deny_service_types, :deny_service_types)
    _parse_json_char_set!(ck, cstr, :allow_service_types, :allow_service_types)
    _parse_json_char_set!(ck, cstr, :deny_body_types, :deny_body_types)
    _parse_json_char_set!(ck, cstr, :allow_body_types, :allow_body_types)

    isempty(ck) && return nothing
    return ParameterSet(; ck...)
end

"""
    `load_constraints(path::String)::SearchConstraints`
---

# Description
- Load a `SearchConstraints` from the `"constraints"` section of a JSON config file
- The same JSON file used by `load_config` may also contain a `"constraints"` section
- Any missing or wrongly-typed fields use `ParameterSet` defaults
- Returns a default `SearchConstraints()` when the file has no `"constraints"` key

# Arguments
1. `path::String`: path to a JSON config file

# Returns
- `::SearchConstraints`: constraints with defaults populated from JSON

# Examples
```julia
julia> sc = load_constraints("config/defaults.json");
julia> sc.defaults.max_stops
Int16(2)
```
"""
function load_constraints(path::String)::SearchConstraints
    raw = JSON3.read(read(path, String))
    raw isa JSON3.Object || return SearchConstraints()
    cstr = _json_obj(raw, :constraints)
    cstr === nothing && return SearchConstraints()
    ps = _parse_constraints_params(cstr)
    ps === nothing && return SearchConstraints()
    return SearchConstraints(defaults=ps)
end

"""
    `load_config(path::String)::SearchConfig`
---

# Description
- Load a SearchConfig from a JSON file
- Any missing field uses the struct default
- Unknown or wrongly-typed JSON values are silently skipped (logged in future)

# Arguments
1. `path::String`: path to a JSON config file

# Returns
- `::SearchConfig`: fully populated config (missing keys use defaults)

# Examples
```julia
julia> cfg = load_config("config/defaults.json");
```
"""
function load_config(path::String)::SearchConfig
    raw = JSON3.read(read(path, String))
    raw isa JSON3.Object || return SearchConfig()

    kwargs = Dict{Symbol,Any}()

    store = _json_obj(raw, :store)
    if store !== nothing
        s = _json_str(store, :backend)
        s !== nothing && (kwargs[:backend] = s)
        s = _json_str(store, :path)
        s !== nothing && (kwargs[:db_path] = s)
    end

    search = _json_obj(raw, :search)
    if search !== nothing
        v = _json_int(search, :max_stops)
        v !== nothing && (kwargs[:max_stops] = v)
        v = _json_int(search, :max_connection_minutes)
        v !== nothing && (kwargs[:max_connection_minutes] = v)
        v = _json_int(search, :max_elapsed_minutes)
        v !== nothing && (kwargs[:max_elapsed_minutes] = v)
        f = _json_float(search, :circuity_factor)
        f !== nothing && (kwargs[:circuity_factor] = f)
        f = _json_float(search, :circuity_extra_miles)
        f !== nothing && (kwargs[:circuity_extra_miles] = f)
        s = _json_str(search, :scope)
        s !== nothing && (kwargs[:scope] = _parse_scope(s))
        s = _json_str(search, :interline)
        s !== nothing && (kwargs[:interline] = _parse_interline(s))
        if haskey(search, :allow_roundtrips)
            val = search[:allow_roundtrips]
            val isa Bool && (kwargs[:allow_roundtrips] = val)
        end
        b = _json_bool(search, :mct_cache_enabled)
        b !== nothing && (kwargs[:mct_cache_enabled] = b)
    end

    data = _json_obj(raw, :data)
    if data !== nothing
        s = _json_str(data, :ssim)
        s !== nothing && (kwargs[:ssim_path] = s)
        s = _json_str(data, :mct)
        s !== nothing && (kwargs[:mct_path] = s)
        s = _json_str(data, :airports)
        s !== nothing && (kwargs[:airports_path] = s)
        s = _json_str(data, :regions)
        s !== nothing && (kwargs[:regions_path] = s)
        s = _json_str(data, :aircrafts)
        s !== nothing && (kwargs[:aircrafts_path] = s)
        s = _json_str(data, :seats)
        s !== nothing && (kwargs[:seats_path] = s)
        s = _json_str(data, :classmap)
        s !== nothing && (kwargs[:classmap_path] = s)
        s = _json_str(data, :serviceclass)
        s !== nothing && (kwargs[:serviceclass_path] = s)
        s = _json_str(data, :oa_control)
        s !== nothing && (kwargs[:oa_control_path] = s)
    end

    sched = _json_obj(raw, :schedule)
    if sched !== nothing
        v = _json_int(sched, :max_days)
        v !== nothing && (kwargs[:max_days] = v)
        v = _json_int(sched, :trailing_days)
        v !== nothing && (kwargs[:trailing_days] = v)
        v = _json_int(sched, :leading_days)
        v !== nothing && (kwargs[:leading_days] = v)
    end

    if data !== nothing
        s = _json_str(data, :constraints)
        s !== nothing && (kwargs[:constraints_path] = s)
    end

    graph = _json_obj(raw, :graph)
    if graph !== nothing
        s = _json_str(graph, :export_path)
        s !== nothing && (kwargs[:graph_export_path] = s)
        s = _json_str(graph, :import_path)
        s !== nothing && (kwargs[:graph_import_path] = s)
    end

    output = _json_obj(raw, :output)
    if output !== nothing
        s = _json_str(output, :metrics_level)
        if s !== nothing
            sym = Symbol(lowercase(s))
            sym in (:basic, :aircraft, :full) && (kwargs[:metrics_level] = sym)
        end
        s = _json_str(output, :event_log_path)
        s !== nothing && (kwargs[:event_log_path] = s)
        b = _json_bool(output, :event_log_enabled)
        b !== nothing && (kwargs[:event_log_enabled] = b)
        s = _json_str(output, :log_level)
        if s !== nothing
            sym = Symbol(lowercase(s))
            sym in (:debug, :info, :warn, :error) && (kwargs[:log_level] = sym)
        end
        s = _json_str(output, :log_json_path)
        s !== nothing && (kwargs[:log_json_path] = s)
        b = _json_bool(output, :log_stdout_json)
        b !== nothing && (kwargs[:log_stdout_json] = b)
        if haskey(output, :output_formats)
            fmts_val = output[:output_formats]
            if fmts_val isa JSON3.Array
                syms = Symbol[Symbol(lowercase(String(f))) for f in fmts_val if f isa String]
                isempty(syms) || (kwargs[:output_formats] = syms)
            end
        end
    end

    audit = _json_obj(raw, :mct_audit)
    if audit !== nothing
        audit_kwargs = Dict{Symbol,Any}()
        b = _json_bool(audit, :enabled)
        b !== nothing && (audit_kwargs[:enabled] = b)
        s = _json_str(audit, :detail)
        if s !== nothing
            sym = Symbol(lowercase(s))
            sym in (:summary, :detailed) && (audit_kwargs[:detail] = sym)
        end
        s = _json_str(audit, :output_path)
        s !== nothing && (audit_kwargs[:output_path] = s)
        v = _json_int(audit, :max_connections)
        v !== nothing && (audit_kwargs[:max_connections] = v)
        v = _json_int(audit, :max_candidates)
        v !== nothing && (audit_kwargs[:max_candidates] = v)
        isempty(audit_kwargs) || (kwargs[:mct_audit] = MCTAuditConfig(; audit_kwargs...))
    end

    SearchConfig(; kwargs...)
end

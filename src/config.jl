# src/config.jl — SearchConfig and JSON loading

using JSON3

"""
    _default_path(filename::String)::String

Resolve a default data path relative to the package's `data/demo/` directory.
"""
function _default_path(filename::String)::String
    joinpath(pkgdir(@__MODULE__), "data", "demo", filename)
end

"""
    _parse_scope(s::AbstractString)::ScopeMode

Parse a scope string from JSON config to ScopeMode enum.
"""
function _parse_scope(s::AbstractString)::ScopeMode
    s = lowercase(String(s))
    s == "all"  && return SCOPE_ALL
    s == "dom"  && return SCOPE_DOM
    s == "intl" && return SCOPE_INTL
    error("Unknown scope mode: $s. Expected: all, dom, intl")
end

"""
    _parse_interline(s::AbstractString)::InterlineMode

Parse an interline string from JSON config to InterlineMode enum.
"""
function _parse_interline(s::AbstractString)::InterlineMode
    s = lowercase(String(s))
    s == "online"    && return INTERLINE_ONLINE
    s == "codeshare" && return INTERLINE_CODESHARE
    s == "all"       && return INTERLINE_ALL
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
    ssim_path::String = _default_path("ssim_demo.dat.zst")
    mct_path::String = _default_path("mct_demo.dat.zst")
    airports_path::String = _default_path("airports.txt")
    regions_path::String = _default_path("regions.dat")
    aircrafts_path::String = _default_path("aircrafts.txt")
    seats_path::String = _default_path("seats.dat")
    classmap_path::String = _default_path("classmap.txt")
    serviceclass_path::String = _default_path("serviceclass.dat")
    oa_control_path::String = _default_path("oa_control.csv")
end

"""
    `load_config(path::String)::SearchConfig`
---

# Description
- Load a SearchConfig from a JSON file
- Any missing field uses the struct default

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
    kwargs = Dict{Symbol, Any}()

    if haskey(raw, :store)
        store = raw[:store]
        haskey(store, :backend) && (kwargs[:backend] = String(store[:backend]))
        haskey(store, :path)    && (kwargs[:db_path]  = String(store[:path]))
    end

    if haskey(raw, :search)
        s = raw[:search]
        haskey(s, :max_stops)               && (kwargs[:max_stops]               = Int(s[:max_stops]))
        haskey(s, :max_connection_minutes)  && (kwargs[:max_connection_minutes]  = Int(s[:max_connection_minutes]))
        haskey(s, :max_elapsed_minutes)     && (kwargs[:max_elapsed_minutes]     = Int(s[:max_elapsed_minutes]))
        haskey(s, :circuity_factor)         && (kwargs[:circuity_factor]         = Float64(s[:circuity_factor]))
        haskey(s, :circuity_extra_miles)    && (kwargs[:circuity_extra_miles]    = Float64(s[:circuity_extra_miles]))
        haskey(s, :scope)                   && (kwargs[:scope]                   = _parse_scope(String(s[:scope])))
        haskey(s, :interline)               && (kwargs[:interline]               = _parse_interline(String(s[:interline])))
    end

    if haskey(raw, :data)
        d = raw[:data]
        haskey(d, :ssim)         && (kwargs[:ssim_path]         = String(d[:ssim]))
        haskey(d, :mct)          && (kwargs[:mct_path]          = String(d[:mct]))
        haskey(d, :airports)     && (kwargs[:airports_path]     = String(d[:airports]))
        haskey(d, :regions)      && (kwargs[:regions_path]      = String(d[:regions]))
        haskey(d, :aircrafts)    && (kwargs[:aircrafts_path]    = String(d[:aircrafts]))
        haskey(d, :seats)        && (kwargs[:seats_path]        = String(d[:seats]))
        haskey(d, :classmap)     && (kwargs[:classmap_path]     = String(d[:classmap]))
        haskey(d, :serviceclass) && (kwargs[:serviceclass_path] = String(d[:serviceclass]))
        haskey(d, :oa_control)   && (kwargs[:oa_control_path]   = String(d[:oa_control]))
    end

    if haskey(raw, :schedule)
        sched = raw[:schedule]
        haskey(sched, :max_days)      && (kwargs[:max_days]      = Int(sched[:max_days]))
        haskey(sched, :trailing_days) && (kwargs[:trailing_days] = Int(sched[:trailing_days]))
    end

    SearchConfig(; kwargs...)
end

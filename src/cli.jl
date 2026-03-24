# src/cli.jl — Command-line interface for ItinerarySearch
#
# Provides five commands:
#   search   — one-way itinerary search (one or more O-D pairs / dates)
#   trip     — multi-leg trip search with pairing and scoring
#   build    — materialise the flight graph and report build stats
#   ingest   — load schedule data into DuckDB and report table stats
#   info     — report store and config state without ingesting
#
# Entry point: ItinerarySearch.CLI.main(ARGS)

module CLI

using ArgParse
using Dates
using JSON3
using Logging

# Import all required names from the parent module
import ..SearchConfig, ..load_config, ..SearchConstraints, ..ParameterSet
import ..DuckDBStore, ..load_schedule!, ..table_stats, ..close
import ..build_graph!, ..RuntimeContext, ..build_itn_rules
import ..itinerary_legs_json, ..search_trip, ..TripLeg
import ..StationCode, ..Minutes
import ..SCOPE_ALL, ..SCOPE_DOM, ..SCOPE_INTL
import ..INTERLINE_ONLINE, ..INTERLINE_CODESHARE, ..INTERLINE_ALL
import .._parse_scope, .._parse_interline

# ── Parser construction ────────────────────────────────────────────────────────

"""
    `function _build_parser()::ArgParseSettings`
---

# Description
- Construct the top-level `ArgParseSettings` with all commands and flags
- Commands are registered with `action = :command` before sub-arg tables are
  populated so ArgParse can route dispatch correctly
- Global flags (config, logging, output, parameter overrides) are defined on
  the root settings object and are visible to every command

# Returns
- `::ArgParseSettings`: fully configured parser
"""
function _build_parser()::ArgParseSettings
    s = ArgParseSettings(;
        prog = "itinerary-search",
        description = "ItinerarySearch.jl — flight itinerary search and trip planning",
        commands_are_required = true,
        add_help = true,
    )

    # ── Global flags ──────────────────────────────────────────────────────────
    @add_arg_table! s begin
        "--config"
        help = "Path to JSON config file"
        arg_type = String
        default = nothing

        "--log-level"
        help = "Log level: debug, info, warn, error"
        arg_type = String
        default = nothing

        "--log-json"
        help = "Write structured JSON logs to this path"
        arg_type = String
        default = nothing

        "--quiet"
        help = "Suppress all log output"
        action = :store_true

        "--compact"
        help = "Compact JSON output (summary fields only)"
        action = :store_true

        "--output"
        help = "Write output to file instead of stdout"
        arg_type = String
        default = nothing

        # ── Parameter overrides ───────────────────────────────────────────────
        "--leading-days"
        help = "Schedule window leading days (override config)"
        arg_type = Int
        default = nothing

        "--trailing-days"
        help = "Schedule window trailing days (override config)"
        arg_type = Int
        default = nothing

        "--max-stops"
        help = "Maximum connection stops per itinerary"
        arg_type = Int
        default = nothing

        "--max-elapsed"
        help = "Maximum elapsed travel time in minutes"
        arg_type = Int
        default = nothing

        "--max-connection"
        help = "Maximum connection time in minutes"
        arg_type = Int
        default = nothing

        "--circuity-factor"
        help = "Maximum circuity ratio (flown / market distance)"
        arg_type = Float64
        default = nothing

        "--scope"
        help = "Scope filter: all, dom, intl"
        arg_type = String
        default = nothing

        "--interline"
        help = "Interline mode: online, codeshare, all"
        arg_type = String
        default = nothing

        "--allow-roundtrips"
        help = "Allow round-trip itineraries"
        action = :store_true

        "--no-mct-cache"
        help = "Disable MCT lookup cache"
        action = :store_true

        # ── Command dispatch ──────────────────────────────────────────────────
        "search"
        action = :command
        help = "Search itineraries for one or more O-D pairs"

        "trip"
        action = :command
        help = "Search multi-leg trip with pairing and scoring"

        "build"
        action = :command
        help = "Build the flight graph and report build stats"

        "ingest"
        action = :command
        help = "Ingest schedule data and report table stats"

        "info"
        action = :command
        help = "Report store state and config without ingesting"
    end

    # ── search sub-args ───────────────────────────────────────────────────────
    @add_arg_table! s["search"] begin
        "origin"
        help = "Origin airport code(s), comma-separated (e.g. ORD,MDW)"
        arg_type = String
        required = true

        "dest"
        help = "Destination airport code(s), comma-separated (e.g. LHR,LGW)"
        arg_type = String
        required = true

        "dates"
        help = "Travel date(s) in YYYY-MM-DD format"
        nargs = '+'
        arg_type = String
        required = true

        "--cross"
        help = "Search all origin×destination combinations"
        action = :store_true
    end

    # ── trip sub-args ─────────────────────────────────────────────────────────
    @add_arg_table! s["trip"] begin
        "legs"
        help = "Leg triples: origin dest date [origin dest date ...] (must be divisible by 3)"
        nargs = '+'
        arg_type = String
        required = true

        "--min-stay"
        help = "Minimum stay between legs in minutes (applied to leg 2+)"
        arg_type = Int
        default = 0

        "--max-trips"
        help = "Maximum number of trips to return"
        arg_type = Int
        default = 1000

        "--max-per-leg"
        help = "Maximum itineraries per leg before pairing"
        arg_type = Int
        default = 100
    end

    # ── build sub-args ────────────────────────────────────────────────────────
    @add_arg_table! s["build"] begin
        "--date"
        help = "Target date in YYYY-MM-DD format (required)"
        arg_type = String
        required = true
    end

    # ingest and info have no extra args; sub-tables already default to empty

    return s
end

# ── Config helpers ─────────────────────────────────────────────────────────────

"""
    `function _load_config(args::Dict)::SearchConfig`
---

# Description
- Load config from `--config` path when provided, otherwise return `SearchConfig()`

# Arguments
1. `args::Dict`: top-level parsed args dict

# Returns
- `::SearchConfig`
"""
function _load_config(args::Dict)::SearchConfig
    path = args["config"]
    path === nothing && return SearchConfig()
    isfile(path) || error("Config file not found: $path")
    return load_config(path)
end

"""
    `function _apply_overrides(config::SearchConfig, args::Dict)::SearchConfig`
---

# Description
- Merge CLI flag overrides into a new `SearchConfig`, leaving all unset flags
  at their existing values from `config`
- Scope and interline are parsed from string values; boolean flags (allow-roundtrips,
  no-mct-cache) are additive (can only turn on, not off from CLI)

# Arguments
1. `config::SearchConfig`: base configuration
2. `args::Dict`: top-level parsed args dict

# Returns
- `::SearchConfig`: new config with CLI overrides applied
"""
function _apply_overrides(config::SearchConfig, args::Dict)::SearchConfig
    kwargs = Dict{Symbol,Any}(
        :backend              => config.backend,
        :db_path              => config.db_path,
        :max_stops            => config.max_stops,
        :max_connection_minutes => config.max_connection_minutes,
        :max_elapsed_minutes  => config.max_elapsed_minutes,
        :circuity_factor      => config.circuity_factor,
        :circuity_extra_miles => config.circuity_extra_miles,
        :scope                => config.scope,
        :interline            => config.interline,
        :max_days             => config.max_days,
        :trailing_days        => config.trailing_days,
        :ssim_path            => config.ssim_path,
        :mct_path             => config.mct_path,
        :airports_path        => config.airports_path,
        :regions_path         => config.regions_path,
        :aircrafts_path       => config.aircrafts_path,
        :seats_path           => config.seats_path,
        :classmap_path        => config.classmap_path,
        :serviceclass_path    => config.serviceclass_path,
        :oa_control_path      => config.oa_control_path,
        :leading_days         => config.leading_days,
        :metrics_level        => config.metrics_level,
        :graph_export_path    => config.graph_export_path,
        :graph_import_path    => config.graph_import_path,
        :constraints_path     => config.constraints_path,
        :event_log_enabled    => config.event_log_enabled,
        :event_log_path       => config.event_log_path,
        :log_level            => config.log_level,
        :log_json_path        => config.log_json_path,
        :log_stdout_json      => config.log_stdout_json,
        :output_formats       => config.output_formats,
        :distance_formula     => config.distance_formula,
        :allow_roundtrips     => config.allow_roundtrips,
        :mct_cache_enabled    => config.mct_cache_enabled,
    )

    v = args["leading-days"]
    v !== nothing && (kwargs[:leading_days] = v)

    v = args["trailing-days"]
    v !== nothing && (kwargs[:trailing_days] = v)

    v = args["circuity-factor"]
    v !== nothing && (kwargs[:circuity_factor] = v)

    v = args["scope"]
    if v !== nothing
        kwargs[:scope] = _parse_scope(v)
    end

    v = args["interline"]
    if v !== nothing
        kwargs[:interline] = _parse_interline(v)
    end

    if args["allow-roundtrips"]
        kwargs[:allow_roundtrips] = true
    end

    if args["no-mct-cache"]
        kwargs[:mct_cache_enabled] = false
    end

    v = args["log-level"]
    if v !== nothing
        sym = Symbol(lowercase(v))
        sym in (:debug, :info, :warn, :error) && (kwargs[:log_level] = sym)
    end

    v = args["log-json"]
    v !== nothing && (kwargs[:log_json_path] = v)

    return SearchConfig(; kwargs...)
end

"""
    `function _apply_constraint_overrides(constraints::SearchConstraints, args::Dict)::SearchConstraints`
---

# Description
- Merge CLI parameter overrides into a new `SearchConstraints` by rebuilding
  the `defaults::ParameterSet` with the overridden values

# Arguments
1. `constraints::SearchConstraints`: base constraints
2. `args::Dict`: top-level parsed args dict

# Returns
- `::SearchConstraints`: new constraints with CLI overrides applied
"""
function _apply_constraint_overrides(
    constraints::SearchConstraints, args::Dict
)::SearchConstraints
    p = constraints.defaults

    max_stops = args["max-stops"]
    max_elapsed = args["max-elapsed"]
    max_connection = args["max-connection"]

    # Only rebuild if at least one override is present
    if max_stops === nothing && max_elapsed === nothing && max_connection === nothing
        return constraints
    end

    new_defaults = ParameterSet(
        min_mct_override          = p.min_mct_override,
        max_mct_override          = max_connection !== nothing ? Minutes(Int16(max_connection)) : p.max_mct_override,
        circuity_factor           = p.circuity_factor,
        circuity_extra_miles      = p.circuity_extra_miles,
        valid_codeshare_partners  = p.valid_codeshare_partners,
        valid_jv_groups           = p.valid_jv_groups,
        valid_wet_leases          = p.valid_wet_leases,
        min_leg_distance          = p.min_leg_distance,
        max_leg_distance          = p.max_leg_distance,
        min_stops                 = p.min_stops,
        max_stops                 = max_stops !== nothing ? Int16(max_stops) : p.max_stops,
        min_elapsed               = p.min_elapsed,
        max_elapsed               = max_elapsed !== nothing ? Int32(max_elapsed) : p.max_elapsed,
        min_total_distance        = p.min_total_distance,
        max_total_distance        = p.max_total_distance,
        itinerary_circuity        = p.itinerary_circuity,
        max_results               = p.max_results,
    )

    return SearchConstraints(
        defaults         = new_defaults,
        overrides        = constraints.overrides,
        closed_stations  = constraints.closed_stations,
        closed_markets   = constraints.closed_markets,
        delays           = constraints.delays,
        flight_delays    = constraints.flight_delays,
    )
end

# ── Output helper ──────────────────────────────────────────────────────────────

"""
    `function _write_output(data::AbstractString, args::Dict)::Nothing`
---

# Description
- Write `data` to the file at `--output` if given, otherwise print to stdout

# Arguments
1. `data::AbstractString`: text content to write
2. `args::Dict`: top-level parsed args dict
"""
function _write_output(data::AbstractString, args::Dict)::Nothing
    path = args["output"]
    if path !== nothing
        write(path, data)
        write(path, "\n")
    else
        println(data)
    end
    return nothing
end

# ── Command handlers ───────────────────────────────────────────────────────────

"""
    `function _cmd_search(config::SearchConfig, constraints::SearchConstraints, args::Dict)::Int`
---

# Description
- Execute the `search` command: load schedule, build graph, search itineraries,
  write JSON output
- `args` is the parsed sub-command dict from `parsed["search"]`
- Derives the build target date as the minimum of all requested travel dates;
  extends `trailing_days` if the date span exceeds the configured window

# Arguments
1. `config::SearchConfig`: effective search configuration
2. `constraints::SearchConstraints`: effective search constraints
3. `args::Dict`: `search` sub-command args

# Returns
- `::Int`: 0 on success
"""
function _cmd_search(
    config::SearchConfig, constraints::SearchConstraints, args::Dict, global_args::Dict
)::Int
    # Parse origins and destinations
    origins = [StationCode(strip(s)) for s in split(args["origin"], ',') if !isempty(strip(s))]
    dests   = [StationCode(strip(s)) for s in split(args["dest"],   ',') if !isempty(strip(s))]
    dates   = [Date(d) for d in args["dates"]]
    cross   = args["cross"]

    isempty(origins) && error("No valid origin codes")
    isempty(dests)   && error("No valid destination codes")
    isempty(dates)   && error("No valid dates")

    # Derive target date and extend trailing_days to cover the full date range
    target = minimum(dates)
    max_date = maximum(dates)
    span_days = Dates.value(max_date - target)
    effective_trailing = max(config.trailing_days, span_days)
    config = SearchConfig(;
        backend              = config.backend,
        db_path              = config.db_path,
        max_stops            = config.max_stops,
        max_connection_minutes = config.max_connection_minutes,
        max_elapsed_minutes  = config.max_elapsed_minutes,
        circuity_factor      = config.circuity_factor,
        circuity_extra_miles = config.circuity_extra_miles,
        scope                = config.scope,
        interline            = config.interline,
        max_days             = config.max_days,
        trailing_days        = effective_trailing,
        ssim_path            = config.ssim_path,
        mct_path             = config.mct_path,
        airports_path        = config.airports_path,
        regions_path         = config.regions_path,
        aircrafts_path       = config.aircrafts_path,
        seats_path           = config.seats_path,
        classmap_path        = config.classmap_path,
        serviceclass_path    = config.serviceclass_path,
        oa_control_path      = config.oa_control_path,
        leading_days         = config.leading_days,
        metrics_level        = config.metrics_level,
        graph_export_path    = config.graph_export_path,
        graph_import_path    = config.graph_import_path,
        constraints_path     = config.constraints_path,
        event_log_enabled    = config.event_log_enabled,
        event_log_path       = config.event_log_path,
        log_level            = config.log_level,
        log_json_path        = config.log_json_path,
        log_stdout_json      = config.log_stdout_json,
        output_formats       = config.output_formats,
        distance_formula     = config.distance_formula,
        allow_roundtrips     = config.allow_roundtrips,
        mct_cache_enabled    = config.mct_cache_enabled,
    )

    store = DuckDBStore()
    try
        load_schedule!(store, config)
        graph = build_graph!(store, config, target)

        itn_rules = build_itn_rules(config)
        ctx = RuntimeContext(
            config      = config,
            constraints = constraints,
            itn_rules   = itn_rules,
        )

        compact = global_args["compact"]
        json = itinerary_legs_json(
            graph.stations, ctx;
            origins      = origins,
            destinations = dests,
            dates        = dates,
            cross        = cross,
            compact      = compact,
        )

        _write_output(json, global_args)
    finally
        close(store)
    end

    return 0
end

"""
    `function _cmd_trip(config::SearchConfig, constraints::SearchConstraints, args::Dict, global_args::Dict)::Int`
---

# Description
- Execute the `trip` command: parse leg triples, build graph, search trips,
  write JSON output
- Positional `legs` argument must be a multiple-of-3 sequence: origin dest date ...
- `min_stay` is applied to legs 2 and beyond

# Arguments
1. `config::SearchConfig`: effective search configuration
2. `constraints::SearchConstraints`: effective search constraints
3. `args::Dict`: `trip` sub-command args
4. `global_args::Dict`: top-level args for output flags

# Returns
- `::Int`: 0 on success
"""
function _cmd_trip(
    config::SearchConfig, constraints::SearchConstraints, args::Dict, global_args::Dict
)::Int
    raw_legs = args["legs"]
    length(raw_legs) % 3 == 0 || error(
        "trip legs must be triplets (origin dest date); got $(length(raw_legs)) tokens"
    )

    min_stay    = args["min-stay"]
    max_trips   = args["max-trips"]
    max_per_leg = args["max-per-leg"]

    trip_legs = TripLeg[]
    for i in 1:3:length(raw_legs)
        origin = StationCode(strip(raw_legs[i]))
        dest   = StationCode(strip(raw_legs[i+1]))
        date   = Date(strip(raw_legs[i+2]))
        stay   = (i == 1) ? 0 : min_stay
        push!(trip_legs, TripLeg(origin=origin, destination=dest, date=date, min_stay=stay))
    end

    isempty(trip_legs) && error("No trip legs provided")

    leg_dates = [l.date for l in trip_legs]
    target = minimum(leg_dates)
    max_date = maximum(leg_dates)
    span_days = Dates.value(max_date - target)
    effective_trailing = max(config.trailing_days, span_days)
    config = SearchConfig(;
        backend              = config.backend,
        db_path              = config.db_path,
        max_stops            = config.max_stops,
        max_connection_minutes = config.max_connection_minutes,
        max_elapsed_minutes  = config.max_elapsed_minutes,
        circuity_factor      = config.circuity_factor,
        circuity_extra_miles = config.circuity_extra_miles,
        scope                = config.scope,
        interline            = config.interline,
        max_days             = config.max_days,
        trailing_days        = effective_trailing,
        ssim_path            = config.ssim_path,
        mct_path             = config.mct_path,
        airports_path        = config.airports_path,
        regions_path         = config.regions_path,
        aircrafts_path       = config.aircrafts_path,
        seats_path           = config.seats_path,
        classmap_path        = config.classmap_path,
        serviceclass_path    = config.serviceclass_path,
        oa_control_path      = config.oa_control_path,
        leading_days         = config.leading_days,
        metrics_level        = config.metrics_level,
        graph_export_path    = config.graph_export_path,
        graph_import_path    = config.graph_import_path,
        constraints_path     = config.constraints_path,
        event_log_enabled    = config.event_log_enabled,
        event_log_path       = config.event_log_path,
        log_level            = config.log_level,
        log_json_path        = config.log_json_path,
        log_stdout_json      = config.log_stdout_json,
        output_formats       = config.output_formats,
        distance_formula     = config.distance_formula,
        allow_roundtrips     = config.allow_roundtrips,
        mct_cache_enabled    = config.mct_cache_enabled,
    )

    store = DuckDBStore()
    try
        load_schedule!(store, config)
        graph = build_graph!(store, config, target)

        itn_rules = build_itn_rules(config)
        ctx = RuntimeContext(
            config      = config,
            constraints = constraints,
            itn_rules   = itn_rules,
        )

        trips = search_trip(
            store, graph, trip_legs, ctx;
            max_trips   = max_trips,
            max_per_leg = max_per_leg,
        )

        json = JSON3.write(trips)
        _write_output(json, global_args)
    finally
        close(store)
    end

    return 0
end

"""
    `function _cmd_build(config::SearchConfig, args::Dict, global_args::Dict)::Int`
---

# Description
- Execute the `build` command: load schedule, build flight graph, print stats
  summary to stderr, write build stats JSON to stdout

# Arguments
1. `config::SearchConfig`: effective search configuration
2. `args::Dict`: `build` sub-command args (contains `--date`)
3. `global_args::Dict`: top-level args for output flags

# Returns
- `::Int`: 0 on success
"""
function _cmd_build(config::SearchConfig, args::Dict, global_args::Dict)::Int
    target = Date(args["date"])

    store = DuckDBStore()
    try
        load_schedule!(store, config)
        graph = build_graph!(store, config, target)

        bs = graph.build_stats
        # Print human-readable summary to stderr
        printstyled(
            stderr,
            "Build complete: $(bs.total_stations) stations, $(bs.total_legs) legs, " *
            "$(bs.total_segments) segments, $(bs.total_connections) connections\n";
            color = :green,
        )

        json = JSON3.write((
            build_id        = string(graph.build_id),
            window_start    = string(graph.window_start),
            window_end      = string(graph.window_end),
            total_stations  = bs.total_stations,
            total_legs      = bs.total_legs,
            total_segments  = bs.total_segments,
            total_connections = bs.total_connections,
            build_time_ms   = round(bs.build_time_ns / 1.0e6; digits=1),
        ))
        _write_output(json, global_args)
    finally
        close(store)
    end

    return 0
end

"""
    `function _cmd_ingest(config::SearchConfig, args::Dict, global_args::Dict)::Int`
---

# Description
- Execute the `ingest` command: load all schedule data into the DuckDB store
  and write table row-count stats as JSON

# Arguments
1. `config::SearchConfig`: effective search configuration
2. `args::Dict`: `ingest` sub-command args (no extra flags)
3. `global_args::Dict`: top-level args for output flags

# Returns
- `::Int`: 0 on success
"""
function _cmd_ingest(config::SearchConfig, _args::Dict, global_args::Dict)::Int
    store = DuckDBStore()
    try
        load_schedule!(store, config)
        stats = table_stats(store)
        _write_output(JSON3.write(stats), global_args)
    finally
        close(store)
    end

    return 0
end

"""
    `function _cmd_info(config::SearchConfig, args::Dict, global_args::Dict)::Int`
---

# Description
- Execute the `info` command: open the store without ingesting, report table
  stats and a config summary as JSON

# Arguments
1. `config::SearchConfig`: effective search configuration
2. `args::Dict`: `info` sub-command args (no extra flags)
3. `global_args::Dict`: top-level args for output flags

# Returns
- `::Int`: 0 on success
"""
function _cmd_info(config::SearchConfig, _args::Dict, global_args::Dict)::Int
    store = DuckDBStore()
    try
        stats = table_stats(store)
        info = (
            table_stats = stats,
            config = (
                backend            = config.backend,
                db_path            = config.db_path,
                max_stops          = config.max_stops,
                max_connection_minutes = config.max_connection_minutes,
                max_elapsed_minutes = config.max_elapsed_minutes,
                circuity_factor    = config.circuity_factor,
                scope              = string(config.scope),
                interline          = string(config.interline),
                leading_days       = config.leading_days,
                trailing_days      = config.trailing_days,
                allow_roundtrips   = config.allow_roundtrips,
                mct_cache_enabled  = config.mct_cache_enabled,
                ssim_path          = config.ssim_path,
                mct_path           = config.mct_path,
            ),
        )
        _write_output(JSON3.write(info), global_args)
    finally
        close(store)
    end

    return 0
end

# ── main ──────────────────────────────────────────────────────────────────────

"""
    `function main(args::Vector{String})::Int`
---

# Description
- Entry point for the ItinerarySearch CLI
- Parses arguments, applies global flags, loads config, dispatches to the
  appropriate command handler
- Returns an integer exit code: 0 = success, 1 = runtime error, 2 = usage error
- Catch-all wraps unexpected exceptions and prints them to stderr before
  returning exit code 1

# Arguments
1. `args::Vector{String}`: command-line argument vector (typically `ARGS`)

# Returns
- `::Int`: process exit code

# Examples
```julia
julia> ItinerarySearch.CLI.main(["info"])
```
"""
function main(args::Vector{String})::Int
    parser = _build_parser()

    # Parse — usage errors return exit 2
    parsed = try
        parse_args(args, parser)
    catch e
        if e isa ArgParse.ArgParseError
            println(stderr, "Error: ", e.text)
            return 2
        end
        rethrow(e)
    end

    parsed === nothing && return 0  # --help was printed

    # Global --quiet: suppress all log output (Error only)
    if parsed["quiet"]
        global_logger(ConsoleLogger(stderr, Logging.Error))
    end

    # Load config, apply overrides
    config = try
        _load_config(parsed)
    catch e
        println(stderr, "Config error: ", sprint(showerror, e))
        return 1
    end
    config = _apply_overrides(config, parsed)

    # Constraints (defaults only; no file-based override at CLI level)
    constraints = SearchConstraints()
    constraints = _apply_constraint_overrides(constraints, parsed)

    # Dispatch
    cmd = parsed["%COMMAND%"]
    sub_args = parsed[cmd]

    return try
        if cmd == "search"
            _cmd_search(config, constraints, sub_args, parsed)
        elseif cmd == "trip"
            _cmd_trip(config, constraints, sub_args, parsed)
        elseif cmd == "build"
            _cmd_build(config, sub_args, parsed)
        elseif cmd == "ingest"
            _cmd_ingest(config, sub_args, parsed)
        elseif cmd == "info"
            _cmd_info(config, sub_args, parsed)
        else
            println(stderr, "Unknown command: $cmd")
            2
        end
    catch e
        println(stderr, "Error: ", sprint(showerror, e))
        println(stderr, sprint(Base.show_backtrace, catch_backtrace()))
        1
    end
end

end # module CLI

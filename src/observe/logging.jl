# src/observe/logging.jl — Structured logging with DynaTrace-compatible JSON output

"""
    `function _resolve_log_level(config::SearchConfig)::Logging.LogLevel`
---

# Description
- Resolve the effective log level from ENV or config
- ENV variable `ITINERARY_SEARCH_LOG_LEVEL` takes precedence over `config.log_level`
- Recognized values: `debug`, `info`, `warn`, `error` (case-insensitive)
- Unrecognized values fall back to `Logging.Info`

# Arguments
1. `config::SearchConfig`: search configuration with `log_level` field

# Returns
- `::Logging.LogLevel`: the resolved log level
"""
function _resolve_log_level(config::SearchConfig)::Logging.LogLevel
    level_str = get(ENV, "ITINERARY_SEARCH_LOG_LEVEL", string(config.log_level))
    sym = Symbol(lowercase(level_str))
    sym == :debug && return Logging.Debug
    sym == :warn && return Logging.Warn
    sym == :error && return Logging.Error
    return Logging.Info  # default
end

"""
    `function _dynatrace_json_formatter(io::IO, args)`
---

# Description
- Format a Julia log record as a single DynaTrace-compatible JSON line
- Called by `FormatLogger` for each log event
- Writes one JSON object per line then a newline

# Arguments
1. `io::IO`: output stream to write the JSON line to
2. `args`: log record provided by the `LoggingExtras.FormatLogger` callback

# Returns
- `nothing`

Output format:
```json
{"timestamp":"...","severity":"INFO","content":"...","service.name":"ItinerarySearch","attributes":{...}}
```
"""
function _dynatrace_json_formatter(io::IO, args)
    severity = if args.level == Logging.Debug
        "DEBUG"
    elseif args.level == Logging.Info
        "INFO"
    elseif args.level == Logging.Warn
        "WARN"
    elseif args.level == Logging.Error
        "ERROR"
    else
        string(args.level)
    end

    attrs = Dict{String,Any}()
    for (k, v) in args.kwargs
        attrs[string(k)] = v
    end
    attrs["module"] = string(args._module)
    attrs["file"] = string(args.file)
    attrs["line"] = args.line
    attrs["thread_id"] = Threads.threadid()

    envelope = Dict{String,Any}(
        "timestamp" => Dates.format(Dates.now(Dates.UTC), dateformat"yyyy-mm-ddTHH:MM:SS.sssZ"),
        "severity" => severity,
        "content" => string(args.message),
        "service.name" => "ItinerarySearch",
        "attributes" => attrs,
    )

    JSON3.write(io, envelope)
    println(io)
    return nothing
end

"""
    `function setup_logger(config::SearchConfig)::AbstractLogger`
---

# Description
- Build a TeeLogger that fans out log events to a ConsoleLogger (stderr)
  and optionally FormatLoggers (DynaTrace JSON to file and/or stdout)
- All child loggers are wrapped in MinLevelLogger for uniform level gating
- Log level resolved from ENV["ITINERARY_SEARCH_LOG_LEVEL"] → config.log_level → :info

# Arguments
1. `config::SearchConfig`: search configuration with `log_level`, `log_json_path`,
   and `log_stdout_json` fields

# Returns
- `::AbstractLogger`: the assembled TeeLogger (two or more sinks) or MinLevelLogger
  (console only)

# Examples
```julia
julia> cfg = SearchConfig(log_level=:debug, log_stdout_json=true);
julia> logger = setup_logger(cfg);
```
"""
function setup_logger(config::SearchConfig)::AbstractLogger
    min_level = _resolve_log_level(config)

    loggers = AbstractLogger[
        MinLevelLogger(ConsoleLogger(stderr), min_level),
    ]

    if !isempty(config.log_json_path)
        mkpath(dirname(config.log_json_path))
        file_io = open(config.log_json_path, "w")
        push!(loggers, MinLevelLogger(FormatLogger(_dynatrace_json_formatter, file_io), min_level))
    end

    if config.log_stdout_json
        push!(loggers, MinLevelLogger(FormatLogger(_dynatrace_json_formatter, stdout), min_level))
    end

    length(loggers) == 1 && return loggers[1]
    return TeeLogger(loggers...)
end

"""
    `function _close_logger(logger)::Nothing`
---

# Description
- Walk the logger tree and close any file IO handles (not stdout/stderr)
- Handles `TeeLogger`, `MinLevelLogger`, and `FormatLogger` node types
- Safe to call on a plain `ConsoleLogger` — no-ops gracefully

# Arguments
1. `logger`: any logger returned by `setup_logger`

# Returns
- `nothing`
"""
function _close_logger(logger)::Nothing
    if logger isa TeeLogger
        for child in logger.loggers
            _close_logger(child)
        end
    elseif logger isa MinLevelLogger
        _close_logger(logger.logger)
    elseif logger isa FormatLogger
        s = logger.stream
        if s isa IO && s !== stdout && s !== stderr
            flush(s)
            close(s)
        end
    end
    return nothing
end

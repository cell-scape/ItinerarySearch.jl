# src/observe/sinks.jl — Event log sinks (JSONL file, stdout)

"""
    struct JsonlSink

Callable sink that writes events as newline-delimited JSON to an IO handle.
Each line has the envelope: `{"type":"EventTypeName","data":{...}}`
"""
struct JsonlSink
    io::IO
end

"""
    `JsonlSink(path::String)`
---

# Description
- Open a file for writing and return a `JsonlSink`
- Creates parent directories if they do not exist
- The caller is responsible for closing the sink via `close(sink.io)`

# Arguments
1. `path::String`: file path for the JSONL output

# Returns
- `::JsonlSink`: sink ready to receive events
"""
function JsonlSink(path::String)
    mkpath(dirname(path))
    io = open(path, "w")
    return JsonlSink(io)
end

function (s::JsonlSink)(event)
    type_name = string(nameof(typeof(event)))
    json = JSON3.write(event)
    println(s.io, """{"type":"$type_name","data":$json}""")
    flush(s.io)
    return nothing
end

"""
    `stdout_sink(event)`
---

# Description
- Write an event as a single JSONL line to stdout
- Envelope format: `{"type":"EventTypeName","data":{...}}`

# Arguments
1. `event`: any event struct registered with the event log

# Returns
- `nothing`
"""
function stdout_sink(event)
    type_name = string(nameof(typeof(event)))
    json = JSON3.write(event)
    println(stdout, """{"type":"$type_name","data":$json}""")
    return nothing
end

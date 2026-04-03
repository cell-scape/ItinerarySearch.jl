# src/audit/mct_inspect.jl — Interactive MCT inspector REPL

"""
    @kwdef mutable struct InspectorState

State for the interactive MCT inspector REPL loop.
"""
@kwdef mutable struct InspectorState
    lookup::MCTLookup
    connections::Vector{NamedTuple}
    position::Int = 0
    filters::Vector{Function} = Function[]
    detail::Bool = false
    max_candidates::Int = 10
    airports::Dict{StationCode,StationRecord} = Dict{StationCode,StationRecord}()
end

function _print_connection_header(io::IO, idx::Int, total::Int, params::NamedTuple)
    stn = String(params.arr_station)
    println(io, "── Connection $idx/$total: $stn ──────────────────────────────────────────")
    arr_cr = String(params.arr_carrier)
    dep_cr = String(params.dep_carrier)
    arr_flt = Int(params.arr_flt_no)
    dep_flt = Int(params.dep_flt_no)
    cs_arr = params.arr_is_codeshare ? "(Y)" : ""
    cs_dep = params.dep_is_codeshare ? "(Y)" : ""
    println(io, "  $(arr_cr)$(arr_flt)$(cs_arr) → $(stn) → $(dep_cr)$(dep_flt)$(cs_dep)")
    status = _status_str(params.status)
    cnx = Int(params.cnx_time)
    their_mct = Int(params.their_mct)
    their_diff = params.their_mct_diff
    println(io, "  Status: $status | CnxTime: $(cnx) min | Their MCT: $(their_mct) | Their diff: $(their_diff)")
end

function _print_cascade(io::IO, trace::MCTTrace, max_candidates::Int)
    cands = trace.candidates
    total = length(cands)
    show_n = max_candidates > 0 ? min(max_candidates, total) : total
    println(io, "\n  Cascade ($total candidates evaluated$(show_n < total ? ", showing top $show_n" : "")):")
    for i in 1:show_n
        c = cands[i]
        id = Int(c.record.mct_id)
        t = Int(c.record.time)
        spec = string(c.record.specificity, base=16)
        if c.matched && c.skip_reason == :none
            println(io, "  #$i [MATCH✓] mct_id=$id time=$t spec=0x$spec")
            fields = decode_matched_fields(c.record.specified)
            !isempty(fields) && println(io, "     matched: $fields")
        elseif c.matched && c.skip_reason == :supp_scope_miss
            println(io, "  #$i [skip: supp_scope_miss] mct_id=$id time=$t spec=0x$spec")
        else
            println(io, "  #$i [skip: $(c.skip_reason)] mct_id=$id time=$t spec=0x$spec")
        end
    end
    if show_n < total
        println(io, "  ... $(total - show_n) more candidates not shown")
    end
end

function _print_result(io::IO, trace::MCTTrace, params::NamedTuple)
    r = trace.result
    println(io, "\n  Result: time=$(Int(r.time)) source=$(_source_str(r.source)) mct_id=$(Int(r.mct_id))")
    fields = decode_matched_fields(r.matched_fields)
    !isempty(fields) && println(io, "  Matched fields: $fields")
    their_mct = Int(params.their_mct)
    our_mct = Int(r.time)
    time_match = our_mct == their_mct
    println(io, "  Their MCT: $their_mct (mctrec=$(Int(params.their_mctrec))) → $(time_match ? "TIME MATCH" : "TIME MISMATCH")")
    our_resolves = our_mct <= Int(params.cnx_time)
    println(io, "  Our MCT resolves: $(our_resolves ? "YES" : "NO") ($our_mct ≤ $(Int(params.cnx_time))? $(our_resolves ? "yes" : "no"))")
end

function _parse_filter(expr::AbstractString)::Union{Function, Nothing}
    expr = strip(expr)
    if expr == "mismatch"
        return (params, trace) -> trace.result.time != params.their_mct
    elseif expr == "resolves"
        return (params, trace) -> trace.result.time <= params.cnx_time
    elseif startswith(expr, "station=")
        stn = StationCode(expr[9:end])
        return (params, _) -> params.arr_station == stn
    elseif startswith(expr, "source=")
        src = expr[8:end]
        return (_, trace) -> _source_str(trace.result.source) == src
    end
    nothing
end

function _passes_filters(filters::Vector{Function}, params::NamedTuple, trace::MCTTrace)::Bool
    for f in filters
        f(params, trace) || return false
    end
    true
end

function _run_trace(state::InspectorState, params::NamedTuple)::MCTTrace
    prv_region = InlineString3("")
    nxt_region = InlineString3("")
    prv_stn_info = get(state.airports, params.arr_station, nothing)
    nxt_stn_info = get(state.airports, params.dep_station, nothing)
    prv_stn_info !== nothing && (prv_region = prv_stn_info.region)
    nxt_stn_info !== nothing && (nxt_region = nxt_stn_info.region)
    # Use prv_stn/nxt_stn from parsed params if available, else NO_STATION
    prv_stn = hasproperty(params, :prv_stn) ? params.prv_stn : NO_STATION
    nxt_stn = hasproperty(params, :nxt_stn) ? params.nxt_stn : NO_STATION
    lookup_mct_traced(
        state.lookup,
        params.arr_carrier, params.dep_carrier,
        params.arr_station, params.dep_station,
        params.status;
        arr_body = params.arr_body,
        dep_body = params.dep_body,
        prv_stn = prv_stn,
        nxt_stn = nxt_stn,
        arr_term = params.arr_term,
        dep_term = params.dep_term,
        arr_op_carrier = params.arr_op_carrier,
        dep_op_carrier = params.dep_op_carrier,
        arr_is_codeshare = params.arr_is_codeshare,
        dep_is_codeshare = params.dep_is_codeshare,
        arr_acft_type = params.arr_acft_type,
        dep_acft_type = params.dep_acft_type,
        arr_flt_no = params.arr_flt_no,
        dep_flt_no = params.dep_flt_no,
        prv_country = params.prv_country,
        nxt_country = params.nxt_country,
        prv_state = params.prv_state,
        nxt_state = params.nxt_state,
        prv_region = prv_region,
        nxt_region = nxt_region,
        target_date = params.target_date,
    )
end

function _print_help(io::IO)
    println(io, """
    MCT Inspector Commands:
      i / inspect     — Show full cascade trace for current connection
      c / enter       — Move to next connection
      s N / skip N    — Skip ahead N connections
      f <expr>        — Add filter (station=ORD, mismatch, resolves, source=exception)
      m / mismatch    — Shortcut: filter mismatch
      r / resolves    — Shortcut: filter resolves
      d / detail      — Toggle detail level
      clear           — Clear all filters
      h / help        — Show this help
      q / quit        — Exit inspector""")
end

"""
    `function mct_inspect(lookup::MCTLookup; misconnect::String="", airports=Dict{StationCode,StationRecord}(), io_in::IO=stdin, io_out::IO=stdout)`

Launch the interactive MCT inspector REPL. Load connections from a misconnect
CSV file and step through them, inspecting MCT cascade decisions.
"""
function mct_inspect(
    lookup::MCTLookup;
    misconnect::String = "",
    airports::Dict{StationCode,StationRecord} = Dict{StationCode,StationRecord}(),
    io_in::IO = stdin,
    io_out::IO = stdout,
)
    connections = NamedTuple[]
    if !isempty(misconnect)
        df = CSV.read(misconnect, DataFrame; stringtype=String)
        for row in eachrow(df)
            try
                push!(connections, parse_misconnect_row(row))
            catch e
                println(io_out, "  Warning: skipping row $(row.rcrd_loc): $e")
            end
        end
        println(io_out, "Loaded $(length(connections)) connections from $(basename(misconnect))")
    end

    if isempty(connections)
        println(io_out, "No connections to inspect.")
        return
    end

    state = InspectorState(
        lookup = lookup,
        connections = connections,
        airports = airports,
    )

    _print_help(io_out)
    println(io_out, "")

    state.position = 1
    _advance_to_next!(state, io_in, io_out)
end

function _advance_to_next!(state::InspectorState, io_in::IO, io_out::IO; skip::Int=0)
    total = length(state.connections)
    skipped = 0

    while state.position <= total
        params = state.connections[state.position]
        trace = _run_trace(state, params)

        if !isempty(state.filters) && !_passes_filters(state.filters, params, trace)
            state.position += 1
            continue
        end

        if skipped < skip
            skipped += 1
            state.position += 1
            continue
        end

        _print_connection_header(io_out, state.position, total, params)
        if state.detail
            _print_cascade(io_out, trace, state.max_candidates)
        end
        _print_result(io_out, trace, params)
        println(io_out, "")

        _command_loop!(state, io_in, io_out, params, trace)
        return
    end

    println(io_out, "No more connections$(isempty(state.filters) ? "." : " matching filters.")")
end

function _command_loop!(state::InspectorState, io_in::IO, io_out::IO, params::NamedTuple, trace::MCTTrace)
    total = length(state.connections)
    while true
        print(io_out, "mct[$(state.position)/$total]> ")
        line = try
            readline(io_in)
        catch
            return
        end
        cmd = strip(line)

        if cmd in ("q", "quit")
            println(io_out, "Exiting inspector.")
            return
        elseif cmd in ("", "c", "continue")
            state.position += 1
            _advance_to_next!(state, io_in, io_out)
            return
        elseif cmd in ("i", "inspect")
            _print_connection_header(io_out, state.position, total, params)
            _print_cascade(io_out, trace, state.max_candidates)
            _print_result(io_out, trace, params)
            println(io_out, "")
        elseif cmd in ("d", "detail")
            state.detail = !state.detail
            println(io_out, "Detail mode: $(state.detail ? "ON" : "OFF")")
        elseif cmd in ("m", "mismatch")
            f = _parse_filter("mismatch")
            push!(state.filters, f)
            println(io_out, "Filter added: mismatch")
        elseif cmd in ("r", "resolves")
            f = _parse_filter("resolves")
            push!(state.filters, f)
            println(io_out, "Filter added: resolves")
        elseif startswith(cmd, "f ") || startswith(cmd, "filter ")
            expr = startswith(cmd, "f ") ? cmd[3:end] : cmd[8:end]
            f = _parse_filter(expr)
            if f !== nothing
                push!(state.filters, f)
                println(io_out, "Filter added: $expr")
            else
                println(io_out, "Unknown filter: $expr")
            end
        elseif startswith(cmd, "s ") || startswith(cmd, "skip ")
            n_str = startswith(cmd, "s ") ? cmd[3:end] : cmd[6:end]
            n = tryparse(Int, strip(n_str))
            if n !== nothing && n > 0
                state.position += 1
                _advance_to_next!(state, io_in, io_out; skip=n)
                return
            else
                println(io_out, "Usage: skip N (positive integer)")
            end
        elseif cmd == "clear"
            empty!(state.filters)
            println(io_out, "Filters cleared.")
        elseif cmd in ("h", "help")
            _print_help(io_out)
        else
            println(io_out, "Unknown command: $cmd (type 'h' for help)")
        end
    end
end

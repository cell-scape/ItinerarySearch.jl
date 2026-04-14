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
    max_candidates::Int = 0    # 0 = show all
    airports::Dict{StationCode,StationRecord} = Dict{StationCode,StationRecord}()
    cascade_page_size::Int = 25     # candidates per page in cascade display
    cascade_shown::Int = 0          # how many candidates shown so far for current connection
    style::DisplayStyle = _default_style()
end

# ── Formatting helpers ────────────────────────────────────────────────────────

_format_packed_date(d::UInt32)::String =
    d == 0 ? "-" : "$(d ÷ 10000)-$(lpad((d % 10000) ÷ 100, 2, '0'))-$(lpad(d % 100, 2, '0'))"

function _format_leg(params::NamedTuple, side::Symbol)::String
    if side === :arr
        carrier  = String(params.arr_carrier)
        flt      = Int(params.arr_flt_no)
        op_cr    = String(params.arr_op_carrier)
        op_flt   = hasproperty(params, :arr_op_flt_no) ? Int(params.arr_op_flt_no) : 0
        is_cs    = params.arr_is_codeshare
        from_stn = hasproperty(params, :prv_stn) ? String(params.prv_stn) : "---"
        to_stn   = String(params.arr_station)
        term     = String(params.arr_term)
        body     = params.arr_body == ' ' ? "-" : string(params.arr_body)
        acft     = String(params.arr_acft_type)
        country  = String(params.prv_country)
        state    = String(params.prv_state)
    else
        carrier  = String(params.dep_carrier)
        flt      = Int(params.dep_flt_no)
        op_cr    = String(params.dep_op_carrier)
        op_flt   = hasproperty(params, :dep_op_flt_no) ? Int(params.dep_op_flt_no) : 0
        is_cs    = params.dep_is_codeshare
        from_stn = String(params.dep_station)
        to_stn   = hasproperty(params, :nxt_stn) ? String(params.nxt_stn) : "---"
        term     = String(params.dep_term)
        body     = params.dep_body == ' ' ? "-" : string(params.dep_body)
        acft     = String(params.dep_acft_type)
        country  = String(params.nxt_country)
        state    = String(params.nxt_state)
    end
    cs_tag = is_cs ? " (CS)" : ""
    lines = "    Mkt:   $(carrier) $(flt)$(cs_tag)"
    if is_cs
        lines *= "\n    Op:    $(op_cr) $(op_flt)"
    end
    lines *= "\n    Route: $(from_stn) → $(to_stn)"
    lines *= "\n    Term:  $(isempty(strip(term)) ? "-" : term)   Body: $(body)   Acft: $(isempty(strip(acft)) ? "-" : acft)"
    lines *= "\n    Geo:   $(isempty(strip(country)) ? "-" : country) / $(isempty(strip(state)) ? "-" : state)"
    lines
end

function _format_mct_record(rec::MCTRecord)::String
    lines = String[]
    push!(lines, "    MCT ID: $(Int(rec.mct_id))   Time: $(Int(rec.time)) min   Spec: 0x$(string(rec.specificity, base=16))   Serial: $(Int(rec.record_serial))")
    push!(lines, "    Station Std: $(rec.station_standard ? "YES" : "NO")   Suppressed: $(rec.suppressed ? "YES" : "NO")")
    # Date validity
    if rec.eff_date != 0 || rec.dis_date != 0
        push!(lines, "    Valid: $(_format_packed_date(rec.eff_date)) to $(_format_packed_date(rec.dis_date))")
    end
    # Specified matching fields with values
    specified_parts = String[]
    for (bit, name, rec_fn, _) in _MCT_FIELD_EXTRACTORS
        if (rec.specified & bit) != 0
            push!(specified_parts, "$(name)=$(rec_fn(rec))")
        end
    end
    if (rec.specified & MCT_BIT_ARR_FLT_RNG) != 0
        push!(specified_parts, "ARR_FLT_RNG=$(Int(rec.arr_flt_rng_start))-$(Int(rec.arr_flt_rng_end))")
    end
    if (rec.specified & MCT_BIT_DEP_FLT_RNG) != 0
        push!(specified_parts, "DEP_FLT_RNG=$(Int(rec.dep_flt_rng_start))-$(Int(rec.dep_flt_rng_end))")
    end
    if !isempty(specified_parts)
        push!(lines, "    Specified: $(join(specified_parts, ", "))")
    end
    # Suppression geography
    supp_parts = String[]
    !isempty(strip(String(rec.supp_region)))  && push!(supp_parts, "region=$(String(rec.supp_region))")
    !isempty(strip(String(rec.supp_country))) && push!(supp_parts, "country=$(String(rec.supp_country))")
    !isempty(strip(String(rec.supp_state)))   && push!(supp_parts, "state=$(String(rec.supp_state))")
    !isempty(supp_parts) && push!(lines, "    Supp Geo: $(join(supp_parts, ", "))")
    join(lines, "\n")
end

function _format_codeshare_table(trace::MCTTrace, params::NamedTuple)::String
    op_time = Int(trace.operating_result.time)
    arr_cr = String(params.arr_carrier)
    dep_cr = String(params.dep_carrier)
    arr_op = String(params.arr_op_carrier)
    dep_op = String(params.dep_op_carrier)
    op_arr = params.arr_is_codeshare ? arr_op : arr_cr
    op_dep = params.dep_is_codeshare ? dep_op : dep_cr

    header = "    Mode      | Arr CR | Dep CR | Time | MCT ID | Spec     | Floor | Winner"
    sep    = "    ----------|--------|--------|------|--------|----------|-------|-------"
    rows = String[]

    function _cs_row(label, result, a_cr, d_cr, mode_sym)
        result === EMPTY_MCT_RESULT && return nothing
        t = Int(result.time)
        id = Int(result.mct_id)
        spec = "0x" * lpad(string(result.specificity, base=16), 6, '0')
        floor_ok = mode_sym === :operating ? "---" : (t >= op_time ? "YES" : "NO")
        winner = trace.codeshare_mode === mode_sym ? ">>>" : ""
        "    $(rpad(label, 10))| $(rpad(a_cr, 7))| $(rpad(d_cr, 7))| $(lpad(string(t), 4)) | $(lpad(string(id), 6)) | $(spec) | $(rpad(floor_ok, 5)) | $(winner)"
    end

    r = _cs_row("YY (mkt)", trace.marketing_result, arr_cr, dep_cr, :marketing)
    r !== nothing && push!(rows, r)
    r = _cs_row("YN (dep)", trace.dep_cs_result, op_arr, dep_cr, :dep_cs)
    r !== nothing && push!(rows, r)
    r = _cs_row("NY (arr)", trace.arr_cs_result, arr_cr, op_dep, :arr_cs)
    r !== nothing && push!(rows, r)
    r = _cs_row("NN (op)", trace.operating_result, op_arr, op_dep, :operating)
    r !== nothing && push!(rows, r)

    join([header, sep, rows...], "\n")
end

# ── Display functions (PlainStyle) ────────────────────────────────────────────

function _print_connection_header(io::IO, idx::Int, total::Int, params::NamedTuple, ::PlainStyle)
    stn = String(params.arr_station)
    println(io, "══ Connection $idx/$total: $stn ══════════════════════════════════════════")
    println(io, "")
    println(io, "  Arriving Leg:")
    println(io, _format_leg(params, :arr))
    println(io, "")
    println(io, "  Departing Leg:")
    println(io, _format_leg(params, :dep))
    println(io, "")
    status = _status_str(params.status)
    cnx = Int(params.cnx_time)
    their_mct = Int(params.their_mct)
    their_diff = params.their_mct_diff
    println(io, "  Status: $status | CnxTime: $(cnx) min | Their MCT: $(their_mct) | Diff: $(their_diff)")
end

function _print_cascade(io::IO, trace::MCTTrace, max_candidates::Int, style::PlainStyle; from::Int=1)
    cands = trace.candidates
    total = length(cands)
    limit = max_candidates > 0 ? min(max_candidates, total) : total
    show_end = min(from + limit - 1, total)
    if from > total
        println(io, "  No more candidates.")
        return show_end
    end
    if from == 1
        println(io, "\n  Cascade ($total candidates evaluated):")
    end
    for i in from:show_end
        c = cands[i]
        _print_candidate(io, i, c, trace, style)
    end
    if show_end < total
        println(io, "  ... $(total - show_end) more (type 'more' to see next page)")
    end
    return show_end
end

function _print_candidate(io::IO, idx::Int, c::MCTCandidateTrace, trace::MCTTrace, ::PlainStyle)
    id = Int(c.record.mct_id)
    t = Int(c.record.time)
    spec = string(c.record.specificity, base=16)
    specified = decode_matched_fields(c.record.specified)

    if c.matched && c.skip_reason == :none
        println(io, "  #$idx [MATCH] mct_id=$id time=$t spec=0x$spec")
        !isempty(specified) && println(io, "       specified: $specified")
        println(io, _format_mct_record(c.record))
    elseif c.skip_reason == :field_mismatch
        mm_str = decode_matched_fields(c.mismatched_fields)
        println(io, "  #$idx [skip: field_mismatch] mct_id=$id time=$t spec=0x$spec")
        !isempty(specified) && println(io, "       specified: $specified")
        !isempty(mm_str) && println(io, "       failed on: $mm_str")
        _print_mismatch_values(io, c, trace, PlainStyle())
    elseif c.skip_reason == :date_expired
        println(io, "  #$idx [skip: date_expired] mct_id=$id time=$t spec=0x$spec")
    elseif c.skip_reason == :supp_scope_miss
        println(io, "  #$idx [skip: supp_scope_miss] mct_id=$id time=$t spec=0x$spec")
    else
        println(io, "  #$idx [skip: $(c.skip_reason)] mct_id=$id time=$t spec=0x$spec")
    end
end

# Field value extraction from MCTRecord (expected) and MCTTrace (actual connection)
const _MCT_FIELD_EXTRACTORS = (
    (MCT_BIT_ARR_CARRIER,   "ARR_CARRIER",   r -> String(r.arr_carrier),   t -> String(t.arr_carrier)),
    (MCT_BIT_DEP_CARRIER,   "DEP_CARRIER",   r -> String(r.dep_carrier),   t -> String(t.dep_carrier)),
    (MCT_BIT_ARR_TERM,      "ARR_TERM",      r -> String(r.arr_term),      t -> String(t.arr_term)),
    (MCT_BIT_DEP_TERM,      "DEP_TERM",      r -> String(r.dep_term),      t -> String(t.dep_term)),
    (MCT_BIT_PRV_STN,       "PRV_STN",       r -> String(r.prv_stn),       t -> String(t.prv_stn)),
    (MCT_BIT_NXT_STN,       "NXT_STN",       r -> String(r.nxt_stn),       t -> String(t.nxt_stn)),
    (MCT_BIT_PRV_COUNTRY,   "PRV_COUNTRY",   r -> String(r.prv_country),   t -> String(t.prv_country)),
    (MCT_BIT_NXT_COUNTRY,   "NXT_COUNTRY",   r -> String(r.nxt_country),   t -> String(t.nxt_country)),
    (MCT_BIT_PRV_REGION,    "PRV_REGION",     r -> String(r.prv_region),    t -> String(t.prv_region)),
    (MCT_BIT_NXT_REGION,    "NXT_REGION",     r -> String(r.nxt_region),    t -> String(t.nxt_region)),
    (MCT_BIT_DEP_BODY,      "DEP_BODY",      r -> string(r.dep_body),      t -> string(t.dep_body)),
    (MCT_BIT_ARR_BODY,      "ARR_BODY",      r -> string(r.arr_body),      t -> string(t.arr_body)),
    (MCT_BIT_ARR_CS_IND,    "ARR_CS_IND",    r -> string(r.arr_cs_ind),    t -> string(t.arr_is_codeshare ? 'Y' : 'N')),
    (MCT_BIT_DEP_CS_IND,    "DEP_CS_IND",    r -> string(r.dep_cs_ind),    t -> string(t.dep_is_codeshare ? 'Y' : 'N')),
    (MCT_BIT_ARR_CS_OP,     "ARR_CS_OP",     r -> String(r.arr_cs_op_carrier), t -> String(t.arr_op_carrier)),
    (MCT_BIT_DEP_CS_OP,     "DEP_CS_OP",     r -> String(r.dep_cs_op_carrier), t -> String(t.dep_op_carrier)),
    (MCT_BIT_ARR_ACFT_TYPE, "ARR_ACFT_TYPE", r -> String(r.arr_acft_type), t -> String(t.arr_acft_type)),
    (MCT_BIT_DEP_ACFT_TYPE, "DEP_ACFT_TYPE", r -> String(r.dep_acft_type), t -> String(t.dep_acft_type)),
    (MCT_BIT_PRV_STATE,     "PRV_STATE",     r -> String(r.prv_state),     t -> String(t.prv_state)),
    (MCT_BIT_NXT_STATE,     "NXT_STATE",     r -> String(r.nxt_state),     t -> String(t.nxt_state)),
)

function _print_mismatch_values(io::IO, c::MCTCandidateTrace, trace::MCTTrace, ::PlainStyle)
    mm = c.mismatched_fields
    mm == UInt32(0) && return
    for (bit, name, rec_fn, trace_fn) in _MCT_FIELD_EXTRACTORS
        if (mm & bit) != 0
            rec_val = rec_fn(c.record)
            trace_val = trace_fn(trace)
            println(io, "                 $name: record=\"$rec_val\" connection=\"$trace_val\"")
        end
    end
    if (mm & MCT_BIT_ARR_FLT_RNG) != 0
        println(io, "                 ARR_FLT_RNG: record=$(Int(c.record.arr_flt_rng_start))-$(Int(c.record.arr_flt_rng_end)) connection=$(Int(trace.arr_flt_no))")
    end
    if (mm & MCT_BIT_DEP_FLT_RNG) != 0
        println(io, "                 DEP_FLT_RNG: record=$(Int(c.record.dep_flt_rng_start))-$(Int(c.record.dep_flt_rng_end)) connection=$(Int(trace.dep_flt_no))")
    end
end

function _print_result(io::IO, trace::MCTTrace, params::NamedTuple, ::PlainStyle)
    r = trace.result
    println(io, "\n  Result: time=$(Int(r.time)) source=$(_source_str(r.source)) mct_id=$(Int(r.mct_id))")
    fields = decode_matched_fields(r.matched_fields)
    !isempty(fields) && println(io, "  Matched fields: $fields")
    if trace.codeshare_mode != :none
        println(io, "\n  Codeshare Resolution (winner=$(trace.codeshare_mode)):")
        println(io, _format_codeshare_table(trace, params))
    end
    their_mct = Int(params.their_mct)
    our_mct = Int(r.time)
    time_match = our_mct == their_mct
    println(io, "  Their MCT: $their_mct (mctrec=$(Int(params.their_mctrec))) → $(time_match ? "TIME MATCH" : "TIME MISMATCH")")
    our_resolves = our_mct <= Int(params.cnx_time)
    println(io, "  Our MCT resolves: $(our_resolves ? "YES" : "NO") ($our_mct ≤ $(Int(params.cnx_time))? $(our_resolves ? "yes" : "no"))")
end

function _print_help(io::IO, ::PlainStyle)
    println(io, """
    MCT Inspector Commands:
      i / inspect     — Show cascade trace for current connection (paged)
      more            — Show next page of candidates
      c / enter       — Move to next connection
      s N / skip N    — Skip ahead N connections
      f <expr>        — Add filter (station=ORD, mismatch, resolves, source=exception)
      m / mismatch    — Shortcut: filter mismatch
      r / resolves    — Shortcut: filter resolves
      d / detail      — Toggle auto-cascade on each connection
      clear           — Clear all filters
      h / help        — Show this help
      q / quit        — Exit inspector""")
end

# ── Filters ───────────────────────────────────────────────────────────────────

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

# ── Trace runner ──────────────────────────────────────────────────────────────

function _run_trace(state::InspectorState, params::NamedTuple)::MCTTrace
    prv_region = InlineString3("")
    nxt_region = InlineString3("")
    prv_stn_info = get(state.airports, params.arr_station, nothing)
    nxt_stn_info = get(state.airports, params.dep_station, nothing)
    prv_stn_info !== nothing && (prv_region = prv_stn_info.region)
    nxt_stn_info !== nothing && (nxt_region = nxt_stn_info.region)
    prv_stn = hasproperty(params, :prv_stn) ? params.prv_stn : NO_STATION
    nxt_stn = hasproperty(params, :nxt_stn) ? params.nxt_stn : NO_STATION
    arr_op_flt_no = hasproperty(params, :arr_op_flt_no) ? params.arr_op_flt_no : FlightNumber(0)
    dep_op_flt_no = hasproperty(params, :dep_op_flt_no) ? params.dep_op_flt_no : FlightNumber(0)
    lookup_mct_codeshare_traced(
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
        arr_op_flt_no = arr_op_flt_no,
        dep_op_flt_no = dep_op_flt_no,
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

# ── Entry point ───────────────────────────────────────────────────────────────

"""
    `function mct_inspect(lookup::MCTLookup; misconnect::String="", airports=Dict{StationCode,StationRecord}(), io_in::IO=stdin, io_out::IO=stdout)`

Launch the interactive MCT inspector REPL. Load connections from a misconnect
CSV file and step through them, inspecting MCT cascade decisions.
"""
function mct_inspect(
    lookup::MCTLookup;
    misconnect::String = "",
    airports::Dict{StationCode,StationRecord} = Dict{StationCode,StationRecord}(),
    acft_body::Dict{String,Char} = Dict{String,Char}(),
    io_in::IO = stdin,
    io_out::IO = stdout,
)
    connections = NamedTuple[]
    if !isempty(misconnect)
        df = CSV.read(misconnect, DataFrame; stringtype=String)
        for row in eachrow(df)
            try
                push!(connections, parse_misconnect_row(row; acft_body))
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

    _print_help(io_out, state.style)
    println(io_out, "")

    state.position = 1
    _advance_to_next!(state, io_in, io_out)
end

# ── REPL loop ─────────────────────────────────────────────────────────────────

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

        state.cascade_shown = 0
        _print_connection_header(io_out, state.position, total, params, state.style)
        if state.detail
            state.cascade_shown = _print_cascade(io_out, trace, state.cascade_page_size, state.style)
        end
        _print_result(io_out, trace, params, state.style)
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
            state.cascade_shown = 0
            _print_connection_header(io_out, state.position, total, params, state.style)
            state.cascade_shown = _print_cascade(io_out, trace, state.cascade_page_size, state.style)
            _print_result(io_out, trace, params, state.style)
            println(io_out, "")
        elseif cmd == "more"
            if state.cascade_shown > 0 && state.cascade_shown < length(trace.candidates)
                state.cascade_shown = _print_cascade(io_out, trace, state.cascade_page_size, state.style; from=state.cascade_shown + 1)
            else
                println(io_out, "No more candidates to show. Use 'i' to restart cascade.")
            end
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
            _print_help(io_out, state.style)
        else
            println(io_out, "Unknown command: $cmd (type 'h' for help)")
        end
    end
end

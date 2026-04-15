# ext/TermExt.jl — Term.jl styled rendering for MCT inspector
#
# Loaded automatically when both ItinerarySearch and Term are active.
# Provides TermStyle <: DisplayStyle with colored panels, tables, and markup.

module TermExt

using ItinerarySearch
using ItinerarySearch: DisplayStyle, PlainStyle, MCTTrace, MCTCandidateTrace,
    MCTRecord, EMPTY_MCT_RESULT, MCTLookup, InspectorState,
    _print_connection_header, _print_cascade, _print_candidate,
    _print_mismatch_values, _print_result, _print_help,
    _print_legs_detail, _print_mct_detail,
    _format_leg, _format_mct_record, _format_codeshare_table,
    _format_legs_detail, _format_mct_detail, _resolve_cs_mode,
    _format_packed_date, _MCT_FIELD_EXTRACTORS,
    _source_str, _status_str, decode_matched_fields,
    MCT_BIT_ARR_FLT_RNG, MCT_BIT_DEP_FLT_RNG,
    StationCode, StationRecord, FlightNumber, NO_STATION, NO_AIRLINE,
    MCTStatus, MCTSource, SOURCE_EXCEPTION, SOURCE_STATION_STANDARD,
    InlineString3
using Term
using Term: Panel, tprint, tprintln

"""
    struct TermStyle <: DisplayStyle

Styled display using Term.jl panels, tables, and ANSI color markup.
Automatically selected when Term.jl is loaded.
"""
struct TermStyle <: DisplayStyle end

# Register TermStyle as default when extension loads
function __init__()
    ItinerarySearch._DEFAULT_STYLE[] = TermStyle()
end

# ── Connection header ─────────────────────────────────────────────────────────

function ItinerarySearch._print_connection_header(io::IO, idx::Int, total::Int, params::NamedTuple, ::TermStyle)
    stn = String(params.arr_station)

    # Build leg content with markup
    arr_content = _styled_leg(params, :arr)
    dep_content = _styled_leg(params, :dep)

    arr_panel = Panel(arr_content; title="Arriving", style="cyan", fit=true, padding=(1, 1, 0, 0))
    dep_panel = Panel(dep_content; title="Departing", style="cyan", fit=true, padding=(1, 1, 0, 0))

    layout = arr_panel * dep_panel
    outer = Panel(string(layout); title="Connection $idx/$total: $stn", style="blue", fit=true)
    print(io, outer)
    println(io)

    status = _status_str(params.status)
    cnx = Int(params.cnx_time)
    their_mct = Int(params.their_mct)
    diff = params.their_mct_diff
    diff_color = diff >= 0 ? "green" : "red"
    tprintln(io, "  Status: {bold}$status{/bold} | CnxTime: {bold}$(cnx){/bold} min | Their MCT: {bold}$(their_mct){/bold} | Diff: {$(diff_color)}$(diff){/$(diff_color)}")
end

function _styled_leg(params::NamedTuple, side::Symbol)::String
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
    cs_tag = is_cs ? " {magenta}(CS){/magenta}" : ""
    lines = "{bold}$(carrier) $(flt){/bold}$(cs_tag)"
    if is_cs
        lines *= "\n{dim}Op: $(op_cr) $(op_flt){/dim}"
    end
    lines *= "\nRoute: $(from_stn) → $(to_stn)"
    t = isempty(strip(term)) ? "-" : term
    a = isempty(strip(acft)) ? "-" : acft
    lines *= "\nTerm: $(t)  Body: $(body)  Acft: $(a)"
    c = isempty(strip(country)) ? "-" : country
    s = isempty(strip(state)) ? "-" : state
    lines *= "\nGeo: $(c) / $(s)"
    lines
end

# ── Cascade ───────────────────────────────────────────────────────────────────

function ItinerarySearch._print_cascade(io::IO, trace::MCTTrace, max_candidates::Int, style::TermStyle; from::Int=1)
    cands = trace.candidates
    total = length(cands)
    limit = max_candidates > 0 ? min(max_candidates, total) : total
    show_end = min(from + limit - 1, total)
    if from > total
        tprintln(io, "  {dim}No more candidates.{/dim}")
        return show_end
    end
    if from == 1
        tprintln(io, "\n  {cyan bold}Cascade ($total candidates evaluated):{/cyan bold}")
    end
    for i in from:show_end
        ItinerarySearch._print_candidate(io, i, cands[i], trace, style)
    end
    if show_end < total
        tprintln(io, "  {dim}... $(total - show_end) more (type 'more' to see next page){/dim}")
    end
    return show_end
end

# ── Candidate ─────────────────────────────────────────────────────────────────

function ItinerarySearch._print_candidate(io::IO, idx::Int, c::MCTCandidateTrace, trace::MCTTrace, ::TermStyle)
    id = Int(c.record.mct_id)
    t = Int(c.record.time)
    spec = string(c.record.specificity, base=16)
    specified = decode_matched_fields(c.record.specified;
        eff_date=c.record.eff_date, dis_date=c.record.dis_date)

    if c.matched && c.skip_reason == :none
        tprintln(io, "  {green bold}#$idx \\[MATCH]{/green bold} mct_id={bold}$id{/bold} time={bold}$t{/bold} spec=0x$spec")
        !isempty(specified) && tprintln(io, "       {dim}specified: $specified{/dim}")
        # Show full decoded MCT record in a styled panel
        rec_content = _format_mct_record(c.record)
        panel = Panel(rec_content; title="Matched MCT Record (id=$id)", style="green", fit=true, padding=(1, 1, 0, 0))
        print(io, panel)
        println(io)
    elseif c.skip_reason == :field_mismatch
        mm_str = decode_matched_fields(c.mismatched_fields)
        tprintln(io, "  {red}#$idx \\[skip: field_mismatch]{/red} mct_id=$id time=$t spec=0x$spec")
        !isempty(specified) && tprintln(io, "       {dim}specified: $specified{/dim}")
        !isempty(mm_str) && tprintln(io, "       {red}failed on: $mm_str{/red}")
        ItinerarySearch._print_mismatch_values(io, c, trace, TermStyle())
    elseif c.skip_reason == :date_expired
        tprintln(io, "  {yellow}#$idx \\[skip: date_expired]{/yellow} {dim}mct_id=$id time=$t spec=0x$spec{/dim}")
    elseif c.skip_reason == :supp_scope_miss
        tprintln(io, "  {yellow}#$idx \\[skip: supp_scope_miss]{/yellow} {dim}mct_id=$id time=$t spec=0x$spec{/dim}")
    else
        tprintln(io, "  {dim}#$idx \\[skip: $(c.skip_reason)] mct_id=$id time=$t spec=0x$spec{/dim}")
    end
end

# ── Mismatch values ──────────────────────────────────────────────────────────

function ItinerarySearch._print_mismatch_values(io::IO, c::MCTCandidateTrace, trace::MCTTrace, ::TermStyle)
    mm = c.mismatched_fields
    mm == UInt32(0) && return
    for (bit, name, rec_fn, trace_fn) in _MCT_FIELD_EXTRACTORS
        if (mm & bit) != 0
            rec_val = rec_fn(c.record)
            trace_val = trace_fn(trace)
            tprintln(io, "                 {cyan}$name{/cyan}: record={red}\"$rec_val\"{/red} connection={green}\"$trace_val\"{/green}")
        end
    end
    if (mm & MCT_BIT_ARR_FLT_RNG) != 0
        tprintln(io, "                 {cyan}ARR_FLT_RNG{/cyan}: record={red}$(Int(c.record.arr_flt_rng_start))-$(Int(c.record.arr_flt_rng_end)){/red} connection={green}$(Int(trace.arr_flt_no)){/green}")
    end
    if (mm & MCT_BIT_DEP_FLT_RNG) != 0
        tprintln(io, "                 {cyan}DEP_FLT_RNG{/cyan}: record={red}$(Int(c.record.dep_flt_rng_start))-$(Int(c.record.dep_flt_rng_end)){/red} connection={green}$(Int(trace.dep_flt_no)){/green}")
    end
end

# ── Result ────────────────────────────────────────────────────────────────────

function ItinerarySearch._print_result(io::IO, trace::MCTTrace, params::NamedTuple, ::TermStyle)
    r = trace.result
    src = _source_str(r.source)
    src_color = r.source == SOURCE_EXCEPTION ? "green" : r.source == SOURCE_STATION_STANDARD ? "yellow" : "dim"
    tprintln(io, "\n  Result: time={bold}$(Int(r.time)){/bold} source={$(src_color)}$src{/$(src_color)} mct_id={bold}$(Int(r.mct_id)){/bold}")
    matched_rec = ItinerarySearch._find_record_by_id(trace, r.mct_id)
    fields = if matched_rec !== nothing
        decode_matched_fields(r.matched_fields;
            eff_date=matched_rec.eff_date, dis_date=matched_rec.dis_date)
    else
        decode_matched_fields(r.matched_fields)
    end
    !isempty(fields) && tprintln(io, "  {dim}Matched fields: $fields{/dim}")

    if trace.codeshare_mode != :none
        cs_content = _format_codeshare_table(trace, params)
        if !isempty(cs_content)
            panel = Panel(cs_content; title="Codeshare Resolution (winner=$(trace.codeshare_mode))", style="magenta", fit=true, padding=(1, 1, 0, 0))
            print(io, panel)
            println(io)
        end
    end

    their_mct = Int(params.their_mct)
    our_mct = Int(r.time)
    time_match = our_mct == their_mct
    match_str = time_match ? "{green bold}TIME MATCH{/green bold}" : "{red bold}TIME MISMATCH{/red bold}"
    tprintln(io, "  Their MCT: $their_mct (mctrec=$(Int(params.their_mctrec))) → $match_str")
    our_resolves = our_mct <= Int(params.cnx_time)
    resolves_color = our_resolves ? "green" : "red"
    tprintln(io, "  Our MCT resolves: {$(resolves_color)}$(our_resolves ? "YES" : "NO"){/$(resolves_color)} ($our_mct ≤ $(Int(params.cnx_time))? $(our_resolves ? "yes" : "no"))")
end

# ── Help ──────────────────────────────────────────────────────────────────────

function ItinerarySearch._print_help(io::IO, ::TermStyle)
    content = """
{cyan bold}i{/cyan bold} / {cyan bold}inspect{/cyan bold}     Show cascade trace for current connection (paged)
{cyan bold}more{/cyan bold}            Show next page of candidates
{cyan bold}l{/cyan bold} / {cyan bold}legs{/cyan bold}        Show all fields for both connecting legs side by side
{cyan bold}x{/cyan bold} / {cyan bold}mct{/cyan bold}         Show legs + matched MCT record in unified view
{cyan bold}x yy{/cyan bold}|{cyan bold}yn{/cyan bold}|{cyan bold}ny{/cyan bold}|{cyan bold}nn{/cyan bold}   Show detail for a specific codeshare lookup option
{cyan bold}c{/cyan bold} / {cyan bold}enter{/cyan bold}       Move to next connection
{cyan bold}s N{/cyan bold} / {cyan bold}skip N{/cyan bold}    Skip ahead N connections
{cyan bold}f{/cyan bold} <expr>        Add filter (station=ORD, mismatch, resolves, source=exception)
{cyan bold}m{/cyan bold} / {cyan bold}mismatch{/cyan bold}    Shortcut: filter mismatch
{cyan bold}r{/cyan bold} / {cyan bold}resolves{/cyan bold}    Shortcut: filter resolves
{cyan bold}d{/cyan bold} / {cyan bold}detail{/cyan bold}      Toggle auto-cascade on each connection
{cyan bold}clear{/cyan bold}           Clear all filters
{cyan bold}h{/cyan bold} / {cyan bold}help{/cyan bold}        Show this help
{cyan bold}q{/cyan bold} / {cyan bold}quit{/cyan bold}        Exit inspector"""
    panel = Panel(content; title="MCT Inspector Commands", style="cyan", fit=true, padding=(1, 1, 0, 0))
    print(io, panel)
    println(io)
end

# ── Legs detail ──────────────────────────────────────────────────────────────

function ItinerarySearch._print_legs_detail(io::IO, params::NamedTuple, ::TermStyle)
    stn = String(params.arr_station)
    content = _format_legs_detail(params)
    panel = Panel(content; title="Connecting Legs at $stn", style="cyan", fit=true, padding=(1, 1, 0, 0))
    print(io, panel)
    println(io)
end

# ── MCT record detail ────────────────────────────────────────────────────────

function ItinerarySearch._print_mct_detail(io::IO, trace::MCTTrace, params::NamedTuple, ::TermStyle;
        mode::AbstractString="",
        airports::Dict{StationCode,StationRecord}=Dict{StationCode,StationRecord}())
    stn = String(params.arr_station)
    cnx_info = get(airports, params.arr_station, nothing)

    if !isempty(mode)
        resolved = _resolve_cs_mode(mode, trace, params)
        if resolved === nothing
            tprintln(io, "  {red}Unknown codeshare mode: $mode (use: yy, yn, ny, nn){/red}")
            return
        end
        label, r, lm = resolved
        if r === EMPTY_MCT_RESULT
            tprintln(io, "  {dim}$label: no result (lookup not performed for this connection).{/dim}")
            return
        end
        if r.mct_id == Int32(0)
            tprintln(io, "\n  {dim}$label: global default ($(Int(r.time)) min), no specific MCT record.{/dim}")
            return
        end
        rec = ItinerarySearch._find_record_by_id(trace, r.mct_id)
        if rec === nothing
            tprintln(io, "\n  {dim}$label: serial=$(Int(r.mct_id)) time=$(Int(r.time)) min (record not in trace candidates).{/dim}")
            return
        end
        content = _format_mct_detail(rec, trace, params; lookup_mode=lm, cnx_stn_info=cnx_info)
        panel = Panel(content; title="$label — Serial $(Int(rec.record_serial)) at $stn", style="magenta", fit=true, padding=(1, 1, 0, 0))
        print(io, panel)
        println(io)
        return
    end

    r = trace.result
    if r.mct_id == Int32(0)
        tprintln(io, "\n  {dim}No specific MCT record matched (global default).{/dim}")
        content = _format_legs_detail(params)
        panel = Panel(content; title="Connecting Legs at $stn", style="cyan", fit=true, padding=(1, 1, 0, 0))
        print(io, panel)
        println(io)
        return
    end
    rec = ItinerarySearch._find_record_by_id(trace, r.mct_id)
    if rec === nothing
        tprintln(io, "\n  {dim}MCT record serial=$(Int(r.mct_id)) not found in trace candidates.{/dim}")
        return
    end
    winner_mode = trace.codeshare_mode
    winner_label = winner_mode == :none ? "" : " ($(winner_mode) winner)"
    lm = winner_mode in (:operating,) ? :operating :
         winner_mode in (:dep_cs,) ? :dep_cs :
         winner_mode in (:arr_cs,) ? :arr_cs : :marketing
    content = _format_mct_detail(rec, trace, params; lookup_mode=lm, cnx_stn_info=cnx_info)
    panel = Panel(content; title="Legs + MCT Serial $(Int(rec.record_serial)) at $stn$winner_label", style="green", fit=true, padding=(1, 1, 0, 0))
    print(io, panel)
    println(io)
end

end # module TermExt

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
    station_regions::Dict{StationCode,Set{InlineString3}} = Dict{StationCode,Set{InlineString3}}()
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

function _format_legs_detail(params::NamedTuple)::String
    # Every field from both legs, side by side
    _v(x) = let s = string(x); isempty(strip(s)) ? "-" : strip(s) end
    _flt(x) = Int(x) == 0 ? "-" : string(Int(x))

    fields = [
        ("Field",           "Arriving Leg",                                 "Departing Leg"),
        ("Carrier",         _v(params.arr_carrier),                         _v(params.dep_carrier)),
        ("Flight No",       _flt(params.arr_flt_no),                        _flt(params.dep_flt_no)),
        ("Codeshare",       params.arr_is_codeshare ? "YES" : "NO",         params.dep_is_codeshare ? "YES" : "NO"),
        ("Op Carrier",      _v(params.arr_op_carrier),                      _v(params.dep_op_carrier)),
        ("Op Flight No",    hasproperty(params,:arr_op_flt_no) ? _flt(params.arr_op_flt_no) : "-",
                            hasproperty(params,:dep_op_flt_no) ? _flt(params.dep_op_flt_no) : "-"),
        ("Station",         _v(params.arr_station),                         _v(params.dep_station)),
        ("Origin/Dest",     hasproperty(params,:prv_stn) ? _v(params.prv_stn) : "-",
                            hasproperty(params,:nxt_stn) ? _v(params.nxt_stn) : "-"),
        ("Terminal",        _v(params.arr_term),                            _v(params.dep_term)),
        ("Body Type",       params.arr_body == ' ' ? "-" : string(params.arr_body),
                            params.dep_body == ' ' ? "-" : string(params.dep_body)),
        ("Aircraft Type",   _v(params.arr_acft_type),                       _v(params.dep_acft_type)),
        ("Country",         _v(params.prv_country),                         _v(params.nxt_country)),
        ("State",           _v(params.prv_state),                           _v(params.nxt_state)),
    ]
    # Compute column widths
    w1 = maximum(length(f[1]) for f in fields)
    w2 = maximum(length(f[2]) for f in fields)
    w3 = maximum(length(f[3]) for f in fields)
    lines = String[]
    for (i, (label, arr, dep)) in enumerate(fields)
        line = "    $(rpad(label, w1))  $(rpad(arr, w2))  $(dep)"
        push!(lines, line)
        if i == 1
            push!(lines, "    $(repeat("─", w1))  $(repeat("─", w2))  $(repeat("─", w3))")
        end
    end
    join(lines, "\n")
end

function _format_mct_detail(rec::MCTRecord, trace::MCTTrace, params::NamedTuple;
                            lookup_mode::Symbol=:marketing,
                            cnx_stn_info::Union{Nothing,StationRecord}=nothing)::String
    _v(x) = let s = strip(string(x)); isempty(s) ? "-" : s end
    _vc(c::Char) = c == ' ' ? "-" : string(c)
    _flt(x) = Int(x) == 0 ? "-" : string(Int(x))
    _mk(spec, mct_val, leg_val) = !spec ? "-" : (strip(mct_val) == strip(leg_val) ? "✓" : "✗")

    # Compute effective carriers used in this lookup mode.
    # For NN: operating carriers, no CS flags.
    # For YN: marketing dep + operating arr, dep CS only.
    # For NY: operating dep + marketing arr, arr CS only.
    # For YY: marketing carriers, both CS flags.
    arr_is_cs = params.arr_is_codeshare
    dep_is_cs = params.dep_is_codeshare
    eff_arr_carrier = (lookup_mode in (:operating, :dep_cs)) && arr_is_cs ? _v(params.arr_op_carrier) : _v(trace.arr_carrier)
    eff_dep_carrier = (lookup_mode in (:operating, :arr_cs)) && dep_is_cs ? _v(params.dep_op_carrier) : _v(trace.dep_carrier)
    eff_arr_cs = lookup_mode in (:marketing, :arr_cs) ? (arr_is_cs ? "Y" : "N") : "N"
    eff_dep_cs = lookup_mode in (:marketing, :dep_cs) ? (dep_is_cs ? "Y" : "N") : "N"
    eff_arr_op = lookup_mode in (:marketing, :arr_cs) && arr_is_cs ? _v(trace.arr_op_carrier) : "-"
    eff_dep_op = lookup_mode in (:marketing, :dep_cs) && dep_is_cs ? _v(trace.dep_op_carrier) : "-"

    # For carrier match: if the lookup used operating carrier but the MCT record
    # doesn't specify a carrier, it's still a wildcard match. If specified, compare
    # against the effective carrier used in this lookup.
    _mk_carrier(bit, mct_val, eff_val) = begin
        (rec.specified & bit) == 0 && return "-"
        strip(mct_val) == strip(eff_val) ? "✓" : "✗"
    end

    R = NTuple{7,String}
    rows = R[]

    # ── Station (arr_station / dep_station — may differ for multi-airport cities) ──
    push!(rows, ("Station",
        _v(params.arr_station), _v(trace.arr_station),  "-",
        _v(params.dep_station), _v(trace.dep_station),  "-"))

    # ── Carrier ──
    push!(rows, ("Carrier",
        _v(params.arr_carrier), _v(rec.arr_carrier),
        _mk_carrier(MCT_BIT_ARR_CARRIER, _v(rec.arr_carrier), eff_arr_carrier),
        _v(params.dep_carrier), _v(rec.dep_carrier),
        _mk_carrier(MCT_BIT_DEP_CARRIER, _v(rec.dep_carrier), eff_dep_carrier)))

    # ── Flight ranges: determine which row gets the range based on lookup mode ──
    # The lookup compares against marketing or operating flight numbers depending
    # on the mode. The range belongs on the row with the flight number that was
    # actually compared.
    arr_flt_spec = (rec.specified & MCT_BIT_ARR_FLT_RNG) != 0
    dep_flt_spec = (rec.specified & MCT_BIT_DEP_FLT_RNG) != 0
    arr_flt_rng = arr_flt_spec ? "$(Int(rec.arr_flt_rng_start))-$(Int(rec.arr_flt_rng_end))" : "-"
    dep_flt_rng = dep_flt_spec ? "$(Int(rec.dep_flt_rng_start))-$(Int(rec.dep_flt_rng_end))" : "-"

    # Which flight number was actually used in this lookup for each side?
    arr_uses_op = (lookup_mode in (:operating, :dep_cs)) && arr_is_cs
    dep_uses_op = (lookup_mode in (:operating, :arr_cs)) && dep_is_cs

    # Effective flight numbers used for range comparison
    eff_arr_flt = arr_uses_op && hasproperty(params,:arr_op_flt_no) ? params.arr_op_flt_no : trace.arr_flt_no
    eff_dep_flt = dep_uses_op && hasproperty(params,:dep_op_flt_no) ? params.dep_op_flt_no : trace.dep_flt_no
    arr_flt_in = arr_flt_spec && (Int(rec.arr_flt_rng_start) <= Int(eff_arr_flt) <= Int(rec.arr_flt_rng_end))
    dep_flt_in = dep_flt_spec && (Int(rec.dep_flt_rng_start) <= Int(eff_dep_flt) <= Int(rec.dep_flt_rng_end))
    arr_flt_mk = arr_flt_spec ? (arr_flt_in ? "✓" : "✗") : "-"
    dep_flt_mk = dep_flt_spec ? (dep_flt_in ? "✓" : "✗") : "-"

    # Flight No row: show range here unless this side uses operating flt
    push!(rows, ("Flight No",
        _flt(params.arr_flt_no), arr_uses_op ? "-" : arr_flt_rng, arr_uses_op ? "-" : arr_flt_mk,
        _flt(params.dep_flt_no), dep_uses_op ? "-" : dep_flt_rng, dep_uses_op ? "-" : dep_flt_mk))

    # ── CS fields ──
    push!(rows, ("CS Indicator",
        params.arr_is_codeshare ? "Y" : "N", _vc(rec.arr_cs_ind),
        _mk((rec.specified & MCT_BIT_ARR_CS_IND)!=0, _vc(rec.arr_cs_ind), eff_arr_cs),
        params.dep_is_codeshare ? "Y" : "N", _vc(rec.dep_cs_ind),
        _mk((rec.specified & MCT_BIT_DEP_CS_IND)!=0, _vc(rec.dep_cs_ind), eff_dep_cs)))

    push!(rows, ("Op Carrier",
        _v(params.arr_op_carrier), _v(rec.arr_cs_op_carrier),
        _mk((rec.specified & MCT_BIT_ARR_CS_OP)!=0, _v(rec.arr_cs_op_carrier), eff_arr_op),
        _v(params.dep_op_carrier), _v(rec.dep_cs_op_carrier),
        _mk((rec.specified & MCT_BIT_DEP_CS_OP)!=0, _v(rec.dep_cs_op_carrier), eff_dep_op)))

    # Op Flight No row: show range here when this side uses operating flt
    # Also show the effective carrier being compared when range is present
    arr_op_flt = hasproperty(params,:arr_op_flt_no) ? _flt(params.arr_op_flt_no) : "-"
    dep_op_flt = hasproperty(params,:dep_op_flt_no) ? _flt(params.dep_op_flt_no) : "-"
    arr_op_mct = arr_uses_op ? arr_flt_rng : "-"
    dep_op_mct = dep_uses_op ? dep_flt_rng : "-"
    push!(rows, ("Op Flight No",
        arr_op_flt, arr_op_mct, arr_uses_op ? arr_flt_mk : "-",
        dep_op_flt, dep_op_mct, dep_uses_op ? dep_flt_mk : "-"))


    # ── Terminal / body / aircraft ──
    push!(rows, ("Terminal",
        _v(params.arr_term), _v(rec.arr_term),
        _mk((rec.specified & MCT_BIT_ARR_TERM)!=0, _v(rec.arr_term), _v(trace.arr_term)),
        _v(params.dep_term), _v(rec.dep_term),
        _mk((rec.specified & MCT_BIT_DEP_TERM)!=0, _v(rec.dep_term), _v(trace.dep_term))))

    push!(rows, ("Body Type",
        params.arr_body == ' ' ? "-" : string(params.arr_body), _vc(rec.arr_body),
        _mk((rec.specified & MCT_BIT_ARR_BODY)!=0, _vc(rec.arr_body), _vc(trace.arr_body)),
        params.dep_body == ' ' ? "-" : string(params.dep_body), _vc(rec.dep_body),
        _mk((rec.specified & MCT_BIT_DEP_BODY)!=0, _vc(rec.dep_body), _vc(trace.dep_body))))

    push!(rows, ("Aircraft",
        _v(params.arr_acft_type), _v(rec.arr_acft_type),
        _mk((rec.specified & MCT_BIT_ARR_ACFT_TYPE)!=0, _v(rec.arr_acft_type), _v(trace.arr_acft_type)),
        _v(params.dep_acft_type), _v(rec.dep_acft_type),
        _mk((rec.specified & MCT_BIT_DEP_ACFT_TYPE)!=0, _v(rec.dep_acft_type), _v(trace.dep_acft_type))))

    # ── Geography ──
    push!(rows, ("Prev/Next Stn",
        hasproperty(params,:prv_stn) ? _v(params.prv_stn) : "-", _v(rec.prv_stn),
        _mk((rec.specified & MCT_BIT_PRV_STN)!=0, _v(rec.prv_stn), _v(trace.prv_stn)),
        hasproperty(params,:nxt_stn) ? _v(params.nxt_stn) : "-", _v(rec.nxt_stn),
        _mk((rec.specified & MCT_BIT_NXT_STN)!=0, _v(rec.nxt_stn), _v(trace.nxt_stn))))

    push!(rows, ("Country",
        _v(params.prv_country), _v(rec.prv_country),
        _mk((rec.specified & MCT_BIT_PRV_COUNTRY)!=0, _v(rec.prv_country), _v(trace.prv_country)),
        _v(params.nxt_country), _v(rec.nxt_country),
        _mk((rec.specified & MCT_BIT_NXT_COUNTRY)!=0, _v(rec.nxt_country), _v(trace.nxt_country))))

    push!(rows, ("State",
        _v(params.prv_state), _v(rec.prv_state),
        _mk((rec.specified & MCT_BIT_PRV_STATE)!=0, _v(rec.prv_state), _v(trace.prv_state)),
        _v(params.nxt_state), _v(rec.nxt_state),
        _mk((rec.specified & MCT_BIT_NXT_STATE)!=0, _v(rec.nxt_state), _v(trace.nxt_state))))

    push!(rows, ("Region",
        _v(trace.prv_region), _v(rec.prv_region),
        _mk((rec.specified & MCT_BIT_PRV_REGION)!=0, _v(rec.prv_region), _v(trace.prv_region)),
        _v(trace.nxt_region), _v(rec.nxt_region),
        _mk((rec.specified & MCT_BIT_NXT_REGION)!=0, _v(rec.nxt_region), _v(trace.nxt_region))))

    # ── Date validity — only show dates that were explicitly set (not defaults) ──
    # Default sentinel values from MCT ingest: 1900-01-01 (eff) and 2099-12-31 (dis)
    _EFF_DEFAULT = UInt32(19000101)
    _DIS_DEFAULT = UInt32(20991231)
    eff_explicit = rec.eff_date != 0 && rec.eff_date != _EFF_DEFAULT
    dis_explicit = rec.dis_date != 0 && rec.dis_date != _DIS_DEFAULT
    any_explicit = eff_explicit || dis_explicit
    target_str = trace.target_date != 0 ? _format_packed_date(trace.target_date) : "-"
    eff_str = eff_explicit ? _format_packed_date(rec.eff_date) : "-"
    dis_str = dis_explicit ? _format_packed_date(rec.dis_date) : "-"
    date_rng = any_explicit ? "$eff_str to $dis_str" : "-"
    date_marker = if any_explicit && trace.target_date != 0
        # Compare against the full range (including defaults for the unset side)
        actual_eff = rec.eff_date != 0 ? rec.eff_date : _EFF_DEFAULT
        actual_dis = rec.dis_date != 0 ? rec.dis_date : _DIS_DEFAULT
        (actual_eff <= trace.target_date <= actual_dis) ? "✓" : "✗"
    else
        "-"
    end
    push!(rows, ("Date Range", target_str, date_rng, date_marker, "", "", ""))

    # ── Suppression geography (if record is a suppression) ──
    # Suppression geo scopes to the CONNECTION STATION's geography, not prv/nxt.
    # e.g. supp_country="CU" means no connections at stations in Cuba.
    if rec.suppressed
        supp_rgn = _v(rec.supp_region)
        supp_cty = _v(rec.supp_country)
        supp_st  = _v(rec.supp_state)
        # Connection station's own geography
        cnx_rgn = cnx_stn_info !== nothing ? _v(cnx_stn_info.region) : "-"
        cnx_cty = cnx_stn_info !== nothing ? _v(cnx_stn_info.country) : "-"
        cnx_st  = cnx_stn_info !== nothing ? _v(cnx_stn_info.state) : "-"
        _supp_mk(supp_val, cnx_val) = supp_val == "-" ? "-" : (supp_val == cnx_val ? "✓" : "✗")
        push!(rows, ("Supp Region",  cnx_rgn, supp_rgn, _supp_mk(supp_rgn, cnx_rgn), "", "", ""))
        push!(rows, ("Supp Country", cnx_cty, supp_cty, _supp_mk(supp_cty, cnx_cty), "", "", ""))
        push!(rows, ("Supp State",   cnx_st,  supp_st,  _supp_mk(supp_st, cnx_st),   "", "", ""))
    end

    # ── Format table ──
    header = ("Field", "Arr Leg", "Arr MCT", "Arr?", "Dep Leg", "Dep MCT", "Dep?")::R
    all_rows = R[header; rows...]
    ncols = 7
    widths = ntuple(c -> maximum(length(r[c]) for r in all_rows), ncols)

    lines = String[]
    for (i, r) in enumerate(all_rows)
        line = "    " * join([rpad(r[c], widths[c]) for c in 1:ncols], "  ")
        push!(lines, line)
        if i == 1
            push!(lines, "    " * join([repeat("─", widths[c]) for c in 1:ncols], "  "))
        end
    end

    # ── MCT metadata footer ──
    push!(lines, "")
    push!(lines, "    Time: $(Int(rec.time)) min   Spec: 0x$(string(rec.specificity, base=16))   Serial: $(Int(rec.record_serial))")
    push!(lines, "    Station Std: $(rec.station_standard ? "YES" : "NO")   Suppressed: $(rec.suppressed ? "YES" : "NO")")
    supp_parts = String[]
    !isempty(strip(String(rec.supp_region)))  && push!(supp_parts, "region=$(String(rec.supp_region))")
    !isempty(strip(String(rec.supp_country))) && push!(supp_parts, "country=$(String(rec.supp_country))")
    !isempty(strip(String(rec.supp_state)))   && push!(supp_parts, "state=$(String(rec.supp_state))")
    !isempty(supp_parts) && push!(lines, "    Supp Geo: $(join(supp_parts, ", "))")
    join(lines, "\n")
end

# ── Display: detail views (PlainStyle) ────────────────────────────────────────

function _print_legs_detail(io::IO, params::NamedTuple, ::PlainStyle)
    stn = String(params.arr_station)
    println(io, "\n  Connecting Legs at $stn:")
    println(io, _format_legs_detail(params))
    println(io, "")
end

function _find_record_by_id(trace::MCTTrace, mct_id::Int32)::Union{MCTRecord, Nothing}
    for c in trace.candidates
        c.record.mct_id == mct_id && return c.record
    end
    nothing
end

# Resolve a codeshare mode keyword to (label, MCTResult, lookup_mode).
# When only one leg is codeshare, the marketing lookup covers the YN/NY
# partition, so "x yn" or "x ny" should route to marketing_result with
# the correct lookup_mode rather than showing "no result."
function _resolve_cs_mode(mode::AbstractString, trace::MCTTrace, params::NamedTuple)
    m = lowercase(mode)
    m in ("yy", "mkt") && return ("YY (marketing)", trace.marketing_result, :marketing)
    m in ("nn", "op")  && return ("NN (operating)",  trace.operating_result,  :operating)
    if m == "yn"
        # YN = arr is CS. Dedicated result exists only when both are CS.
        if trace.arr_cs_result !== EMPTY_MCT_RESULT
            return ("YN (arr CS)", trace.arr_cs_result, :arr_cs)
        elseif params.arr_is_codeshare && !params.dep_is_codeshare
            # Marketing lookup effectively covered YN
            return ("YN (mkt)", trace.marketing_result, :marketing)
        end
        return ("YN (arr CS)", EMPTY_MCT_RESULT, :arr_cs)
    elseif m == "ny"
        if trace.dep_cs_result !== EMPTY_MCT_RESULT
            return ("NY (dep CS)", trace.dep_cs_result, :dep_cs)
        elseif !params.arr_is_codeshare && params.dep_is_codeshare
            return ("NY (mkt)", trace.marketing_result, :marketing)
        end
        return ("NY (dep CS)", EMPTY_MCT_RESULT, :dep_cs)
    end
    return nothing  # unknown mode
end

function _print_mct_detail(io::IO, trace::MCTTrace, params::NamedTuple, style::PlainStyle;
                           mode::AbstractString="",
                           airports::Dict{StationCode,StationRecord}=Dict{StationCode,StationRecord}())
    stn = String(params.arr_station)
    cnx_info = get(airports, params.arr_station, nothing)

    # Determine which result to show
    if !isempty(mode)
        resolved = _resolve_cs_mode(mode, trace, params)
        if resolved === nothing
            println(io, "  Unknown codeshare mode: $mode (use: yy, yn, ny, nn)")
            return
        end
        label, r, lm = resolved
        if r === EMPTY_MCT_RESULT
            println(io, "  $label: no result (lookup not performed for this connection).")
            return
        end
        if r.mct_id == Int32(0)
            println(io, "\n  $label: global default ($(Int(r.time)) min), no specific MCT record.")
            return
        end
        rec = _find_record_by_id(trace, r.mct_id)
        if rec === nothing
            println(io, "\n  $label: serial=$(Int(r.mct_id)) time=$(Int(r.time)) min (record not in trace candidates).")
            return
        end
        println(io, "\n  $label — Legs + MCT Record at $stn:")
        println(io, _format_mct_detail(rec, trace, params; lookup_mode=lm, cnx_stn_info=cnx_info))
        println(io, "")
        return
    end

    # Default: show the winning result
    r = trace.result
    if r.mct_id == Int32(0)
        println(io, "\n  No specific MCT record matched (global default).")
        println(io, "\n  Connecting Legs:")
        println(io, _format_legs_detail(params))
        println(io, "")
        return
    end
    rec = _find_record_by_id(trace, r.mct_id)
    if rec === nothing
        println(io, "\n  MCT record serial=$(Int(r.mct_id)) not found in trace candidates.")
        return
    end
    winner_mode = trace.codeshare_mode
    winner_label = winner_mode == :none ? "" : " ($(winner_mode) winner)"
    lm = winner_mode in (:operating,) ? :operating :
         winner_mode in (:dep_cs,) ? :dep_cs :
         winner_mode in (:arr_cs,) ? :arr_cs : :marketing
    println(io, "\n  Legs + MCT Record at $stn$winner_label:")
    println(io, _format_mct_detail(rec, trace, params; lookup_mode=lm, cnx_stn_info=cnx_info))
    println(io, "")
end

function _format_codeshare_table(trace::MCTTrace, params::NamedTuple)::String
    op_time = Int(trace.operating_result.time)
    arr_cr = String(params.arr_carrier)
    dep_cr = String(params.dep_carrier)
    arr_op = String(params.arr_op_carrier)
    dep_op = String(params.dep_op_carrier)
    op_arr = params.arr_is_codeshare ? arr_op : arr_cr
    op_dep = params.dep_is_codeshare ? dep_op : dep_cr

    header = "    Mode      | Arr CR | Dep CR | Time | Spec       | Floor | Winner"
    sep    = "    ----------|--------|--------|------|------------|-------|-------"
    rows = String[]

    function _cs_row(label, result, a_cr, d_cr, mode_sym)
        result === EMPTY_MCT_RESULT && return nothing
        t = Int(result.time)
        spec = "0x" * lpad(string(result.specificity, base=16), 6, '0')
        floor_ok = mode_sym === :operating ? "---" : (t >= op_time ? "YES" : "NO")
        winner = trace.codeshare_mode === mode_sym ? ">>>" : ""
        "    $(rpad(label, 10))| $(rpad(a_cr, 7))| $(rpad(d_cr, 7))| $(lpad(string(t), 4)) | $(rpad(spec, 10)) | $(rpad(floor_ok, 5)) | $(winner)"
    end

    # Label the marketing lookup based on which legs are actually codeshare:
    # both CS → YY, arr CS only → NY, dep CS only → YN, neither → (shouldn't be here)
    arr_cs = params.arr_is_codeshare
    dep_cs = params.dep_is_codeshare
    # Y/N notation: first letter = arr cs_ind, second = dep cs_ind
    mkt_label = (arr_cs && dep_cs) ? "YY (mkt)" :
                (arr_cs && !dep_cs) ? "YN (mkt)" :
                (!arr_cs && dep_cs) ? "NY (mkt)" : "-- (mkt)"
    r = _cs_row(mkt_label, trace.marketing_result, arr_cr, dep_cr, :marketing)
    r !== nothing && push!(rows, r)
    r = _cs_row("YN (arr)", trace.arr_cs_result, arr_cr, op_dep, :arr_cs)
    r !== nothing && push!(rows, r)
    r = _cs_row("NY (dep)", trace.dep_cs_result, op_arr, dep_cr, :dep_cs)
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
      l / legs        — Show all fields for both connecting legs side by side
      x / mct         — Show legs + matched MCT record in unified view
      x yy|yn|ny|nn   — Show detail for a specific codeshare lookup option
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

const _SCH_REGION = InlineString3("SCH")
const _EUR_REGION = InlineString3("EUR")

# Schengen/EUR region helpers mirroring production (_sch_eur_primary / _sch_eur_fallback)
function _sch_primary(region::InlineString3)::InlineString3
    # Default mode: sch_then_eur — prefer SCH over EUR
    region == _EUR_REGION ? _SCH_REGION : region
end
function _sch_fallback(region::InlineString3)::Union{InlineString3, Nothing}
    region == _SCH_REGION ? _EUR_REGION : (region == _EUR_REGION ? nothing : nothing)
end

function _run_traced_lookup(state::InspectorState, params::NamedTuple,
                            prv_region::InlineString3, nxt_region::InlineString3)::MCTTrace
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
        cnx_country = _cnx_geo(state, params.arr_station).country,
        cnx_state = _cnx_geo(state, params.arr_station).state,
        cnx_region = _cnx_geo(state, params.arr_station).region,
    )
end

# Look up connection station geography from the airports dict
function _cnx_geo(state::InspectorState, stn::StationCode)::@NamedTuple{country::InlineString3, state::InlineString3, region::InlineString3}
    info = get(state.airports, stn, nothing)
    info === nothing && return (country=InlineString3(""), state=InlineString3(""), region=InlineString3(""))
    (country=info.country, state=info.state, region=info.region)
end

function _run_trace(state::InspectorState, params::NamedTuple)::MCTTrace
    prv_region = InlineString3("")
    nxt_region = InlineString3("")
    prv_stn = hasproperty(params, :prv_stn) ? params.prv_stn : NO_STATION
    nxt_stn = hasproperty(params, :nxt_stn) ? params.nxt_stn : NO_STATION

    # Look up regions from the origin/destination stations
    prv_stn_info = prv_stn != NO_STATION ? get(state.airports, prv_stn, nothing) : nothing
    nxt_stn_info = nxt_stn != NO_STATION ? get(state.airports, nxt_stn, nothing) : nothing
    prv_stn_info !== nothing && (prv_region = prv_stn_info.region)
    nxt_stn_info !== nothing && (nxt_region = nxt_stn_info.region)

    # Schengen resolution (mirrors production sch_then_eur default):
    # If station has SCH region, try SCH first; if primary didn't match on
    # region bits, retry with EUR fallback.
    prv_primary = _sch_primary(prv_region)
    nxt_primary = _sch_primary(nxt_region)

    trace = _run_traced_lookup(state, params, prv_primary, nxt_primary)

    # Schengen fallback: retry with EUR if primary was SCH and didn't match on region
    prv_fb = _sch_fallback(prv_region)
    nxt_fb = _sch_fallback(nxt_region)
    if prv_fb !== nothing || nxt_fb !== nothing
        region_bits = MCT_BIT_PRV_REGION | MCT_BIT_NXT_REGION
        if (trace.result.matched_fields & region_bits) == 0
            fb_prv = prv_fb !== nothing ? prv_fb : prv_primary
            fb_nxt = nxt_fb !== nothing ? nxt_fb : nxt_primary
            fb_trace = _run_traced_lookup(state, params, fb_prv, fb_nxt)
            if fb_trace.result.specificity > trace.result.specificity
                trace = fb_trace
            end
        end
    end

    trace
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
    station_regions::Dict{StationCode,Set{InlineString3}} = Dict{StationCode,Set{InlineString3}}(),
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
        station_regions = station_regions,
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
        elseif cmd in ("l", "legs")
            _print_legs_detail(io_out, params, state.style)
        elseif cmd in ("x", "mct")
            _print_mct_detail(io_out, trace, params, state.style; airports=state.airports)
        elseif startswith(cmd, "x ") || startswith(cmd, "mct ")
            cs_mode = String(strip(startswith(cmd, "x ") ? cmd[3:end] : cmd[5:end]))
            _print_mct_detail(io_out, trace, params, state.style; mode=cs_mode, airports=state.airports)
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

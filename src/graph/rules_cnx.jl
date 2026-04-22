# src/graph/rules_cnx.jl — Connection rule chain for build_connections!
#
# Each rule receives (cp::GraphConnection, ctx) and returns an Int:
#   positive (PASS)   — connection passed this rule, continue chain
#   zero or negative  — connection failed; unique code identifies which rule
#
# ctx is typed Any here because RuntimeContext is defined in a later task.
# The fields accessed by each rule are documented in the per-rule docstrings.
#
# Rule chain (in the order `build_cnx_rules` assembles them):
#   1.  check_cnx_roundtrip  — tag round-trips (always passes)
#   2.  check_cnx_backtrack  — reject connections that revisit a previous station
#   3.  check_cnx_scope      — DOM/INTL/ALL scope filter
#   4.  check_cnx_interline  — online/codeshare/all carrier filter
#   5.  MCTRule              — minimum connection time from SSIM8 MCT lookup
#   6.  ConnectionTimeRule   — [optional] connection-time bounds from overrides
#   7.  check_cnx_opdays     — operating-day intersection non-empty
#   8.  check_cnx_suppcodes  — TRC suppression code check
#   9.  MAFTRule             — [optional, maft_enabled=true] max feasible travel time
#   10. CircuityRule         — route circuity filter
#   11. ConnectionGeoRule    — [optional] station/country/region/state allow-deny filter
#   12. check_cnx_trfrest    — traffic restriction code filter
#
# Section headers below match this definition order (including optional rules
# defined later in the file). The runtime default chain is rules 1, 2, 3, 4,
# 5, 7, 8, 9, 10, 12 — optional rules are inserted only when their configuration
# conditions trigger.

# ── Return-code constants ──────────────────────────────────────────────────────

"""
    Connection rule return codes

Each rule returns one of these `Int` constants.  Any positive value is a pass;
zero or negative is a fail with a unique diagnostic code.

- `PASS            =  1` — rule passed; continue chain
- `FAIL_ROUNDTRIP  =  0` — (unused; roundtrip rule always passes)
- `FAIL_SCOPE      = -1` — failed scope filter (DOM vs INTL)
- `FAIL_ONLINE     = -2` — interline/codeshare rejected by online-only mode
- `FAIL_CODESHARE  = -3` — interline rejected by codeshare-only mode
- `FAIL_INTERLINE  = -4` — interline blocked in current mode
- `FAIL_TIME_MIN   = -5` — cnx_time < MCT (or MCT suppression)
- `FAIL_TIME_MAX   = -6` — cnx_time > max connection time
- `FAIL_OPDAYS     = -7` — no overlapping operating days
- `FAIL_SUPPCODE   = -8` — TRC suppression code on from_leg or to_leg
- `FAIL_MAFT       = -9` — maximum-feasible-travel-time exceeded
- `FAIL_CIRCUITY   = -10` — route too circuitous
- `FAIL_TRFREST    = -11` — traffic restriction code blocks connection
- `FAIL_BACKTRACK  = -12` — connection backtracks to from_leg's departure station
- `FAIL_GEO        = -13` — connection station fails geographic allow/deny filter
"""
const PASS          = Int(1)
const FAIL_ROUNDTRIP = Int(0)
const FAIL_SCOPE    = Int(-1)
const FAIL_ONLINE   = Int(-2)
const FAIL_CODESHARE = Int(-3)
const FAIL_INTERLINE = Int(-4)
const FAIL_TIME_MIN = Int(-5)
const FAIL_TIME_MAX = Int(-6)
const FAIL_OPDAYS   = Int(-7)
const FAIL_SUPPCODE = Int(-8)
const FAIL_MAFT     = Int(-9)
const FAIL_CIRCUITY = Int(-10)
const FAIL_TRFREST  = Int(-11)
const FAIL_BACKTRACK = Int(-12)
const FAIL_GEO       = Int(-13)

# ── Shared filter helpers ────────────────────────────────────────────────────

@inline function _check_categorical(value, allow_set::Set, deny_set::Set)::Bool
    !isempty(deny_set) && value in deny_set && return false
    !isempty(allow_set) && value ∉ allow_set && return false
    return true
end

@inline function _get_trc(rec)::Char
    trc = rec.traffic_restriction_for_leg
    (isempty(trc) || trc == InlineString15(".")) && return ' '
    length(trc) <= 1 && return trc[1]
    seq = Int(rec.leg_sequence_number)
    (seq > 0 && seq <= length(trc)) ? trc[seq] : ' '
end

# ── Rule 1: Roundtrip tagger ──────────────────────────────────────────────────

"""
    `function check_cnx_roundtrip(cp::GraphConnection, ctx)::Int`
---

# Description
- Sets the `STATUS_ROUNDTRIP` bit on `cp.status` when the connection's origin
  matches its final destination (i.e. the traveller returns to their starting
  airport)
- Always returns `PASS`; this rule never rejects a connection, it only annotates

# Arguments
1. `cp::GraphConnection`: the connection to inspect and possibly annotate
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: always `PASS`
"""
function check_cnx_roundtrip(cp::GraphConnection, ctx)::Int
    from_org = ((cp.from_leg::GraphLeg).org::GraphStation).code
    to_dst = ((cp.to_leg::GraphLeg).dst::GraphStation).code
    if from_org != NO_STATION && to_dst != NO_STATION && from_org == to_dst
        cp.status |= STATUS_ROUNDTRIP
    end
    return PASS
end

# ── Rule 2: Backtrack rejection ────────────────────────────────────────────────

"""
    `function check_cnx_backtrack(cp::GraphConnection, ctx)::Int`
---

# Description
- Rejects connections where the departing leg returns to the arriving leg's
  origin station (trivial cycle / backtrack)
- Example: DEN→CPR connecting to CPR→DEN is rejected because
  from_leg.departure_station (DEN) == to_leg.arrival_station (DEN)
- This prevents pointless backtracks at the connection level rather than
  needing cycle detection during search

# Arguments
1. `cp::GraphConnection`: the connection to evaluate

# Returns
- `::Int`: `PASS` or `FAIL_BACKTRACK`
"""
function check_cnx_backtrack(cp::GraphConnection, ctx)::Int
    from_dep = (cp.from_leg::GraphLeg).record.departure_station
    to_arr = (cp.to_leg::GraphLeg).record.arrival_station
    return from_dep == to_arr ? FAIL_BACKTRACK : PASS
end

# ── Rule 3: Scope filter ───────────────────────────────────────────────────────
# (Rule 2 "Backtrack rejection" is defined just above)

"""
    `function check_cnx_scope(cp::GraphConnection, ctx)::Int`
---

# Description
- Enforces the DOM/INTL/ALL scope filter from `ctx.config.scope`
- `SCOPE_ALL` always passes
- `SCOPE_DOM` rejects connections tagged `STATUS_INTERNATIONAL`
- `SCOPE_INTL` rejects connections not tagged `STATUS_INTERNATIONAL`

# Arguments
1. `cp::GraphConnection`: the connection to evaluate
2. `ctx`: runtime context; accesses `ctx.config::SearchConfig`

# Returns
- `::Int`: `PASS` or `FAIL_SCOPE`
"""
function check_cnx_scope(cp::GraphConnection, ctx)::Int
    scope = ctx.config.scope
    scope == SCOPE_ALL && return PASS
    intl = is_international(cp.status)
    if scope == SCOPE_DOM
        return intl ? FAIL_SCOPE : PASS
    else  # SCOPE_INTL
        return intl ? PASS : FAIL_SCOPE
    end
end

# ── Rule 4: Interline filter ───────────────────────────────────────────────────

"""
    `function check_cnx_interline(cp::GraphConnection, ctx)::Int`
---

# Description
- Enforces the interline policy from `ctx.config.interline`
- `INTERLINE_ONLINE`: rejects any connection where `STATUS_INTERLINE` or
  `STATUS_CODESHARE` is set
- `INTERLINE_CODESHARE`: rejects connections where `STATUS_INTERLINE` is set
  (codeshare connections are allowed)
- `INTERLINE_ALL`: rejects non-international interline connections (domestic
  interline is blocked by default)

# Arguments
1. `cp::GraphConnection`: the connection to evaluate
2. `ctx`: runtime context; accesses `ctx.config::SearchConfig`

# Returns
- `::Int`: `PASS`, `FAIL_ONLINE`, `FAIL_CODESHARE`, or `FAIL_INTERLINE`
"""
function check_cnx_interline(cp::GraphConnection, ctx)::Int
    mode = ctx.config.interline
    # Under the user-facing bit semantics:
    #   STATUS_INTERLINE         = marketing carriers differ across the cnx
    #   STATUS_CNX_OP_THROUGH    = STATUS_INTERLINE && operating carriers match
    # Filter modes:
    #   INTERLINE_ONLINE     — reject ANY marketing change at the cnx
    #   INTERLINE_CODESHARE  — reject only "true" interline (op carriers also differ)
    #   INTERLINE_ALL        — reject only domestic true-interline; allow intl ones
    if mode == INTERLINE_ONLINE
        return is_interline(cp.status) ? FAIL_ONLINE : PASS
    elseif mode == INTERLINE_CODESHARE
        return (is_interline(cp.status) && !is_cnx_op_through(cp.status)) ?
            FAIL_CODESHARE : PASS
    else  # INTERLINE_ALL
        return (is_interline(cp.status) && !is_cnx_op_through(cp.status) &&
                !is_international(cp.status)) ? FAIL_INTERLINE : PASS
    end
end

# ── Rule 5: MCTRule (callable struct) ─────────────────────────────────────────

const _SCH = InlineString3("SCH")
const _EUR = InlineString3("EUR")

# Determine fallback region for Schengen/Europe based on mode.
# Returns nothing when no fallback applies.
@inline function _sch_eur_fallback(region::InlineString3, mode::Symbol)::Union{InlineString3, Nothing}
    if region == _SCH
        mode === :sch_then_eur && return _EUR
        mode === :eur_then_sch && return nothing  # SCH is the fallback, not primary
        return nothing  # :sch_only or :eur_only — no fallback
    elseif region == _EUR
        mode === :eur_then_sch && return _SCH
        mode === :sch_then_eur && return nothing  # EUR is the fallback, not primary
        return nothing
    end
    return nothing  # not SCH/EUR
end

# Determine primary region for Schengen/Europe based on mode.
# Swaps the region when mode says the other should be tried first.
@inline function _sch_eur_primary(region::InlineString3, mode::Symbol)::InlineString3
    if region == _SCH
        mode === :eur_then_sch && return _EUR  # prefer EUR, SCH is fallback
        mode === :eur_only && return _EUR       # force EUR
        return region  # :sch_then_eur or :sch_only — keep SCH
    elseif region == _EUR
        mode === :sch_then_eur && return _SCH  # prefer SCH, EUR is fallback
        mode === :sch_only && return _SCH       # force SCH
        return region  # :eur_then_sch or :eur_only — keep EUR
    end
    return region  # not SCH/EUR
end

"""
    `struct MCTRule`
---

# Description
- Callable struct implementing the MCT (Minimum Connecting Time) rule
- Performs a hierarchical SSIM8 cascade lookup via the embedded `MCTLookup`
- Through-flight connections (same segment, `cp.is_through == true`) bypass the
  MCT check and always pass
- Sets `cp.mct`, `cp.mxct`, `cp.cnx_time`, and `cp.mct_result` as side-effects
- Returns `FAIL_TIME_MIN` if the connection is MCT-suppressed or if actual
  connection time is below the MCT; returns `FAIL_TIME_MAX` if above `mxct`

# Fields
- `lookup::MCTLookup` — populated in-memory MCT lookup structure

# Context fields accessed
- `ctx.constraints::SearchConstraints` — for `defaults.min_mct_override` and
  `defaults.max_mct_override`
"""
struct MCTRule
    lookup::MCTLookup
end

"""
    `function (r::MCTRule)(cp::GraphConnection, ctx)::Int`
---

# Description
- Callable entry-point for `MCTRule`; invoked as `rule(cp, ctx)` in the rule chain
- Computes connection time from `to_leg.pax_dep - from_leg.pax_arr` with
  overnight wrap-around
- Resolves codeshare status for both legs: a leg is a codeshare when
  `codeshare_airline` is non-empty and differs from the marketing `airline`
- Passes full SSIM8 context to `lookup_mct`: operating carriers, codeshare
  indicators, equipment types, flight numbers, origin/destination station
  geography (country, state, region), and the connection date
- Applies `ctx.constraints.defaults.min_mct_override` if set
- Sets `cp.mct`, `cp.mxct`, `cp.cnx_time`, and `cp.mct_result`

# Arguments
1. `cp::GraphConnection`: connection to evaluate (mutated)
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints` and
   `ctx.target_date::UInt32`

# Returns
- `::Int`: `PASS`, `FAIL_TIME_MIN`, or `FAIL_TIME_MAX`
"""
# Low-level MCT lookup with explicit carrier, flight number, and region arguments.
# Called by the codeshare and Schengen resolution layers with different
# parameter combinations.
@inline function _mct_direct_lookup(
    r::MCTRule, ctx,
    arr_carrier::AirlineCode, dep_carrier::AirlineCode,
    arr_flt_no::FlightNumber, dep_flt_no::FlightNumber,
    stn_code, mct_status, from_rec, to_rec,
    arr_op_carrier, dep_op_carrier, arr_is_codeshare, dep_is_codeshare,
    prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
    prv_region::InlineString3 = prv_stn_rec.region,
    nxt_region::InlineString3 = nxt_stn_rec.region,
)::MCTResult
    lookup_mct(
        r.lookup,
        arr_carrier, dep_carrier,
        stn_code, stn_code, mct_status;
        arr_body = from_rec.body_type,
        dep_body = to_rec.body_type,
        arr_term = from_rec.arrival_terminal,
        dep_term = to_rec.departure_terminal,
        prv_stn = from_rec.departure_station,
        nxt_stn = to_rec.arrival_station,
        arr_op_carrier = arr_op_carrier,
        dep_op_carrier = dep_op_carrier,
        arr_is_codeshare = arr_is_codeshare,
        dep_is_codeshare = dep_is_codeshare,
        arr_acft_type = from_rec.aircraft_type,
        dep_acft_type = to_rec.aircraft_type,
        arr_flt_no = arr_flt_no,
        dep_flt_no = dep_flt_no,
        prv_country = prv_stn_rec.country,
        nxt_country = nxt_stn_rec.country,
        prv_state = prv_stn_rec.state,
        nxt_state = nxt_stn_rec.state,
        prv_region = prv_region,
        nxt_region = nxt_region,
        target_date = ctx.target_date,
        cnx_country = cnx_stn_rec.country,
        cnx_state = cnx_stn_rec.state,
        cnx_region = cnx_stn_rec.region,
    )
end

# Codeshare-aware MCT resolution. Per SSIM Ch. 8 (p. 398): "A marketing (Y)
# flight MCT will override an operating MCT." For codeshare flights, up to
# four lookups are performed, mirroring the four MCT codeshare-indicator
# partitions (YY, YN, NY, NN):
#
# 1. YY — Marketing lookup: marketing carriers with both codeshare flags set
# 2. YN — Dep-CS-only: marketing dep carrier + operating arr carrier, dep CS flag
# 3. NY — Arr-CS-only: operating dep carrier + marketing arr carrier, arr CS flag
# 4. NN — Operating lookup: operating carriers, no codeshare flags
#
# The mixed lookups (YN, NY) are only needed when both legs are codeshare;
# when only one leg is codeshare the non-CS side's marketing carrier already
# equals its operating carrier, so the marketing and operating lookups cover
# all four partitions.
#
# The operating (NN) result establishes the time floor. A codeshare result
# only overrides the operating MCT when ALL of:
#   1. The matched record has a carrier specified (MCT_BIT_*_CARRIER)
#   2. The matched record has a codeshare indicator set (MCT_BIT_*_CS_IND)
#   3. The codeshare time >= the operating time (SSIM Ch. 8 floor rule)
#   4. The codeshare specificity > the operating specificity
# At equal specificity, the operating result takes precedence.
#
# For non-codeshare connections, a single lookup is performed (no overhead).
@inline function _mct_codeshare_resolve(
    r::MCTRule, ctx, stn_code, mct_status, from_rec, to_rec,
    arr_op_carrier, dep_op_carrier, arr_is_codeshare, dep_is_codeshare,
    prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
    prv_region::InlineString3 = prv_stn_rec.region,
    nxt_region::InlineString3 = nxt_stn_rec.region,
)::MCTResult
    mode = ctx.config.mct_codeshare_mode  # :both, :marketing, or :operating

    # ── Operating carrier + flight number for codeshare sides ────────────
    op_arr_carrier = arr_is_codeshare ? arr_op_carrier : from_rec.carrier
    op_dep_carrier = dep_is_codeshare ? dep_op_carrier : to_rec.carrier
    op_arr_flt = arr_is_codeshare ? from_rec.operating_flight_number : from_rec.flight_number
    op_dep_flt = dep_is_codeshare ? to_rec.operating_flight_number : to_rec.flight_number

    # ── Mode: operating only ─────────────────────────────────────────────
    if mode === :operating
        return _mct_direct_lookup(
            r, ctx,
            op_arr_carrier, op_dep_carrier,
            op_arr_flt, op_dep_flt,
            stn_code, mct_status, from_rec, to_rec,
            NO_AIRLINE, NO_AIRLINE,
            false, false,
            prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
            prv_region = prv_region, nxt_region = nxt_region,
        )
    end

    # ── YY: Marketing lookup — marketing carriers + codeshare context (both sides)
    marketing_result = _mct_direct_lookup(
        r, ctx,
        from_rec.carrier, to_rec.carrier,
        from_rec.flight_number, to_rec.flight_number,
        stn_code, mct_status, from_rec, to_rec,
        arr_op_carrier, dep_op_carrier,
        arr_is_codeshare, dep_is_codeshare,
        prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
        prv_region = prv_region, nxt_region = nxt_region,
    )

    # Mode: marketing only — skip other lookups
    mode === :marketing && return marketing_result

    # If neither leg is a codeshare, no additional lookups needed
    (!arr_is_codeshare && !dep_is_codeshare) && return marketing_result

    # ── NN: Operating lookup — operating carriers, no codeshare flags.
    # Computed first: the operating MCT establishes the time floor.
    # Per SSIM Ch. 8: a marketing carrier can only request a longer MCT
    # than the operating carrier.
    operating_result = _mct_direct_lookup(
        r, ctx,
        op_arr_carrier, op_dep_carrier,
        op_arr_flt, op_dep_flt,
        stn_code, mct_status, from_rec, to_rec,
        NO_AIRLINE, NO_AIRLINE,
        false, false,
        prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
        prv_region = prv_region, nxt_region = nxt_region,
    )

    # Start with operating as baseline
    best = operating_result

    # Codeshare override check: the matched MCT record must have a carrier
    # specified AND a codeshare indicator set to qualify as a codeshare MCT.
    # Without both, it's a regular carrier MCT and cannot override the operating floor.
    _CS_IND_BITS = MCT_BIT_ARR_CS_IND | MCT_BIT_DEP_CS_IND
    _CARRIER_BITS = MCT_BIT_ARR_CARRIER | MCT_BIT_DEP_CARRIER
    @inline function _cs_overrides(cs_result::MCTResult, op_result::MCTResult)::Bool
        cs_result.specificity > op_result.specificity &&
        cs_result.time >= op_result.time &&
        (cs_result.matched_fields & _CS_IND_BITS) != 0 &&
        (cs_result.matched_fields & _CARRIER_BITS) != 0
    end

    if _cs_overrides(marketing_result, operating_result)
        best = marketing_result
    end

    # ── Mixed lookups: only needed when both legs are codeshare ───────────
    # When only one leg is CS, marketing carrier = operating carrier on the
    # non-CS side, so YY+NN already cover YN and NY.
    if arr_is_codeshare && dep_is_codeshare
        # YN: dep CS only — marketing dep carrier + operating arr carrier
        yn_result = _mct_direct_lookup(
            r, ctx,
            op_arr_carrier, to_rec.carrier,
            op_arr_flt, to_rec.flight_number,
            stn_code, mct_status, from_rec, to_rec,
            NO_AIRLINE, dep_op_carrier,
            false, dep_is_codeshare,
            prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
            prv_region = prv_region, nxt_region = nxt_region,
        )
        if _cs_overrides(yn_result, operating_result) && yn_result.specificity > best.specificity
            best = yn_result
        end

        # NY: arr CS only — operating dep carrier + marketing arr carrier
        ny_result = _mct_direct_lookup(
            r, ctx,
            from_rec.carrier, op_dep_carrier,
            from_rec.flight_number, op_dep_flt,
            stn_code, mct_status, from_rec, to_rec,
            arr_op_carrier, NO_AIRLINE,
            arr_is_codeshare, false,
            prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
            prv_region = prv_region, nxt_region = nxt_region,
        )
        if _cs_overrides(ny_result, operating_result) && ny_result.specificity > best.specificity
            best = ny_result
        end
    end

    return best
end

@inline function _mct_lookup_cached(
    r::MCTRule, ctx, stn_code, mct_status, from_rec, to_rec,
    arr_op_carrier, dep_op_carrier, arr_is_codeshare, dep_is_codeshare,
    prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
    prv_region::InlineString3 = prv_stn_rec.region,
    nxt_region::InlineString3 = nxt_stn_rec.region,
)::MCTResult
    if !ctx.config.mct_cache_enabled
        return _mct_codeshare_resolve(r, ctx, stn_code, mct_status, from_rec, to_rec,
                                      arr_op_carrier, dep_op_carrier,
                                      arr_is_codeshare, dep_is_codeshare,
                                      prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
                                      prv_region = prv_region, nxt_region = nxt_region)
    end

    cache_key = MCTCacheKey(
        from_rec.carrier, to_rec.carrier,
        stn_code, stn_code, mct_status,
        from_rec.body_type, to_rec.body_type,
        from_rec.departure_station, to_rec.arrival_station,
        from_rec.arrival_terminal, to_rec.departure_terminal,
        arr_op_carrier, dep_op_carrier,
        arr_is_codeshare, dep_is_codeshare,
        from_rec.aircraft_type, to_rec.aircraft_type,
        prv_stn_rec.country, nxt_stn_rec.country,
        prv_stn_rec.state, nxt_stn_rec.state,
        prv_region, nxt_region,
    )

    cached = get(ctx.mct_cache, cache_key, nothing)
    if cached !== nothing &&
       (cached.matched_fields & _MCT_CACHE_REVALIDATE_MASK) == 0 &&
       (cached.specificity & _MCT_CACHE_DATE_BIT) == 0
        return cached
    end

    result = _mct_codeshare_resolve(r, ctx, stn_code, mct_status, from_rec, to_rec,
                                    arr_op_carrier, dep_op_carrier,
                                    arr_is_codeshare, dep_is_codeshare,
                                    prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
                                    prv_region = prv_region, nxt_region = nxt_region)
    if cached === nothing
        ctx.mct_cache[cache_key] = result
    end
    return result
end

function (r::MCTRule)(cp::GraphConnection, ctx)::Int
    # Through-flight: same segment, no MCT needed
    cp.is_through && return PASS

    from_leg = cp.from_leg::GraphLeg
    to_leg = cp.to_leg::GraphLeg

    # Connection time in UTC — accounts for timezone differences at inter-station connections
    # dep_utc = local_dep - dep_utc_offset; arr_utc = local_arr - arr_utc_offset
    dep_utc = Int32(to_leg.record.passenger_departure_time) - Int32(to_leg.record.departure_utc_offset)
    arr_utc = Int32(from_leg.record.passenger_arrival_time) - Int32(from_leg.record.arrival_utc_offset) +
              Int32(from_leg.record.arrival_date_variation) * Int32(1440)
    cnx_time = dep_utc - arr_utc
    if cnx_time < 0
        cnx_time += Int32(1440)  # overnight wrap
    end

    # Determine MCT status from dep/arr domestic/international flags
    mct_status = _chars_to_mct_status(from_leg.record.arr_intl_dom,
                                       to_leg.record.dep_intl_dom)

    # ── Codeshare: resolve operating carrier and codeshare indicator ──────────
    # Direct InlineString comparison — no String allocation
    from_rec = from_leg.record
    to_rec = to_leg.record

    arr_is_codeshare = from_rec.operating_carrier != NO_AIRLINE &&
                       from_rec.operating_carrier != from_rec.carrier
    arr_op_carrier = arr_is_codeshare ? from_rec.operating_carrier : from_rec.carrier

    dep_is_codeshare = to_rec.operating_carrier != NO_AIRLINE &&
                       to_rec.operating_carrier != to_rec.carrier
    dep_op_carrier = dep_is_codeshare ? to_rec.operating_carrier : to_rec.carrier

    # ── Geographic context from origin/destination station records ────────────
    prv_stn_rec = (from_leg.org::GraphStation).record
    nxt_stn_rec = (to_leg.dst::GraphStation).record

    # Cascade lookup — same station for both arr and dep (intra-station connection)
    cnx_stn = cp.station::GraphStation
    stn_code = cnx_stn.code
    cnx_stn_rec = cnx_stn.record

    # ── Schengen/Europe region resolution ────────────────────────────────
    # Determine primary region codes based on mct_schengen_mode. For SCH/EUR
    # regions, the mode controls priority; non-SCH/EUR regions pass through.
    sch_mode = ctx.config.mct_schengen_mode
    prv_rgn = _sch_eur_primary(prv_stn_rec.region, sch_mode)
    nxt_rgn = _sch_eur_primary(nxt_stn_rec.region, sch_mode)

    result = _mct_lookup_cached(r, ctx, stn_code, mct_status, from_rec, to_rec,
                                arr_op_carrier, dep_op_carrier,
                                arr_is_codeshare, dep_is_codeshare,
                                prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
                                prv_region = prv_rgn, nxt_region = nxt_rgn)

    # Schengen fallback: if a fallback region exists and the primary lookup
    # didn't match on region bits, retry with the fallback region
    prv_fb = _sch_eur_fallback(prv_stn_rec.region, sch_mode)
    nxt_fb = _sch_eur_fallback(nxt_stn_rec.region, sch_mode)
    if prv_fb !== nothing || nxt_fb !== nothing
        region_bits = MCT_BIT_PRV_REGION | MCT_BIT_NXT_REGION
        if (result.matched_fields & region_bits) == 0
            fb_prv = prv_fb !== nothing ? prv_fb : prv_rgn
            fb_nxt = nxt_fb !== nothing ? nxt_fb : nxt_rgn
            fb_result = _mct_lookup_cached(r, ctx, stn_code, mct_status, from_rec, to_rec,
                                           arr_op_carrier, dep_op_carrier,
                                           arr_is_codeshare, dep_is_codeshare,
                                           prv_stn_rec, nxt_stn_rec, cnx_stn_rec;
                                           prv_region = fb_prv, nxt_region = fb_nxt)
            if fb_result.specificity > result.specificity
                result = fb_result
            end
        end
    end

    cp.mct_result = result
    cp.mct = result.time

    # ── Tier 1: MCT cascade instrumentation ──────────────────────────────────
    bs = ctx.build_stats
    bs.mct_lookups += 1
    if result.suppressed
        bs.mct_suppressions += 1
    elseif result.source == SOURCE_EXCEPTION
        bs.mct_exceptions += 1
    elseif result.source == SOURCE_STATION_STANDARD
        bs.mct_standards += 1
    elseif result.source == SOURCE_GLOBAL_DEFAULT
        bs.mct_defaults += 1
    end
    # Histogram and average for non-suppressed lookups
    if !result.suppressed
        bucket = clamp(div(Int(result.time), 10) + 1, 1, 48)
        bs.mct_time_hist[bucket] += 1
        n_nonsup = bs.mct_lookups - bs.mct_suppressions
        if n_nonsup > 0
            bs.mct_avg_time = bs.mct_avg_time * (n_nonsup - 1) / n_nonsup +
                              Float64(result.time) / n_nonsup
        end
    end

    # ── Tier 1: MCT audit logging (gated by metrics_level) ───────────────────
    if ctx.config.metrics_level == :full
        cascade_level = if result.suppressed
            UInt8(3)
        elseif result.source == SOURCE_EXCEPTION
            UInt8(1)
        elseif result.source == SOURCE_STATION_STANDARD
            UInt8(2)
        else
            UInt8(4)
        end
        row = MCTSelectionRow(
            (cp.station::GraphStation).code,
            from_rec.carrier,
            to_rec.carrier,
            mct_status,
            cascade_level,
            result.specificity,
            result.time,
            Minutes(cnx_time),
            Int16(cnx_time - Int32(result.time)),
            result.suppressed,
            false,
            result.matched_fields,
        )
        push!(ctx.mct_selections, row)
    end

    # Apply min MCT override from constraints
    if ctx.constraints.defaults.min_mct_override != NO_MINUTES
        cp.mct = max(cp.mct, ctx.constraints.defaults.min_mct_override)
    end

    cp.mxct = ctx.constraints.defaults.max_mct_override
    cp.cnx_time = Minutes(cnx_time)

    if result.suppressed
        return FAIL_TIME_MIN
    end

    if cnx_time < Int32(cp.mct)
        return FAIL_TIME_MIN
    elseif cnx_time > Int32(cp.mxct)
        return FAIL_TIME_MAX
    end

    return PASS
end

"""
    `function _chars_to_mct_status(arr::Char, dep::Char)::MCTStatus`

Convert arrival and departure domestic/international status characters to the
corresponding `MCTStatus` enum value.  Both 'D' and 'I' are the expected inputs;
any other character is treated as domestic.
"""
function _chars_to_mct_status(arr::Char, dep::Char)::MCTStatus
    if arr == 'D' && dep == 'D'
        return MCT_DD
    elseif arr == 'D' && dep == 'I'
        return MCT_DI
    elseif arr == 'I' && dep == 'D'
        return MCT_ID
    else
        return MCT_II
    end
end

# ── Rule 7: Operating-days filter ─────────────────────────────────────────────

"""
    `function check_cnx_opdays(cp::GraphConnection, ctx)::Int`
---

# Description
- Rejects connections whose DOW bits (bits 0–6 of `cp.status`) are all zero,
  meaning there are no overlapping operating days between the two legs
- A zero `DOW_MASK` intersection means the connection can never operate on any
  day of the week

# Arguments
1. `cp::GraphConnection`: the connection to evaluate
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_OPDAYS`
"""
function check_cnx_opdays(cp::GraphConnection, ctx)::Int
    op_days = cp.status & DOW_MASK
    return op_days == StatusBits(0) ? FAIL_OPDAYS : PASS
end

# ── Rule 8: TRC suppression code check ────────────────────────────────────────

"""
    `function check_cnx_suppcodes(cp::GraphConnection, ctx)::Int`
---

# Description
- Checks the TRC (Traffic Restriction Code) field for both legs and applies the
  full SSIM Appendix G connection-level suppression logic
- Uses `_get_trc` to extract the character at `leg_sequence_number` position;
  returns `' '` for empty/missing TRC fields
- Delegates per-code logic to `_trc_blocks_connection`, which covers all codes
  defined in SSIM Appendix G:

  | Codes          | Behavior                                       |
  |----------------|------------------------------------------------|
  | A, H, I, B, M, T | No connections at all — always FAIL         |
  | C              | Domestic connections only — FAIL if international |
  | N, W           | International only — FAIL if not international |
  | F, Y, E, G, X  | Online only — FAIL if interline               |
  | D, O, Q        | International online only — FAIL if not international OR interline |
  | K, V           | Any connection allowed — PASS                 |
  | blank, Z, J, P, R, S, U | Informational/ignored — PASS         |

# Arguments
1. `cp::GraphConnection`: the connection to evaluate; `cp.status` is read for
   `STATUS_INTERNATIONAL` and `STATUS_INTERLINE` bits
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_SUPPCODE`
"""
function check_cnx_suppcodes(cp::GraphConnection, ctx)::Int
    from_leg = cp.from_leg::GraphLeg
    to_leg = cp.to_leg::GraphLeg
    from_leg === to_leg && return PASS  # nonstop self-connection

    from_trc = _get_trc(from_leg.record)
    to_trc = _get_trc(to_leg.record)

    _trc_blocks_connection(from_trc, cp) && return FAIL_SUPPCODE
    _trc_blocks_connection(to_trc, cp) && return FAIL_SUPPCODE

    return PASS
end

@inline function _trc_blocks_connection(ch::Char, cp::GraphConnection)::Bool
    ch == ' ' && return false
    (ch == 'A' || ch == 'H' || ch == 'I' || ch == 'B' || ch == 'M' || ch == 'T') && return true
    ch == 'C' && return is_international(cp.status)
    (ch == 'N' || ch == 'W') && return !is_international(cp.status)
    (ch == 'F' || ch == 'Y' || ch == 'E' || ch == 'G' || ch == 'X') && return is_interline(cp.status)
    (ch == 'D' || ch == 'O' || ch == 'Q') && return !is_international(cp.status) || is_interline(cp.status)
    return false
end

# ── Rule 9: MAFTRule (callable struct) ────────────────────────────────────────

"""
    `struct MAFTRule`
---

# Description
- Callable struct implementing the Maximum Allowable Flying Time (MAFT) rule
- Rejects connections where the accumulated block time of both legs exceeds the
  MAFT derived from the combined route distance
- Round-trip connections (`STATUS_ROUNDTRIP` set) always pass
- MAFT = max(total_distance / speed × 60, 30.0) + rest_time

# Fields
- `speed::Float64` — assumed cruise speed in knots (default 400)
- `rest_time::Float64` — minimum ground rest time added to MAFT (default 240 min)
"""
struct MAFTRule
    speed::Float64      # knots
    rest_time::Float64  # minutes
end

"""
    `MAFTRule()`

Construct a `MAFTRule` with default speed (400 knots) and rest time (240 min).

Per the C reference (`CheckCnxMaxAllFlyTime`): MAFT = max(distance/400*60, 30) + 240 minutes
for a single connection.
"""
MAFTRule() = MAFTRule(400.0, 240.0)

"""
    `function (r::MAFTRule)(cp::GraphConnection, ctx)::Int`
---

# Description
- Callable entry-point for `MAFTRule`; invoked as `rule(cp, ctx)` in the rule chain
- Computes MAFT from total distance and cruise speed, then compares with the
  combined block time approximation

# Arguments
1. `cp::GraphConnection`: connection to evaluate
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_MAFT`
"""
function (r::MAFTRule)(cp::GraphConnection, ctx)::Int
    is_roundtrip(cp.status) && return PASS
    cp.from_leg === cp.to_leg && return PASS  # nonstop: MAFT is tautological

    from_l = cp.from_leg::GraphLeg
    to_l = cp.to_leg::GraphLeg

    # Actual block time in UTC
    from_rec = from_l.record
    to_rec = to_l.record
    from_block = max(Int32(0),
        (Int32(from_rec.passenger_arrival_time) - Int32(from_rec.arrival_utc_offset) + Int32(from_rec.arrival_date_variation) * Int32(1440)) -
        (Int32(from_rec.passenger_departure_time) - Int32(from_rec.departure_utc_offset)))
    to_block = max(Int32(0),
        (Int32(to_rec.passenger_arrival_time) - Int32(to_rec.arrival_utc_offset) + Int32(to_rec.arrival_date_variation) * Int32(1440)) -
        (Int32(to_rec.passenger_departure_time) - Int32(to_rec.departure_utc_offset)))
    actual_block = Float64(from_block + to_block)

    # MAFT from combined distance (pass when distance is unknown)
    total_dist = Float64(from_l.distance) + Float64(to_l.distance)
    total_dist <= 0.0 && return PASS
    maft = max((total_dist / r.speed) * 60.0, 30.0) + r.rest_time

    return actual_block <= maft ? PASS : FAIL_MAFT
end

# ── Rule 10: CircuityRule (callable struct) ──────────────────────────────────

"""
    `struct CircuityRule`
---

# Description
- Callable struct implementing the circuity filter for two-leg connections
- Rejects connections where the sum of both leg distances exceeds
  `factor × great_circle_distance(org, dst) + extra_miles`
- Extra miles are split by international status: domestic connections use
  `domestic_extra_miles`, international connections use `international_extra_miles`
- `factor` is resolved per-connection via `_resolve_circuity_params` +
  `_effective_circuity_factor` (see `src/types/constraints.jl`)
- Great-circle distances are cached in `ctx.gc_cache` (keyed by
  `(org_code, dst_code)` tuple) to avoid repeated haversine calls
- Round-trip connections and same-origin/destination pairs always pass

# Fields
- `domestic_extra_miles::Float64` — flat mileage tolerance for domestic routes (default 500.0)
- `international_extra_miles::Float64` — flat mileage tolerance for international routes (default 1000.0)

# Context fields accessed
- `ctx.constraints::SearchConstraints` — used by `_resolve_circuity_params` to pick the effective tier
- `ctx.gc_cache::Dict{Tuple{StationCode,StationCode}, Float64}` — cache of GC distances (mutated)
"""
struct CircuityRule
    domestic_extra_miles::Float64
    international_extra_miles::Float64
end

"""
    `CircuityRule()`

Construct a `CircuityRule` with default domestic extra miles (500.0) and international
extra miles (1000.0). The circuity factor is resolved per-connection at evaluation time
from `ctx.constraints` via `_resolve_circuity_params` and `_effective_circuity_factor`.
"""
CircuityRule() = CircuityRule(500.0, 1000.0)

"""
    `function (r::CircuityRule)(cp::GraphConnection, ctx)::Int`
---

# Description
- Callable entry-point for `CircuityRule`; invoked as `rule(cp, ctx)` in the
  rule chain
- Uses `_geodesic_distance(ctx.config, ...)` for GC computation when no cache entry is found

# Arguments
1. `cp::GraphConnection`: connection to evaluate
2. `ctx`: runtime context; accesses `ctx.gc_cache::Dict{UInt64, Float64}` and `ctx.config::SearchConfig`

# Returns
- `::Int`: `PASS` or `FAIL_CIRCUITY`
"""
function (r::CircuityRule)(cp::GraphConnection, ctx)::Int
    is_roundtrip(cp.status) && return PASS

    from_l = cp.from_leg::GraphLeg
    to_l = cp.to_leg::GraphLeg
    from_org = from_l.org::GraphStation
    to_dst = to_l.dst::GraphStation
    (from_org.code == NO_STATION || to_dst.code == NO_STATION) && return PASS
    from_org.code == to_dst.code && return PASS  # same O-D, never circuitous

    gc_key = (from_org.code, to_dst.code)
    gc_dist = get(ctx.gc_cache, gc_key, -1.0)
    if gc_dist < 0.0
        gc_dist = _geodesic_distance(
            ctx.config,
            from_org.record.latitude, from_org.record.longitude,
            to_dst.record.latitude, to_dst.record.longitude,
        )
        ctx.gc_cache[gc_key] = gc_dist
    end

    p = _resolve_circuity_params(ctx.constraints, from_org.code, to_dst.code)
    factor = _effective_circuity_factor(p, gc_dist)
    extra = is_international(cp.status) ? r.international_extra_miles : r.domestic_extra_miles
    route_dist = Float64(from_l.distance) + Float64(to_l.distance)
    return route_dist <= factor * gc_dist + extra ? PASS : FAIL_CIRCUITY
end

"""
    `function _haversine_distance(lat1::Float64, lng1::Float64, lat2::Float64, lng2::Float64)::Float64`

Compute the great-circle distance between two points in statute miles using the
haversine formula.  Earth mean radius is taken as 3958.8 statute miles.
"""
function _haversine_distance(
    lat1::Float64,
    lng1::Float64,
    lat2::Float64,
    lng2::Float64,
)::Float64
    R = 3958.8  # Earth mean radius in statute miles
    φ1 = deg2rad(lat1)
    φ2 = deg2rad(lat2)
    Δφ = deg2rad(lat2 - lat1)
    Δλ = deg2rad(lng2 - lng1)
    a = sin(Δφ / 2)^2 + cos(φ1) * cos(φ2) * sin(Δλ / 2)^2
    c = 2 * atan(sqrt(a), sqrt(1 - a))
    return R * c
end

"""
    `function _vincenty_distance(lat1::Float64, lng1::Float64, lat2::Float64, lng2::Float64)::Float64`

Compute the geodesic distance between two points in statute miles using the
Vincenty inverse formula on the WGS-84 ellipsoid.  Falls back to
`_haversine_distance` if the iterative solution does not converge (e.g. for
nearly-antipodal points).

WGS-84 parameters used:
- Semi-major axis `a = 3963.19` statute miles (6 378 137 m / 1609.344 m mi⁻¹)
- Flattening `f = 1/298.257 223 563`
- `b = a(1−f)`
"""
function _vincenty_distance(
    lat1::Float64,
    lng1::Float64,
    lat2::Float64,
    lng2::Float64,
)::Float64
    # WGS-84 ellipsoid parameters in statute miles
    a = 3963.19         # semi-major axis (statute miles)
    f = 1.0 / 298.257223563
    b = a * (1.0 - f)

    φ1 = deg2rad(lat1)
    φ2 = deg2rad(lat2)
    L  = deg2rad(lng2 - lng1)

    U1 = atan((1.0 - f) * tan(φ1))
    U2 = atan((1.0 - f) * tan(φ2))
    sinU1 = sin(U1);  cosU1 = cos(U1)
    sinU2 = sin(U2);  cosU2 = cos(U2)

    λ = L
    λ_prev = Inf
    sinσ = 0.0;  cosσ = 0.0;  σ = 0.0
    sinα = 0.0;  cos2α = 0.0;  cos2σm = 0.0

    for _ in 1:200
        sinλ = sin(λ);  cosλ = cos(λ)
        sinσ = sqrt((cosU2 * sinλ)^2 + (cosU1 * sinU2 - sinU1 * cosU2 * cosλ)^2)
        sinσ == 0.0 && return 0.0   # coincident points
        cosσ  = sinU1 * sinU2 + cosU1 * cosU2 * cosλ
        σ     = atan(sinσ, cosσ)
        sinα  = cosU1 * cosU2 * sinλ / sinσ
        cos2α = 1.0 - sinα^2
        cos2σm = (cos2α == 0.0) ? 0.0 : cosσ - 2.0 * sinU1 * sinU2 / cos2α
        C = f / 16.0 * cos2α * (4.0 + f * (4.0 - 3.0 * cos2α))
        λ_prev = λ
        λ = L + (1.0 - C) * f * sinα *
            (σ + C * sinσ * (cos2σm + C * cosσ * (-1.0 + 2.0 * cos2σm^2)))
        abs(λ - λ_prev) < 1e-12 && break
    end

    # Non-convergence (nearly antipodal) — fall back to haversine
    if abs(λ - λ_prev) >= 1e-12
        return _haversine_distance(lat1, lng1, lat2, lng2)
    end

    u2 = cos2α * (a^2 - b^2) / b^2
    A_coeff = 1.0 + u2 / 16384.0 * (4096.0 + u2 * (-768.0 + u2 * (320.0 - 175.0 * u2)))
    B_coeff = u2 / 1024.0 * (256.0 + u2 * (-128.0 + u2 * (74.0 - 47.0 * u2)))
    Δσ = B_coeff * sinσ *
         (cos2σm + B_coeff / 4.0 *
          (cosσ * (-1.0 + 2.0 * cos2σm^2) -
           B_coeff / 6.0 * cos2σm * (-3.0 + 4.0 * sinσ^2) * (-3.0 + 4.0 * cos2σm^2)))

    return b * A_coeff * (σ - Δσ)
end

"""
    `function _geodesic_distance(config, lat1::Float64, lng1::Float64, lat2::Float64, lng2::Float64)::Float64`

Dispatch to `_haversine_distance` or `_vincenty_distance` based on
`config.distance_formula` (`:haversine` or `:vincenty`).  Defaults to
haversine for any unrecognised symbol.
"""
function _geodesic_distance(
    config,
    lat1::Float64,
    lng1::Float64,
    lat2::Float64,
    lng2::Float64,
)::Float64
    if config.distance_formula === :vincenty
        return _vincenty_distance(lat1, lng1, lat2, lng2)
    else
        return _haversine_distance(lat1, lng1, lat2, lng2)
    end
end

# ── Rule 12: Traffic restriction code check ──────────────────────────────────

"""
    `function check_cnx_trfrest(cp::GraphConnection, ctx)::Int`
---

# Description
- Checks the TRC field for both legs for code 'A', which unconditionally
  prohibits all connecting traffic regardless of connection geography or carrier
- Uses `_get_trc` to extract the character at `leg_sequence_number` position;
  returns `' '` for empty/missing TRC fields
- Broader SSIM Appendix G connection suppression (codes B, C, D, F, H, etc.)
  is handled by `check_cnx_suppcodes` (rule 6)

# Arguments
1. `cp::GraphConnection`: the connection to evaluate
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_TRFREST`
"""
function check_cnx_trfrest(cp::GraphConnection, ctx)::Int
    from_l = cp.from_leg::GraphLeg
    to_l = cp.to_leg::GraphLeg
    from_l === to_l && return PASS

    from_trc = _get_trc(from_l.record)
    from_trc == 'A' && return FAIL_TRFREST

    to_trc = _get_trc(to_l.record)
    to_trc == 'A' && return FAIL_TRFREST

    return PASS
end

# ── Rule 6: ConnectionTimeRule (callable struct, optional) ───────────────────
# Defined here because it lives near the other callable structs; in the runtime
# chain it is inserted between MCTRule (5) and check_cnx_opdays (7) when a
# min/max_connection_time override is configured.

"""
    `struct ConnectionTimeRule`
---

# Description
- Callable struct enforcing user-configurable minimum and maximum connection time
  bounds, applied after MCTRule has enforced the SSIM8 schedule minimum
- Through-flight connections (`cp.is_through == true`) and nonstop
  self-connections (`cp.from_leg === cp.to_leg`) always pass
- `min_time` is only checked when it is not `NO_MINUTES` (i.e. `!= Int16(-1)`)

# Fields
- `min_time::Minutes` — user minimum connection time; `NO_MINUTES` disables
- `max_time::Minutes` — user maximum connection time
"""
struct ConnectionTimeRule
    min_time::Minutes
    max_time::Minutes
end

"""
    `function (r::ConnectionTimeRule)(cp::GraphConnection, ctx)::Int`
---

# Description
- Callable entry-point for `ConnectionTimeRule`
- Reads `cp.cnx_time` (already set by `MCTRule`) and compares against `min_time`
  and `max_time`

# Arguments
1. `cp::GraphConnection`: connection to evaluate
2. `ctx`: runtime context (no fields accessed)

# Returns
- `::Int`: `PASS`, `FAIL_TIME_MIN`, or `FAIL_TIME_MAX`
"""
function (r::ConnectionTimeRule)(cp::GraphConnection, ctx)::Int
    cp.from_leg === cp.to_leg && return PASS  # nonstop
    cp.is_through && return PASS
    cnx = Int32(cp.cnx_time)
    r.min_time != NO_MINUTES && cnx < Int32(r.min_time) && return FAIL_TIME_MIN
    cnx > Int32(r.max_time) && return FAIL_TIME_MAX
    return PASS
end

# ── Rule 11: ConnectionGeoRule (callable struct, optional) ──────────────────
# Inserted between CircuityRule (10) and check_cnx_trfrest (12) when any of the
# station/country/region/state allow/deny sets is non-empty.

"""
    `struct ConnectionGeoRule`
---

# Description
- Callable struct enforcing geographic allow/deny filters on the connection
  station and the origin/destination station records
- Each filter set is optional: an empty set means "no restriction"
- Deny sets are checked before allow sets; a station present in a deny set
  always fails regardless of allow sets

# Fields
- `allow_stations::Set{StationCode}` — if non-empty, station code must be in set
- `deny_stations::Set{StationCode}` — if non-empty, station code must not be in set
- `allow_countries::Set{InlineString3}` — country filter on connection station record
- `deny_countries::Set{InlineString3}`
- `allow_regions::Set{InlineString3}` — region filter on connection station record
- `deny_regions::Set{InlineString3}`
- `allow_states::Set{InlineString3}` — state/province filter on connection station record
- `deny_states::Set{InlineString3}`
"""
struct ConnectionGeoRule
    allow_stations::Set{StationCode}
    deny_stations::Set{StationCode}
    allow_countries::Set{InlineString3}
    deny_countries::Set{InlineString3}
    allow_regions::Set{InlineString3}
    deny_regions::Set{InlineString3}
    allow_states::Set{InlineString3}
    deny_states::Set{InlineString3}
end

"""
    `function (r::ConnectionGeoRule)(cp::GraphConnection, ctx)::Int`
---

# Description
- Callable entry-point for `ConnectionGeoRule`
- Checks the connection station's code, country, region, and state against
  the configured allow/deny sets via `_check_categorical`

# Arguments
1. `cp::GraphConnection`: connection to evaluate; `cp.station` must be a `GraphStation`
2. `ctx`: runtime context (no fields accessed)

# Returns
- `::Int`: `PASS` or `FAIL_GEO`
"""
function (r::ConnectionGeoRule)(cp::GraphConnection, ctx)::Int
    stn = cp.station::GraphStation
    _check_categorical(stn.code, r.allow_stations, r.deny_stations) || return FAIL_GEO
    rec = stn.record
    _check_categorical(rec.country, r.allow_countries, r.deny_countries) || return FAIL_GEO
    _check_categorical(rec.region, r.allow_regions, r.deny_regions) || return FAIL_GEO
    _check_categorical(rec.state, r.allow_states, r.deny_states) || return FAIL_GEO
    return PASS
end

# ── Rule chain assembly ────────────────────────────────────────────────────────

"""
    `function build_cnx_rules(config::SearchConfig, constraints::SearchConstraints, mct_lookup::MCTLookup)`
---

# Description
- Assembles and returns the connection rule chain as a `Tuple` of callables
- Rules are ordered for maximum short-circuit efficiency: cheap structural checks
  first, expensive MCT and geometry checks later
- `MAFTRule` and `CircuityRule` are constructed from `constraints.defaults`
  parameters; `MCTRule` embeds the provided `mct_lookup`
- `ConnectionTimeRule` is included only when non-default time bounds are configured
  (`min_connection_time != NO_MINUTES` or `max_connection_time != 480`)
- `ConnectionGeoRule` is included only when at least one geographic filter set is
  non-empty

# Arguments
1. `config::SearchConfig`: search configuration (scope, interline mode, etc.)
2. `constraints::SearchConstraints`: parameter set for MCT overrides and circuity
3. `mct_lookup::MCTLookup`: pre-materialised in-memory MCT lookup structure

# Returns
- `::Tuple`: tuple of callables in chain order (Tuple enables full specialization in the O(n²) loop)

# Examples
```julia
julia> rules = build_cnx_rules(SearchConfig(), SearchConstraints(), MCTLookup());
julia> length(rules) >= 9
true
```
"""
function build_cnx_rules(
    config::SearchConfig,
    constraints::SearchConstraints,
    mct_lookup::MCTLookup,
)
    p = constraints.defaults
    rules = Any[
        check_cnx_roundtrip,
        check_cnx_backtrack,
        check_cnx_scope,
        check_cnx_interline,
        MCTRule(mct_lookup),
    ]
    # Connection time rule (only when non-default bounds are configured)
    if p.min_connection_time != NO_MINUTES || p.max_connection_time != Minutes(480)
        push!(rules, ConnectionTimeRule(p.min_connection_time, p.max_connection_time))
    end
    push!(rules, check_cnx_opdays)
    push!(rules, check_cnx_suppcodes)
    config.maft_enabled && push!(rules, MAFTRule())
    push!(rules,
        CircuityRule(
            p.domestic_circuity_extra_miles,
            p.international_circuity_extra_miles,
        ),
    )
    # Geographic filter (only when any set is non-empty)
    if !isempty(p.allow_stations) || !isempty(p.deny_stations) ||
       !isempty(p.allow_countries) || !isempty(p.deny_countries) ||
       !isempty(p.allow_regions) || !isempty(p.deny_regions) ||
       !isempty(p.allow_states) || !isempty(p.deny_states)
        push!(rules, ConnectionGeoRule(
            p.allow_stations, p.deny_stations,
            p.allow_countries, p.deny_countries,
            p.allow_regions, p.deny_regions,
            p.allow_states, p.deny_states,
        ))
    end
    push!(rules, check_cnx_trfrest)
    return Tuple(rules)
end

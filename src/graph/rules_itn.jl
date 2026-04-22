# src/graph/rules_itn.jl — Itinerary rule chain for search_itineraries
#
# Each rule receives (itn::Itinerary, ctx) and returns an Int:
#   positive (PASS)   — itinerary passed this rule, continue chain
#   zero or negative  — itinerary failed; unique code identifies which rule
#
# ctx is typed Any here because RuntimeContext is defined in a later task.
# The fields accessed by each rule are documented in the per-rule docstrings.
#
# Rule chain (default):
#   1. check_itn_scope           — DOM/INTL/ALL scope filter
#   2. check_itn_opdays          — operating-day intersection non-empty
#   3. check_itn_circuity_range  — total route distance vs great-circle O-D (with min)
#   4. check_itn_suppcodes       — itinerary-level TRC suppression codes
#   5. check_itn_maft            — total block time vs MAFT formula with stop allowance
# Optional (added conditionally by build_itn_rules):
#   6. check_itn_elapsed_range   — total elapsed time bounds
#   7. check_itn_distance_range  — total flown distance bounds
#   8. check_itn_stops_range     — minimum stops enforcement
#   9. check_itn_flight_time     — total airborne time bounds
#  10. check_itn_layover_time    — total layover time bounds
#  11. check_itn_carriers        — marketing/operating carrier allow/deny filters
#  12. check_itn_interline_dcnx  — interline double-connect restriction
#  13. check_itn_crs_cnx         — CRS distance-based connection time limit

# ── Return-code constants ──────────────────────────────────────────────────────

"""
    Itinerary rule return codes

Each itinerary rule returns one of these `Int` constants.  Any positive value is
a pass; zero or negative is a fail with a unique diagnostic code.

- `FAIL_ITN_SCOPE         = -20` — failed scope filter (DOM vs INTL)
- `FAIL_ITN_OPDAYS        = -21` — no overlapping operating days across all legs
- `FAIL_ITN_CIRCUITY      = -22` — total route too circuitous vs great-circle O-D
- `FAIL_ITN_SUPPCODE      = -23` — TRC suppression code blocks this itinerary type
- `FAIL_ITN_MAFT          = -24` — total block time exceeds MAFT with stop allowance
- `FAIL_ITN_ELAPSED       = -25` — total elapsed time outside allowed range
- `FAIL_ITN_DISTANCE      = -26` — total flown distance outside allowed range
- `FAIL_ITN_STOPS         = -27` — number of stops below minimum
- `FAIL_ITN_FLIGHT_TIME   = -28` — total airborne (block) time outside allowed range
- `FAIL_ITN_LAYOVER       = -29` — total layover time outside allowed range
- `FAIL_ITN_CARRIER       = -30` — marketing or operating carrier not permitted
- `FAIL_ITN_INTERLINE_DCNX = -31` — interline double-connect pattern rejected
- `FAIL_ITN_CRS_CNX       = -32` — connection time exceeds CRS distance-based limit
"""
const FAIL_ITN_SCOPE         = Int(-20)
const FAIL_ITN_OPDAYS        = Int(-21)
const FAIL_ITN_CIRCUITY      = Int(-22)
const FAIL_ITN_SUPPCODE      = Int(-23)
const FAIL_ITN_MAFT          = Int(-24)
const FAIL_ITN_ELAPSED       = Int(-25)
const FAIL_ITN_DISTANCE      = Int(-26)
const FAIL_ITN_STOPS         = Int(-27)
const FAIL_ITN_FLIGHT_TIME   = Int(-28)
const FAIL_ITN_LAYOVER       = Int(-29)
const FAIL_ITN_CARRIER       = Int(-30)
const FAIL_ITN_INTERLINE_DCNX = Int(-31)
const FAIL_ITN_CRS_CNX       = Int(-32)

# ── Rule 1: Scope filter ───────────────────────────────────────────────────────

"""
    `function check_itn_scope(itn::Itinerary, ctx)::Int`
---

# Description
- Enforces the DOM/INTL/ALL scope filter from `ctx.config.scope` for the full
  itinerary
- `SCOPE_ALL` always passes
- `SCOPE_DOM` rejects itineraries tagged `STATUS_INTERNATIONAL`
- `SCOPE_INTL` rejects itineraries not tagged `STATUS_INTERNATIONAL`

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.status`
2. `ctx`: runtime context; accesses `ctx.config::SearchConfig`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_SCOPE`
"""
function check_itn_scope(itn::Itinerary, ctx)::Int
    scope = ctx.config.scope
    scope == SCOPE_ALL && return PASS
    intl = is_international(itn.status)
    scope == SCOPE_DOM && return intl ? FAIL_ITN_SCOPE : PASS
    return intl ? PASS : FAIL_ITN_SCOPE  # SCOPE_INTL
end

# ── Rule 2: Operating-days filter ─────────────────────────────────────────────

"""
    `function check_itn_opdays(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries whose DOW bits (bits 0–6 of `itn.status`) are all zero,
  meaning there are no overlapping operating days across all legs in the path
- A zero `DOW_MASK` intersection means the itinerary can never operate on any
  day of the week

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.status`
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_ITN_OPDAYS`
"""
function check_itn_opdays(itn::Itinerary, ctx)::Int
    op_days = itn.status & DOW_MASK
    return op_days == StatusBits(0) ? FAIL_ITN_OPDAYS : PASS
end

# ── Rule 3: Circuity filter ────────────────────────────────────────────────────

"""
    `@inline function _itinerary_endpoints(itn::Itinerary)::Tuple{StationCode,StationCode}`
---

# Description
- Extracts `(origin_station_code, destination_station_code)` from an itinerary's
  connections vector for use in market-override lookups
- Handles the nonstop self-connection case where `from_leg === to_leg` (the
  destination is the arrival station of that same leg)
- Returns `(NO_STATION, NO_STATION)` when `itn.connections` is empty

# Arguments
1. `itn::Itinerary`: the itinerary to inspect; accesses `itn.connections`

# Returns
- `::Tuple{StationCode,StationCode}`: `(origin, destination)` station codes
"""
@inline function _itinerary_endpoints(itn::Itinerary)::Tuple{StationCode,StationCode}
    isempty(itn.connections) && return (NO_STATION, NO_STATION)
    first_cp = itn.connections[1]
    last_cp = itn.connections[end]
    origin = (first_cp.from_leg::GraphLeg).record.departure_station
    dest_leg = (last_cp.to_leg === last_cp.from_leg ?
        last_cp.from_leg : last_cp.to_leg)::GraphLeg
    destination = dest_leg.record.arrival_station
    return (origin, destination)
end

"""
    `function check_itn_circuity_range(itn::Itinerary, ctx)::Int`
---

# Description
- Checks whether the total flown distance of a 2+ stop itinerary is within an
  acceptable ratio of the great-circle origin-to-destination distance
- Nonstop (`num_stops=0`) and 1-stop (`num_stops=1`) itineraries are skipped —
  the connection-level `CircuityRule` already handles single-leg circuity
- Skips the check when the market distance is zero (no coordinates available)
- Resolves the effective `ParameterSet` via `_resolve_circuity_params` to apply
  any per-market override; carrier is ignored (circuity is a geographic property)
- The effective factor is `min(tier_factor, p.max_circuity)` via
  `_effective_circuity_factor`; `max_circuity=Inf` (default) means tier wins
- Flat tolerance for the upper bound is `domestic_circuity_extra_miles` for
  domestic itineraries and `international_circuity_extra_miles` for international,
  selected via `itn.status`
- `min_circuity` floor check is applied when `p.min_circuity > 0`

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.num_stops`,
   `itn.total_distance`, `itn.market_distance`, `itn.connections`, and `itn.status`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_CIRCUITY`
"""
function check_itn_circuity_range(itn::Itinerary, ctx)::Int
    # Nonstop and 1-stop are handled by the connection-level CircuityRule
    # (when `circuity_check_scope` includes `:connection`).
    itn.num_stops < Int16(2) && return PASS

    market_dist = Float64(itn.market_distance)
    market_dist > 0.0 || return PASS

    origin_code, dest_code = _itinerary_endpoints(itn)
    p = _resolve_circuity_params(ctx.constraints, origin_code, dest_code)
    factor = _effective_circuity_factor(p, market_dist)

    extra = is_international(itn.status) ?
        p.international_circuity_extra_miles :
        p.domestic_circuity_extra_miles
    max_dist = factor * market_dist + extra
    Float64(itn.total_distance) <= max_dist || return FAIL_ITN_CIRCUITY

    if p.min_circuity > 0.0
        circ_ratio = Float64(itn.total_distance) / market_dist
        circ_ratio < p.min_circuity && return FAIL_ITN_CIRCUITY
    end

    return PASS
end

# ── Rule 4: TRC suppression code check ────────────────────────────────────────

"""
    `function check_itn_suppcodes(itn::Itinerary, ctx)::Int`
---

# Description
- Evaluates each leg's TRC code in the context of the full itinerary and
  returns `FAIL_ITN_SUPPCODE` when the code prohibits this itinerary type
- Uses `_get_trc(record)` to extract the applicable TRC character (handles
  both SSIM indexed and NewSSIM single-char formats); legs with no code (`' '`)
  are skipped
- Code semantics (ns = num_stops, intl = international, inter = interline):
  - `I` — always fail
  - `A` — fail if nonstop (ns == 0)
  - `B` — fail if not nonstop (connecting traffic not allowed)
  - `C` — fail if international itinerary
  - `G`, `L`, `T`, `X`, `Y` — fail if nonstop or interline
  - `K`, `S`, `V` — fail if nonstop
  - `M`, `O`, `Q` — fail if nonstop, not international, or interline
  - `N`, `U`, `W` — fail if nonstop or not international
  - `Z`, `J`, `P`, `R`, `H` — pass (informational or ignored)

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.connections`,
   `itn.num_stops`, and `itn.status`
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_ITN_SUPPCODE`
"""
function check_itn_suppcodes(itn::Itinerary, ctx)::Int
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        ch = _get_trc(from_l.record)
        ch == ' ' && continue

        ns = itn.num_stops
        intl = is_international(itn.status)
        inter = is_interline(itn.status)

        # I — always fail
        ch == 'I' && return FAIL_ITN_SUPPCODE
        # A — fail if nonstop
        ch == 'A' && ns == Int16(0) && return FAIL_ITN_SUPPCODE
        # B — fail if not nonstop (connecting traffic not allowed)
        ch == 'B' && ns != Int16(0) && return FAIL_ITN_SUPPCODE
        # C — fail if international itinerary
        ch == 'C' && intl && return FAIL_ITN_SUPPCODE
        # G — fail if nonstop or interline
        ch == 'G' && (ns == Int16(0) || inter) && return FAIL_ITN_SUPPCODE
        # K, S, V — fail if nonstop
        (ch == 'K' || ch == 'S' || ch == 'V') && ns == Int16(0) && return FAIL_ITN_SUPPCODE
        # L — fail if nonstop or interline
        ch == 'L' && (ns == Int16(0) || inter) && return FAIL_ITN_SUPPCODE
        # M — fail if nonstop or not international or interline
        ch == 'M' && (ns == Int16(0) || !intl || inter) && return FAIL_ITN_SUPPCODE
        # N, W — fail if nonstop or not international
        (ch == 'N' || ch == 'W') && (ns == Int16(0) || !intl) && return FAIL_ITN_SUPPCODE
        # O, Q — fail if nonstop or not international or interline
        (ch == 'O' || ch == 'Q') && (ns == Int16(0) || !intl || inter) && return FAIL_ITN_SUPPCODE
        # T — fail if nonstop or interline
        ch == 'T' && (ns == Int16(0) || inter) && return FAIL_ITN_SUPPCODE
        # U — fail if nonstop or not international
        ch == 'U' && (ns == Int16(0) || !intl) && return FAIL_ITN_SUPPCODE
        # X, Y — fail if nonstop or interline
        (ch == 'X' || ch == 'Y') && (ns == Int16(0) || inter) && return FAIL_ITN_SUPPCODE
        # Z, J, P, R, H — pass (informational or ignored)
    end
    return PASS
end

# ── Rule 5: MAFT (Maximum Feasible Travel Time) filter ────────────────────────
# Block-time helper `_leg_utc_block` lives in src/graph/time_helpers.jl —
# previously this file had its own `_leg_block_time` with a silent
# max(0, ...) clamp that hid the LH-overnight-without-arr_date_var bug.
# All call sites now use the shared `_leg_utc_block` which infers +1 day
# rollover when the data is missing.
const _leg_block_time = _leg_utc_block

"""
    `function check_itn_maft(itn::Itinerary, ctx)::Int`
---

# Description
- Validates that the total actual block time of the itinerary does not exceed
  the Maximum Feasible Travel Time (MAFT) formula per the C reference
  (`CheckCnxMaxAllFlyTime`)
- Skip conditions: no connections, market distance zero, nonstop (`num_stops < 1`),
  or roundtrip
- MAFT formula:
  - `base = max(gc_dist / 400.0 × 60, 30.0)` (where `gc_dist` is in NM/statute miles)
  - `stop_allowance`: 240 min for 1-stop, 360 min for 2+ stops
  - `taxi`: 30 min (15 in + 15 out)
  - `maft = base + stop_allowance + taxi`
- Block time is computed from actual UTC-adjusted leg times via `_leg_block_time`,
  deduplicating legs that appear in multiple connections

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.market_distance`,
   `itn.num_stops`, `itn.status`, and `itn.connections`
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_ITN_MAFT`
"""
function check_itn_maft(itn::Itinerary, ctx)::Int
    isempty(itn.connections) && return PASS
    itn.num_stops < Int16(1) && return PASS
    is_roundtrip(itn.status) && return PASS
    itn.market_distance <= Distance(0) && return PASS

    gc_dist = Float64(itn.market_distance)
    base_maft = max((gc_dist / 400.0) * 60.0, 30.0)
    stop_allowance = itn.num_stops == Int16(1) ? 240.0 : 360.0
    taxi = 30.0
    maft = base_maft + stop_allowance + taxi

    total_bt = 0.0
    last_leg = nothing
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        if from_l !== last_leg
            total_bt += Float64(_leg_block_time(from_l.record))
            last_leg = from_l
        end
        to_l = cp.to_leg::GraphLeg
        if !(from_l === to_l) && to_l !== last_leg
            total_bt += Float64(_leg_block_time(to_l.record))
            last_leg = to_l
        end
    end

    return total_bt <= maft ? PASS : FAIL_ITN_MAFT
end

# ── Rules 6–13: Optional range, carrier, interline, and CRS rules ─────────────

"""
    `function check_itn_elapsed_range(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries whose total elapsed time (gate-to-gate, minutes) falls
  outside the `[min_elapsed, max_elapsed]` window from `ctx.constraints.defaults`

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.elapsed_time`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_ELAPSED`
"""
function check_itn_elapsed_range(itn::Itinerary, ctx)::Int
    p = ctx.constraints.defaults
    e = Int32(itn.elapsed_time)
    e < p.min_elapsed && return FAIL_ITN_ELAPSED
    e > p.max_elapsed && return FAIL_ITN_ELAPSED
    return PASS
end

"""
    `function check_itn_distance_range(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries whose total flown distance falls outside the
  `[min_total_distance, max_total_distance]` window from `ctx.constraints.defaults`

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.total_distance`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_DISTANCE`
"""
function check_itn_distance_range(itn::Itinerary, ctx)::Int
    p = ctx.constraints.defaults
    d = itn.total_distance
    d < p.min_total_distance && return FAIL_ITN_DISTANCE
    d > p.max_total_distance && return FAIL_ITN_DISTANCE
    return PASS
end

"""
    `function check_itn_stops_range(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries that have fewer intermediate stops than `min_stops`
- The maximum stops bound is enforced upstream by the DFS depth limit and is
  not rechecked here

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.num_stops`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_STOPS`
"""
function check_itn_stops_range(itn::Itinerary, ctx)::Int
    p = ctx.constraints.defaults
    itn.num_stops < p.min_stops && return FAIL_ITN_STOPS
    return PASS  # max_stops enforced by DFS depth limit
end

"""
    `function check_itn_flight_time(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries whose total airborne (block) time falls outside the
  `[min_flight_time, max_flight_time]` window from `ctx.constraints.defaults`
- Block time is computed from UTC-adjusted leg times via `_leg_block_time`,
  deduplicating legs that appear in multiple connections

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.connections`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_FLIGHT_TIME`
"""
function check_itn_flight_time(itn::Itinerary, ctx)::Int
    p = ctx.constraints.defaults
    total_bt = Int32(0)
    last_leg = nothing
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        if from_l !== last_leg
            total_bt += _leg_block_time(from_l.record)
            last_leg = from_l
        end
        to_l = cp.to_leg::GraphLeg
        if !(from_l === to_l) && to_l !== last_leg
            total_bt += _leg_block_time(to_l.record)
            last_leg = to_l
        end
    end
    total_bt < p.min_flight_time && return FAIL_ITN_FLIGHT_TIME
    total_bt > p.max_flight_time && return FAIL_ITN_FLIGHT_TIME
    return PASS
end

"""
    `function check_itn_layover_time(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries whose total layover time (elapsed minus block time) falls
  outside the `[min_layover_time, max_layover_time]` window
- Block time is computed from UTC-adjusted leg times via `_leg_block_time`,
  deduplicating legs that appear in multiple connections
- Layover is clamped at zero to avoid negative values from UTC rounding

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.connections`
   and `itn.elapsed_time`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_LAYOVER`
"""
function check_itn_layover_time(itn::Itinerary, ctx)::Int
    p = ctx.constraints.defaults
    total_bt = Int32(0)
    last_leg = nothing
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        if from_l !== last_leg
            total_bt += _leg_block_time(from_l.record)
            last_leg = from_l
        end
        to_l = cp.to_leg::GraphLeg
        if !(from_l === to_l) && to_l !== last_leg
            total_bt += _leg_block_time(to_l.record)
            last_leg = to_l
        end
    end
    layover = max(Int32(0), Int32(itn.elapsed_time) - total_bt)
    layover < p.min_layover_time && return FAIL_ITN_LAYOVER
    layover > p.max_layover_time && return FAIL_ITN_LAYOVER
    return PASS
end

"""
    `function check_itn_carriers(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects itineraries containing a marketing carrier not in `allow_carriers`,
  in `deny_carriers`, an operating carrier not in `allow_operating_carriers`,
  or in `deny_operating_carriers`
- Only operating carriers that differ from the marketing carrier and are not
  `NO_AIRLINE` are checked against the operating-carrier sets
- Legs are deduplicated by identity across connections

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.connections`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_CARRIER`
"""
function check_itn_carriers(itn::Itinerary, ctx)::Int
    p = ctx.constraints.defaults
    last_leg = nothing
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        if from_l !== last_leg
            rec = from_l.record
            _check_categorical(rec.carrier, p.allow_carriers, p.deny_carriers) || return FAIL_ITN_CARRIER
            if rec.operating_carrier != NO_AIRLINE && rec.operating_carrier != rec.carrier
                _check_categorical(rec.operating_carrier, p.allow_operating_carriers, p.deny_operating_carriers) || return FAIL_ITN_CARRIER
            end
            last_leg = from_l
        end
        to_l = cp.to_leg::GraphLeg
        if !(from_l === to_l) && to_l !== last_leg
            rec = to_l.record
            _check_categorical(rec.carrier, p.allow_carriers, p.deny_carriers) || return FAIL_ITN_CARRIER
            if rec.operating_carrier != NO_AIRLINE && rec.operating_carrier != rec.carrier
                _check_categorical(rec.operating_carrier, p.allow_operating_carriers, p.deny_operating_carriers) || return FAIL_ITN_CARRIER
            end
            last_leg = to_l
        end
    end
    return PASS
end

"""
    `function check_itn_interline_dcnx(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects interline 2-stop itineraries whose three legs form a
  double-connect pattern that crosses the domestic/international boundary
  in a way that creates clearance/recheck conflicts:
  - `dom → intl → dom`: middle leg is international, outer legs domestic
  - `intl → dom → intl`: middle leg is domestic, outer legs international
- Only applied when `itn.num_stops == 2`, `is_interline(itn.status)` is true,
  and the itinerary has at least 3 connections

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.num_stops`,
   `itn.status`, and `itn.connections`
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_ITN_INTERLINE_DCNX`
"""
function check_itn_interline_dcnx(itn::Itinerary, ctx)::Int
    itn.num_stops != Int16(2) && return PASS
    !is_interline(itn.status) && return PASS
    length(itn.connections) < 3 && return PASS

    c1_intl = (itn.connections[1].from_leg::GraphLeg).record.arr_intl_dom == 'I'
    c2_intl = (itn.connections[2].from_leg::GraphLeg).record.arr_intl_dom == 'I'
    c3_intl = (itn.connections[3].from_leg::GraphLeg).record.arr_intl_dom == 'I'

    # dom-intl-dom or intl-dom-intl patterns are rejected
    (c1_intl && !c2_intl && c3_intl) && return FAIL_ITN_INTERLINE_DCNX
    (!c1_intl && c2_intl && !c3_intl) && return FAIL_ITN_INTERLINE_DCNX

    return PASS
end

"""
    `function check_itn_crs_cnx(itn::Itinerary, ctx)::Int`
---

# Description
- Rejects international connecting itineraries where any connection time
  exceeds MCT by more than a distance-based threshold
- "CRS" here refers to the Computer Reservation System origin of the
  threshold tables: published distance-tier caps on max connection time that
  a CRS will display in an airline's itinerary search results. Not the same
  as *CRS Elapsed Time* (gate-to-gate duration); this rule limits how long
  a passenger may sit on the ground at a connect point, not how long the
  full itinerary takes.
- Only applied to international itineraries with at least 1 stop
- Threshold: `300` minutes for routes under 1000 total miles, `480` otherwise
- Through-connections (`cp.is_through`) and self-connections (`from_leg === to_leg`)
  are exempt

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.status`,
   `itn.num_stops`, `itn.total_distance`, and `itn.connections`
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_ITN_CRS_CNX`
"""
function check_itn_crs_cnx(itn::Itinerary, ctx)::Int
    !is_international(itn.status) && return PASS
    itn.num_stops < Int16(1) && return PASS

    total_dist = Float64(itn.total_distance)
    max_ct_diff = total_dist < 1000.0 ? Int32(300) : Int32(480)

    for cp in itn.connections
        cp.from_leg === cp.to_leg && continue
        cp.is_through && continue
        cnx = Int32(cp.cnx_time)
        mct = Int32(cp.mct)
        cnx > mct + max_ct_diff && return FAIL_ITN_CRS_CNX
    end

    return PASS
end

# ── Rule chain assembly ────────────────────────────────────────────────────────

"""
    `function build_itn_rules(config::SearchConfig; constraints::SearchConstraints = SearchConstraints())`
---

# Description
- Assembles and returns the itinerary rule chain as a `Tuple` of callables
- The first four rules are always included: scope, opdays, circuity range,
  and suppression codes
- `check_itn_maft` is included when `config.maft_enabled` is `true`
- Additional range and filter rules are conditionally added based on the
  active `constraints.defaults` parameters — rules whose parameters are all
  at their defaults (no-op) are omitted to avoid unnecessary evaluations:
  - `check_itn_elapsed_range` — added when `min_elapsed > 0` or `max_elapsed < 1440`
  - `check_itn_distance_range` — added when bounds are non-default
  - `check_itn_stops_range` — added when `min_stops > 0`
  - `check_itn_flight_time` — added when bounds are non-default
  - `check_itn_layover_time` — added when bounds are non-default
  - `check_itn_carriers` — added when any carrier allow/deny set is non-empty
  - `check_itn_interline_dcnx` — added when `config.interline_dcnx_enabled`
  - `check_itn_crs_cnx` — added when `config.crs_cnx_enabled`
- All rules share the `(itn::Itinerary, ctx) -> Int` signature

# Arguments
1. `config::SearchConfig`: controls `maft_enabled`, `interline_dcnx_enabled`,
   `crs_cnx_enabled` toggles
2. `constraints::SearchConstraints`: provides `ParameterSet` defaults for
   conditional rule inclusion

# Returns
- `::Tuple`: variable-length tuple of callables, in chain order

# Examples
```julia
julia> rules = build_itn_rules(SearchConfig());
julia> length(rules) >= 4
true
```
"""
function build_itn_rules(config::SearchConfig; constraints::SearchConstraints = SearchConstraints())
    p = constraints.defaults
    rules = Any[
        check_itn_scope,
        check_itn_opdays,
    ]
    if config.circuity_check_scope === :itinerary || config.circuity_check_scope === :both
        push!(rules, check_itn_circuity_range)
    end
    push!(rules, check_itn_suppcodes)

    config.maft_enabled && push!(rules, check_itn_maft)

    (p.min_elapsed > Int32(0) || p.max_elapsed < Int32(1440)) &&
        push!(rules, check_itn_elapsed_range)

    (p.min_total_distance > Distance(0) || p.max_total_distance < Distance(Inf32)) &&
        push!(rules, check_itn_distance_range)

    p.min_stops > Int16(0) && push!(rules, check_itn_stops_range)

    (p.min_flight_time > Int32(0) || p.max_flight_time < Int32(9999)) &&
        push!(rules, check_itn_flight_time)

    (p.min_layover_time > Int32(0) || p.max_layover_time < Int32(9999)) &&
        push!(rules, check_itn_layover_time)

    has_carrier = !isempty(p.allow_carriers) || !isempty(p.deny_carriers) ||
                  !isempty(p.allow_operating_carriers) || !isempty(p.deny_operating_carriers)
    has_carrier && push!(rules, check_itn_carriers)

    config.interline_dcnx_enabled && push!(rules, check_itn_interline_dcnx)

    config.crs_cnx_enabled && push!(rules, check_itn_crs_cnx)

    return Tuple(rules)
end

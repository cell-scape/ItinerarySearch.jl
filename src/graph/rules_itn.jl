# src/graph/rules_itn.jl — Itinerary rule chain for search_itineraries
#
# Each rule receives (itn::Itinerary, ctx) and returns an Int:
#   positive (PASS)   — itinerary passed this rule, continue chain
#   zero or negative  — itinerary failed; unique code identifies which rule
#
# ctx is typed Any here because RuntimeContext is defined in a later task.
# The fields accessed by each rule are documented in the per-rule docstrings.
#
# Rule chain:
#   1. check_itn_scope      — DOM/INTL/ALL scope filter
#   2. check_itn_opdays     — operating-day intersection non-empty
#   3. check_itn_circuity   — total route distance vs great-circle origin→dest
#   4. check_itn_suppcodes  — itinerary-level TRC suppression code 'I'
#   5. check_itn_maft       — total block time vs MAFT formula with stop allowance

# ── Return-code constants ──────────────────────────────────────────────────────

"""
    Itinerary rule return codes

Each itinerary rule returns one of these `Int` constants.  Any positive value is
a pass; zero or negative is a fail with a unique diagnostic code.

- `FAIL_ITN_SCOPE    = -20` — failed scope filter (DOM vs INTL)
- `FAIL_ITN_OPDAYS   = -21` — no overlapping operating days across all legs
- `FAIL_ITN_CIRCUITY = -22` — total route too circuitous vs great-circle O-D
- `FAIL_ITN_SUPPCODE = -23` — TRC suppression code blocks this itinerary type
- `FAIL_ITN_MAFT     = -24` — total block time exceeds MAFT with stop allowance
"""
const FAIL_ITN_SCOPE    = Int(-20)
const FAIL_ITN_OPDAYS   = Int(-21)
const FAIL_ITN_CIRCUITY = Int(-22)
const FAIL_ITN_SUPPCODE = Int(-23)
const FAIL_ITN_MAFT     = Int(-24)

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
    `function check_itn_circuity(itn::Itinerary, ctx)::Int`
---

# Description
- Checks whether the total flown distance of the itinerary is within an
  acceptable ratio of the great-circle origin-to-destination distance
- Skips the check when the itinerary has no connections or the market distance
  is zero (no coordinates available)
- Uses `ctx.constraints.defaults.max_circuity` as the maximum allowed ratio
- Flat tolerance is `domestic_circuity_extra_miles` for domestic itineraries and
  `international_circuity_extra_miles` for international itineraries, selected via
  `itn.status`

# Arguments
1. `itn::Itinerary`: the itinerary to evaluate; accesses `itn.total_distance`,
   `itn.market_distance`, and `itn.status`
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS` or `FAIL_ITN_CIRCUITY`
"""
function check_itn_circuity(itn::Itinerary, ctx)::Int
    isempty(itn.connections) && return PASS
    itn.market_distance <= Distance(0) && return PASS
    p = ctx.constraints.defaults
    extra = is_international(itn.status) ?
        p.international_circuity_extra_miles :
        p.domestic_circuity_extra_miles
    return Float64(itn.total_distance) <= p.max_circuity * Float64(itn.market_distance) + extra ? PASS : FAIL_ITN_CIRCUITY
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

"""
    `@inline function _leg_block_time(rec)::Int32`

Compute block time in minutes for a leg record, UTC-adjusted.

Formula: `(arr_local - arr_utc_offset + arr_date_variation × 1440) - (dep_local - dep_utc_offset)`.
Returns at least 0.
"""
@inline function _leg_block_time(rec)::Int32
    max(Int32(0),
        (Int32(rec.passenger_arrival_time) - Int32(rec.arrival_utc_offset) +
         Int32(rec.arrival_date_variation) * Int32(1440)) -
        (Int32(rec.passenger_departure_time) - Int32(rec.departure_utc_offset)))
end

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

# ── Rule chain assembly ────────────────────────────────────────────────────────

"""
    `function build_itn_rules(config::SearchConfig)`
---

# Description
- Assembles and returns the canonical 5-rule itinerary rule chain
- Rules are ordered from structural checks (scope, opdays) to geometric
  checks (circuity, MAFT) with the suppression code check in between
- The returned tuple contains plain functions; all rules share the same
  `(itn::Itinerary, ctx) -> Int` signature

# Arguments
1. `config::SearchConfig`: search configuration (currently unused; reserved for
   future rule-enable/disable toggles)

# Returns
- `::Tuple`: 5-element tuple of callables, in chain order

# Examples
```julia
julia> rules = build_itn_rules(SearchConfig());
julia> length(rules)
5
```
"""
function build_itn_rules(config::SearchConfig)
    return (
        check_itn_scope,
        check_itn_opdays,
        check_itn_circuity,
        check_itn_suppcodes,
        check_itn_maft,
    )
end

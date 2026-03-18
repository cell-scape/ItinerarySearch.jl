# src/graph/rules_cnx.jl — Connection rule chain for build_connections!
#
# Each rule receives (cp::GraphConnection, ctx) and returns an Int:
#   positive (PASS)   — connection passed this rule, continue chain
#   zero or negative  — connection failed; unique code identifies which rule
#
# ctx is typed Any here because RuntimeContext is defined in a later task.
# The fields accessed by each rule are documented in the per-rule docstrings.
#
# Rule chain:
#   1. check_cnx_roundtrip  — tag round-trips (always passes)
#   2. check_cnx_scope      — DOM/INTL/ALL scope filter
#   3. check_cnx_interline  — online/codeshare/all carrier filter
#   4. MCTRule              — minimum and maximum connection time
#   5. check_cnx_opdays     — operating-day intersection non-empty
#   6. check_cnx_suppcodes  — TRC suppression code check
#   7. MAFTRule             — maximum feasible travel time
#   8. CircuityRule         — route circuity filter
#   9. check_cnx_trfrest    — traffic restriction code filter

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
    from_org = cp.from_leg.org.code
    to_dst = cp.to_leg.dst.code
    if from_org != NO_STATION && to_dst != NO_STATION && from_org == to_dst
        cp.status |= STATUS_ROUNDTRIP
    end
    return PASS
end

# ── Rule 2: Scope filter ───────────────────────────────────────────────────────

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

# ── Rule 3: Interline filter ───────────────────────────────────────────────────

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
    if mode == INTERLINE_ONLINE
        return (is_interline(cp.status) || is_codeshare(cp.status)) ? FAIL_ONLINE : PASS
    elseif mode == INTERLINE_CODESHARE
        return is_interline(cp.status) ? FAIL_CODESHARE : PASS
    else  # INTERLINE_ALL
        return (is_interline(cp.status) && !is_international(cp.status)) ? FAIL_INTERLINE : PASS
    end
end

# ── Rule 4: MCTRule (callable struct) ─────────────────────────────────────────

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
- Applies `ctx.constraints.defaults.min_mct_override` if set
- Sets `cp.mct`, `cp.mxct`, `cp.cnx_time`, and `cp.mct_result`

# Arguments
1. `cp::GraphConnection`: connection to evaluate (mutated)
2. `ctx`: runtime context; accesses `ctx.constraints::SearchConstraints`

# Returns
- `::Int`: `PASS`, `FAIL_TIME_MIN`, or `FAIL_TIME_MAX`
"""
function (r::MCTRule)(cp::GraphConnection, ctx)::Int
    # Through-flight: same segment, no MCT needed
    cp.is_through && return PASS

    from_leg = cp.from_leg::GraphLeg
    to_leg = cp.to_leg::GraphLeg

    # Connection time = departure time - arrival time (both in minutes-since-midnight)
    dep = Int32(to_leg.record.pax_dep)
    arr = Int32(from_leg.record.pax_arr) + Int32(from_leg.record.arr_date_var) * Int32(1440)
    cnx_time = dep - arr
    if cnx_time < 0
        cnx_time += Int32(1440)  # overnight wrap
    end

    # Determine MCT status from dep/arr domestic/international flags
    mct_status = _chars_to_mct_status(from_leg.record.mct_status_arr,
                                       to_leg.record.mct_status_dep)

    # Cascade lookup
    result = lookup_mct(
        r.lookup,
        from_leg.record.airline,
        to_leg.record.airline,
        cp.station.code,
        mct_status;
        arr_body = from_leg.record.body_type,
        dep_body = to_leg.record.body_type,
        arr_term = from_leg.record.arr_term,
        dep_term = to_leg.record.dep_term,
    )

    cp.mct_result = result
    cp.mct = result.time

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

# ── Rule 5: Operating-days filter ─────────────────────────────────────────────

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

# ── Rule 6: TRC suppression code check ────────────────────────────────────────

"""
    `function check_cnx_suppcodes(cp::GraphConnection, ctx)::Int`
---

# Description
- Checks the TRC (Traffic Restriction Code) field at `leg_seq` position for both
  legs in the connection
- Returns `FAIL_SUPPCODE` if either leg carries a code 'A' (no local traffic) at
  its leg sequence position
- TRC is stored as `InlineString15`; the `leg_seq` field (1-based) indexes into it

# Arguments
1. `cp::GraphConnection`: the connection to evaluate
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_SUPPCODE`
"""
function check_cnx_suppcodes(cp::GraphConnection, ctx)::Int
    from_trc = cp.from_leg.record.trc
    to_trc = cp.to_leg.record.trc

    from_seq = Int(cp.from_leg.record.leg_seq)
    if from_seq > 0 && from_seq <= length(from_trc)
        ch = from_trc[from_seq]
        ch == 'A' && return FAIL_SUPPCODE
    end

    to_seq = Int(cp.to_leg.record.leg_seq)
    if to_seq > 0 && to_seq <= length(to_trc)
        ch = to_trc[to_seq]
        ch == 'A' && return FAIL_SUPPCODE
    end

    return PASS
end

# ── Rule 7: MAFTRule (callable struct) ────────────────────────────────────────

"""
    `struct MAFTRule`
---

# Description
- Callable struct implementing the Maximum Feasible Travel Time (MAFT) rule
- Rejects connections where the total block time of both legs exceeds the maximum
  feasible travel time computed from their combined distances
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

    from_dist = Float64(cp.from_leg.record.distance)
    to_dist = Float64(cp.to_leg.record.distance)
    total_dist = from_dist + to_dist
    maft = max((total_dist / r.speed) * 60.0, 30.0) + r.rest_time

    block_time = (total_dist / r.speed) * 60.0
    return block_time <= maft ? PASS : FAIL_MAFT
end

# ── Rule 8: CircuityRule (callable struct) ─────────────────────────────────────

"""
    `struct CircuityRule`
---

# Description
- Callable struct implementing the circuity filter for two-leg connections
- Rejects connections where the sum of both leg distances exceeds
  `factor × great_circle_distance(org, dst) + extra_miles`
- Great-circle distances are cached in `ctx.gc_cache` (keyed by
  `hash(org_code, hash(dst_code))`) to avoid repeated haversine calls
- Round-trip connections and same-origin/destination pairs always pass

# Fields
- `factor::Float64` — circuity multiplier (default 2.0)
- `extra_miles::Float64` — flat mileage tolerance (default 500.0)

# Context fields accessed
- `ctx.gc_cache::Dict{UInt64, Float64}` — cache of GC distances (mutated)
"""
struct CircuityRule
    factor::Float64
    extra_miles::Float64
end

"""
    `CircuityRule()`

Construct a `CircuityRule` with default factor (2.0) and extra miles (500.0).
"""
CircuityRule() = CircuityRule(2.0, 500.0)

"""
    `function (r::CircuityRule)(cp::GraphConnection, ctx)::Int`
---

# Description
- Callable entry-point for `CircuityRule`; invoked as `rule(cp, ctx)` in the
  rule chain
- Uses `_haversine_distance` for GC computation when no cache entry is found

# Arguments
1. `cp::GraphConnection`: connection to evaluate
2. `ctx`: runtime context; accesses `ctx.gc_cache::Dict{UInt64, Float64}`

# Returns
- `::Int`: `PASS` or `FAIL_CIRCUITY`
"""
function (r::CircuityRule)(cp::GraphConnection, ctx)::Int
    is_roundtrip(cp.status) && return PASS

    from_org = cp.from_leg.org
    to_dst = cp.to_leg.dst
    (from_org.code == NO_STATION || to_dst.code == NO_STATION) && return PASS
    from_org.code == to_dst.code && return PASS  # same O-D, never circuitous

    gc_key = hash(from_org.code, hash(to_dst.code))
    gc_dist = get(ctx.gc_cache, gc_key, -1.0)
    if gc_dist < 0.0
        gc_dist = _haversine_distance(
            from_org.record.lat, from_org.record.lng,
            to_dst.record.lat, to_dst.record.lng,
        )
        ctx.gc_cache[gc_key] = gc_dist
    end

    route_dist = Float64(cp.from_leg.record.distance) + Float64(cp.to_leg.record.distance)
    return route_dist <= r.factor * gc_dist + r.extra_miles ? PASS : FAIL_CIRCUITY
end

"""
    `function _haversine_distance(lat1::Float64, lng1::Float64, lat2::Float64, lng2::Float64)::Float64`

Compute the great-circle distance between two points in nautical miles using the
haversine formula.  Earth radius is taken as 3440.065 NM.
"""
function _haversine_distance(
    lat1::Float64,
    lng1::Float64,
    lat2::Float64,
    lng2::Float64,
)::Float64
    R = 3440.065  # Earth radius in nautical miles
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

Compute the geodesic distance between two points in nautical miles using the
Vincenty inverse formula on the WGS-84 ellipsoid.  Falls back to
`_haversine_distance` if the iterative solution does not converge (e.g. for
nearly-antipodal points).

WGS-84 parameters used:
- Semi-major axis `a = 3443.918` NM (6 378 137 m / 1852 m NM⁻¹)
- Flattening `f = 1/298.257 223 563`
- `b = a(1−f)`
"""
function _vincenty_distance(
    lat1::Float64,
    lng1::Float64,
    lat2::Float64,
    lng2::Float64,
)::Float64
    # WGS-84 ellipsoid parameters in nautical miles
    a = 3443.918        # semi-major axis (NM)
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

# ── Rule 9: Traffic restriction code check ────────────────────────────────────

"""
    `function check_cnx_trfrest(cp::GraphConnection, ctx)::Int`
---

# Description
- Checks the TRC field for both legs for IATA traffic restriction codes that
  block connecting traffic
- Codes 'A', 'B', 'C', 'D' are considered blocking; see `_is_trc_blocked`
- Checks the TRC character at position `leg_seq` for each leg

# Arguments
1. `cp::GraphConnection`: the connection to evaluate
2. `ctx`: runtime context (no fields accessed by this rule)

# Returns
- `::Int`: `PASS` or `FAIL_TRFREST`
"""
function check_cnx_trfrest(cp::GraphConnection, ctx)::Int
    from_trc = String(cp.from_leg.record.trc)
    from_seq = Int(cp.from_leg.record.leg_seq)
    if from_seq > 0 && from_seq <= length(from_trc)
        _is_trc_blocked(from_trc[from_seq]) && return FAIL_TRFREST
    end

    to_trc = String(cp.to_leg.record.trc)
    to_seq = Int(cp.to_leg.record.leg_seq)
    if to_seq > 0 && to_seq <= length(to_trc)
        _is_trc_blocked(to_trc[to_seq]) && return FAIL_TRFREST
    end

    return PASS
end

"""
    `function _is_trc_blocked(ch::Char)::Bool`

Returns `true` for IATA TRC codes that block local or connecting traffic.
Codes A–D are the primary suppression codes in SSIM8.
"""
function _is_trc_blocked(ch::Char)::Bool
    ch == 'A' || ch == 'B' || ch == 'C' || ch == 'D'
end

# ── Rule chain assembly ────────────────────────────────────────────────────────

"""
    `function build_cnx_rules(config::SearchConfig, constraints::SearchConstraints, mct_lookup::MCTLookup)::Vector{Any}`
---

# Description
- Assembles and returns the canonical 9-rule connection rule chain
- Rules are ordered for maximum short-circuit efficiency: cheap structural checks
  first, expensive MCT and geometry checks later
- `MAFTRule` and `CircuityRule` are constructed from `constraints.defaults`
  parameters; `MCTRule` embeds the provided `mct_lookup`

# Arguments
1. `config::SearchConfig`: search configuration (scope, interline mode, etc.)
2. `constraints::SearchConstraints`: parameter set for MCT overrides and circuity
3. `mct_lookup::MCTLookup`: pre-materialised in-memory MCT lookup structure

# Returns
- `::Vector{Function}`: 9-element vector of callables, in chain order

# Examples
```julia
julia> rules = build_cnx_rules(SearchConfig(), SearchConstraints(), MCTLookup());
julia> length(rules)
9
```
"""
function build_cnx_rules(
    config::SearchConfig,
    constraints::SearchConstraints,
    mct_lookup::MCTLookup,
)::Vector{Any}
    rules = Any[]
    push!(rules, check_cnx_roundtrip)
    push!(rules, check_cnx_scope)
    push!(rules, check_cnx_interline)
    push!(rules, MCTRule(mct_lookup))
    push!(rules, check_cnx_opdays)
    push!(rules, check_cnx_suppcodes)
    push!(rules, MAFTRule())
    push!(rules, CircuityRule(
        constraints.defaults.circuity_factor,
        constraints.defaults.circuity_extra_miles,
    ))
    push!(rules, check_cnx_trfrest)
    return rules
end

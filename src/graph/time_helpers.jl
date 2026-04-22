# src/graph/time_helpers.jl — Shared time-arithmetic helpers for leg records.
#
# Loaded early in the include sequence (before rules_itn.jl) so every consumer
# of leg block-time can use the same implementation.  The previous design had
# this logic duplicated across three files (search.jl, rules_itn.jl,
# formats.jl), all of which independently silently clamped negative results
# to zero — masking the LH-overnight-without-arr_date_var bug for months.

"""
    `_leg_utc_block(rec)::Int32`

Compute a single leg's UTC block time in minutes from a `LegRecord` (or any
duck-typed record carrying `passenger_departure_time`,
`passenger_arrival_time`, `departure_utc_offset`, `arrival_utc_offset`,
`arrival_date_variation`).

If the source SSIM row left `arrival_date_variation` blank (parsed as 0) on
an overnight flight, the raw UTC math is negative.  In that case we infer
a +1 day rollover.  Observed in practice on non-UA carrier records in
`uaoa_ssim.new.dat` (e.g. LH 431 ORD→FRA, column 194 is blank instead of
`'1'`), where the provider doesn't populate the date-variation byte.
Explicit `arrival_date_variation = 1` or `2` still takes precedence.

Returns the actual block time in minutes (never silently clamped to 0 —
that was the previous behaviour and it hid the bug).  If somehow still
negative after inference, the result is whatever the math produces;
callers that need a non-negative invariant should clamp at the call site
with appropriate logging.
"""
function _leg_utc_block(rec)::Int32
    utc_dep = Int32(rec.passenger_departure_time) - Int32(rec.departure_utc_offset)
    utc_arr = Int32(rec.passenger_arrival_time) - Int32(rec.arrival_utc_offset) +
              Int32(rec.arrival_date_variation) * Int32(1440)
    block = utc_arr - utc_dep
    if block < 0 && rec.arrival_date_variation == 0
        block += Int32(1440)
    end
    return block
end

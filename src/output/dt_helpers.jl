# src/output/dt_helpers.jl — DateTime construction helpers for visualization
# and downstream tooling.
#
# The internal LegRecord uses Int16 minutes-since-midnight + UTC offset +
# arrival_date_variation for memory/perf reasons (see project notes on
# Minutes vs DateTime).  These helpers compose those fields back into
# absolute UTC `DateTime` values for output and visualization.
#
# Lives in src/output/ rather than src/types/ because it depends on graph
# types (`GraphLeg`) that aren't loaded until later in the include order.

"""
    `leg_departure_dt(leg::GraphLeg)::DateTime`

Return the leg's passenger departure time as an absolute UTC `DateTime`
constructed from `operating_date + passenger_departure_time -
departure_utc_offset`.

The result is in UTC.  To display in the departure airport's local time,
add `Minute(rec.departure_utc_offset)` after this call:

```julia
local_dep = leg_departure_dt(leg) + Minute(leg.record.departure_utc_offset)
```
"""
function leg_departure_dt(leg::GraphLeg)::DateTime
    rec = leg.record
    return DateTime(unpack_date(rec.operating_date)) +
           Minute(Int(rec.passenger_departure_time)) -
           Minute(Int(rec.departure_utc_offset))
end

"""
    `leg_arrival_dt(leg::GraphLeg)::DateTime`

Return the leg's passenger arrival time as an absolute UTC `DateTime`,
respecting `arrival_date_variation` (overnight flights).

Includes the same +1 day inference used in `_compute_elapsed`: when
`arrival_date_variation == 0` but the raw computed arrival sits before
the departure (impossible for a real flight), we infer a one-day
rollover.  This handles non-UA SSIM records that leave column 194
blank instead of marking overnight with `'1'` (e.g. LH 431 ORD→FRA).
"""
function leg_arrival_dt(leg::GraphLeg)::DateTime
    rec = leg.record
    base = DateTime(unpack_date(rec.operating_date))
    dep = base + Minute(Int(rec.passenger_departure_time)) -
                 Minute(Int(rec.departure_utc_offset))
    arr = base + Minute(Int(rec.passenger_arrival_time)) -
                 Minute(Int(rec.arrival_utc_offset)) +
                 Day(Int(rec.arrival_date_variation))
    if arr < dep && rec.arrival_date_variation == 0
        arr += Day(1)
    end
    return arr
end

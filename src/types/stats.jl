# src/types/stats.jl — Tier 1 instrumentation accumulator types
# All mutable structs use @kwdef so callers can construct with named keyword args.
# MCTSelectionRow is an immutable isbits record for zero-allocation logging.

"""
    mutable struct StationStats
---

# Description
- Per-station accumulator for Tier 1 connection build instrumentation
- Tracks departure/arrival counts, connection category counts, carrier/equipment sets,
  and running distance and ground-time averages
- Mutated in-place during `build_connections!` for each station; merged across workers
  via `merge_station_stats!`

# Fields
- `num_departures::Int32` — total departure legs loaded for this station
- `num_arrivals::Int32` — total arrival legs loaded for this station
- `num_connections::Int32` — accepted connections built at this station
- `num_pairs_evaluated::Int32` — total arrival×departure pairs tested
- `num_nonstops::Int32` — connections where org == dst (pass-through, loop suppressed)
- `num_through::Int32` — through-service (same flight number, different leg)
- `num_international::Int32` — at least one international leg in the pair
- `num_domestic::Int32` — both legs domestic
- `num_interline::Int32` — different operating carrier between legs
- `num_codeshare::Int32` — one leg is a codeshare of the other carrier
- `num_online::Int32` — same operating carrier, same alliance
- `unique_carriers::Set{AirlineCode}` — distinct operating carriers seen at this station
- `unique_equipment::Set{InlineString7}` — distinct equipment types seen at this station
- `total_dep_distance::Float64` — sum of departure leg distances (miles)
- `total_arr_distance::Float64` — sum of arrival leg distances (miles)
- `avg_ground_time::Float64` — running weighted average ground time (minutes)
"""
@kwdef mutable struct StationStats
    num_departures::Int32 = 0
    num_arrivals::Int32 = 0
    num_connections::Int32 = 0
    num_pairs_evaluated::Int32 = 0
    num_nonstops::Int32 = 0
    num_through::Int32 = 0
    num_international::Int32 = 0
    num_domestic::Int32 = 0
    num_interline::Int32 = 0
    num_codeshare::Int32 = 0
    num_online::Int32 = 0
    unique_carriers::Set{AirlineCode} = Set{AirlineCode}()
    unique_equipment::Set{InlineString7} = Set{InlineString7}()
    total_dep_distance::Float64 = 0.0
    total_arr_distance::Float64 = 0.0
    avg_ground_time::Float64 = 0.0
end

"""
    mutable struct BuildStats
---

# Description
- Global accumulator for a single `build_connections!` pass over the entire graph
- Tracks total graph size, MCT lookup statistics, rule pass/fail counters, and a
  10-minute-bucket histogram of MCT matched times for diagnostics
- Merged across parallel worker threads via `merge_build_stats!`

# Fields
- `total_stations::Int32` — number of stations in the graph
- `total_legs::Int32` — total leg records loaded
- `total_segments::Int32` — total segment records loaded
- `total_connections::Int32` — total accepted connections across all stations
- `total_pairs_evaluated::Int64` — total arrival×departure pairs tested globally
- `rule_pass::Vector{Int64}` — per-rule pass counts (indexed by rule position)
- `rule_fail::Vector{Int64}` — per-rule fail counts (indexed by rule position)
- `mct_lookups::Int64` — total MCT table lookups performed
- `mct_cache_hits::Int64` — MCT lookups satisfied from the L1 LRU cache
- `mct_exceptions::Int64` — lookups matched by a carrier/equipment exception row
- `mct_standards::Int64` — lookups matched by a station standard row
- `mct_defaults::Int64` — lookups that fell back to global MCT_DEFAULTS
- `mct_suppressions::Int64` — connections suppressed by an MCT suppression record
- `mct_dual_pass::Int64` — connections that passed both inbound and outbound MCT
- `mct_avg_time::Float64` — running average of matched MCT times (minutes)
- `mct_time_hist::Vector{Int64}` — 48-bucket histogram of matched MCT in 10-min steps (0–480 min)
- `build_time_ns::UInt64` — wall-clock nanoseconds for the build pass
"""
@kwdef mutable struct BuildStats
    total_stations::Int32 = 0
    total_legs::Int32 = 0
    total_segments::Int32 = 0
    total_connections::Int32 = 0
    total_pairs_evaluated::Int64 = 0
    rule_pass::Vector{Int64} = Int64[]
    rule_fail::Vector{Int64} = Int64[]
    mct_lookups::Int64 = 0
    mct_cache_hits::Int64 = 0
    mct_exceptions::Int64 = 0
    mct_standards::Int64 = 0
    mct_defaults::Int64 = 0
    mct_suppressions::Int64 = 0
    mct_dual_pass::Int64 = 0
    mct_avg_time::Float64 = 0.0
    mct_time_hist::Vector{Int64} = zeros(Int64, 48)  # 10-min buckets 0–480
    build_time_ns::UInt64 = 0
end

"""
    mutable struct SearchStats
---

# Description
- Per-search (or aggregate) accumulator for Tier 1 search instrumentation
- Tracks query counts, path counts by stop depth, rejection counts, cache
  behavior, and elapsed-time and distance histograms
- Mutated in-place during `search_itineraries` and optionally aggregated across
  concurrent search workers

# Fields
- `queries::Int32` — total search queries executed
- `paths_found::Int32` — total itineraries returned across all queries
- `paths_by_stops::Vector{Int32}` — [nonstop, 1-stop, 2-stop, 3-stop] path counts
- `paths_rejected::Int32` — candidate paths eliminated by post-search filters
- `max_depth_reached::Int32` — deepest DFS level hit during any search
- `layer1_hits::Int64` — L1 cache hits during connection traversal
- `layer1_misses::Int64` — L1 cache misses during connection traversal
- `elapsed_time_hist::Vector{Int32}` — 48-bucket histogram of query elapsed time (ms)
- `total_distance_hist::Vector{Int32}` — 40-bucket histogram of itinerary total distance
- `search_time_ns::UInt64` — total wall-clock nanoseconds spent in search
"""
@kwdef mutable struct SearchStats
    queries::Int32 = 0
    paths_found::Int32 = 0
    paths_by_stops::Vector{Int32} = Int32[0, 0, 0, 0]  # [nonstop, 1-stop, 2-stop, 3-stop]
    paths_rejected::Int32 = 0
    max_depth_reached::Int32 = 0
    layer1_hits::Int64 = 0
    layer1_misses::Int64 = 0
    elapsed_time_hist::Vector{Int32} = zeros(Int32, 48)
    total_distance_hist::Vector{Int32} = zeros(Int32, 40)
    search_time_ns::UInt64 = 0
end

"""
    struct MCTSelectionRow
---

# Description
- Immutable isbits record capturing the full MCT cascade decision for a single
  connection pair; used for structured audit logging and diagnostics
- Written once per connection evaluation when Tier 1 MCT tracing is enabled;
  collected into a `Vector{MCTSelectionRow}` for batch export

# Fields
- `station::StationCode` — the connect point airport
- `arr_carrier::AirlineCode` — operating carrier of the arriving leg
- `dep_carrier::AirlineCode` — operating carrier of the departing leg
- `mct_status::MCTStatus` — DD/DI/ID/II status of the connection
- `cascade_level::UInt8` — 1=exception, 2=standard, 3=suppression, 4=global default
- `specificity::UInt32` — composite specificity score of the matched MCT row
- `matched_time::Minutes` — MCT time (minutes) returned by the cascade
- `actual_cnx_time::Minutes` — actual connection time available (arr→dep gap)
- `margin::Int16` — `actual_cnx_time - matched_time`; negative means MCT not met
- `suppressed::Bool` — true if the connection was suppressed by an MCT suppression record
- `dual_pass::Bool` — true if both inbound and outbound MCT were satisfied
- `matched_fields::UInt32` — bitmask of which MCT row fields were non-wildcard in the match
"""
struct MCTSelectionRow
    station::StationCode
    arr_carrier::AirlineCode
    dep_carrier::AirlineCode
    mct_status::MCTStatus
    cascade_level::UInt8
    specificity::UInt32
    matched_time::Minutes
    actual_cnx_time::Minutes
    margin::Int16
    suppressed::Bool
    dual_pass::Bool
    matched_fields::UInt32
end

"""
    `function merge_build_stats!(target::BuildStats, source::BuildStats)`
---

# Description
- Accumulates all scalar counters and element-wise vector counters from `source` into `target`
- Used to reduce per-thread `BuildStats` into a single global accumulator after a parallel
  `build_connections!` pass
- Does NOT update `build_time_ns` — the caller is responsible for recording wall-clock time

# Arguments
1. `target::BuildStats`: accumulator to mutate in-place
2. `source::BuildStats`: read-only source to merge from

# Returns
- `nothing`

# Examples
```julia
julia> a = BuildStats(mct_lookups=100); b = BuildStats(mct_lookups=50);
julia> merge_build_stats!(a, b);
julia> a.mct_lookups
150
```
"""
function merge_build_stats!(target::BuildStats, source::BuildStats)
    target.total_connections += source.total_connections
    target.total_pairs_evaluated += source.total_pairs_evaluated
    target.mct_lookups += source.mct_lookups
    target.mct_cache_hits += source.mct_cache_hits
    target.mct_exceptions += source.mct_exceptions
    target.mct_standards += source.mct_standards
    target.mct_defaults += source.mct_defaults
    target.mct_suppressions += source.mct_suppressions
    target.mct_dual_pass += source.mct_dual_pass
    target.rule_pass .+= source.rule_pass
    target.rule_fail .+= source.rule_fail
    target.mct_time_hist .+= source.mct_time_hist
    return nothing
end

"""
    `function merge_station_stats!(target::StationStats, source::StationStats)`
---

# Description
- Accumulates all additive fields from `source` into `target`
- Computes a weighted average for `avg_ground_time` based on connection counts before
  updating `num_connections`, so the order of operations is correct
- Unions `unique_carriers` and `unique_equipment` sets in-place
- Safe when either or both stations have zero connections (avoids divide-by-zero)

# Arguments
1. `target::StationStats`: accumulator to mutate in-place
2. `source::StationStats`: read-only source to merge from

# Returns
- `nothing`

# Examples
```julia
julia> a = StationStats(num_connections=10, avg_ground_time=60.0);
julia> b = StationStats(num_connections=5, avg_ground_time=90.0);
julia> merge_station_stats!(a, b);
julia> a.avg_ground_time  # weighted average
70.0
```
"""
function merge_station_stats!(target::StationStats, source::StationStats)
    # Weighted average for avg_ground_time (must compute before updating num_connections)
    total_cnx = target.num_connections + source.num_connections
    if total_cnx > 0
        target.avg_ground_time = (
            target.avg_ground_time * target.num_connections +
            source.avg_ground_time * source.num_connections
        ) / total_cnx
    end
    target.num_connections += source.num_connections
    target.num_pairs_evaluated += source.num_pairs_evaluated
    target.num_nonstops += source.num_nonstops
    target.num_through += source.num_through
    target.num_international += source.num_international
    target.num_domestic += source.num_domestic
    target.num_interline += source.num_interline
    target.num_codeshare += source.num_codeshare
    target.num_online += source.num_online
    union!(target.unique_carriers, source.unique_carriers)
    union!(target.unique_equipment, source.unique_equipment)
    target.total_dep_distance += source.total_dep_distance
    target.total_arr_distance += source.total_arr_distance
    return nothing
end

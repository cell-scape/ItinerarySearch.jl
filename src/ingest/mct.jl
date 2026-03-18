# src/ingest/mct.jl — MCT ingest via DuckDB Appender

using DuckDB
using Dates

"""
    `_build_schedule_filters(store::DuckDBStore)::Tuple{Set{String}, Set{String}}`
---

# Description
- Build station and carrier filter sets from the already-ingested legs table
- Used to pre-filter MCT records so only schedule-relevant records are loaded

# Arguments
1. `store::DuckDBStore`: store with legs table already populated

# Returns
- `(stations::Set{String}, carriers::Set{String})`: sets of station codes and carrier codes
"""
function _build_schedule_filters(store::DuckDBStore)::Tuple{Set{String}, Set{String}}
    stations = Set{String}()
    carriers = Set{String}()

    result = DBInterface.execute(store.db, "SELECT DISTINCT org AS stn FROM legs UNION SELECT DISTINCT dst AS stn FROM legs")
    for row in result
        s = strip(String(row.stn))
        isempty(s) || push!(stations, s)
    end

    result = DBInterface.execute(store.db, "SELECT DISTINCT airline FROM legs")
    for row in result
        c = strip(String(row.airline))
        isempty(c) || push!(carriers, c)
    end

    (stations, carriers)
end

"""
    `ingest_mct!(store::DuckDBStore, path::String; station_filter, carrier_filter)::Nothing`
---

# Description
- Stream-parse an MCT file and load Type 2 records into the `mct` table
- Uses DuckDB Appender for constant-memory ingest
- Transparent decompression (gzip, zstd, bzip2, xz) via `open_maybe_compressed`
- Each Type 2 record produces one row; Type 1 (header) records are skipped
- `station_standard` is true when both arr_carrier, dep_carrier, and
  submitting_carrier are blank (no carrier specificity)
- `specificity` is stored as 0 and computed in a post-ingest pipeline step
- When `station_filter` is provided, MCT records for stations not in the set are skipped
- When `carrier_filter` is provided, carrier-specific MCT records referencing
  carriers not in the set are skipped (station standards are always kept)

# Arguments
1. `store::DuckDBStore`: initialized store (tables must already exist)
2. `path::String`: path to MCT file (may be compressed)

# Keyword Arguments
- `station_filter::Union{Nothing, Set{String}}=nothing`: if provided, only keep records
  where both arr_stn and dep_stn are in the set
- `carrier_filter::Union{Nothing, Set{String}}=nothing`: if provided, skip carrier-specific
  records where none of the referenced carriers appear in the set

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> ingest_mct!(store, "data/MCTIMFILUA.DAT");
julia> table_stats(store).mct
12345
```
"""
function ingest_mct!(store::DuckDBStore, path::String;
                     station_filter::Union{Nothing, Set{String}} = nothing,
                     carrier_filter::Union{Nothing, Set{String}} = nothing)::Nothing
    io = open_maybe_compressed(path)
    appender = DuckDB.Appender(store.db, "mct")
    mct_id = 0
    skipped = 0
    filtered = 0

    try
        for line in eachline(io)
            length(line) < 1 && continue
            rt = line[1]

            try
                if rt == '2' && length(line) >= 96
                    # Quick-extract station and carrier fields for filtering
                    arr_stn_raw = strip(line[2:min(4, length(line))])
                    dep_stn_raw = strip(line[11:min(13, length(line))])

                    # Station filter: skip if either station is not in the schedule
                    if station_filter !== nothing
                        if arr_stn_raw ∉ station_filter || dep_stn_raw ∉ station_filter
                            filtered += 1
                            continue
                        end
                    end

                    # Carrier filter: for carrier-specific records, skip if no
                    # referenced carrier appears in the schedule
                    if carrier_filter !== nothing
                        arr_carrier_raw = strip(line[14:min(15, length(line))])
                        dep_carrier_raw = strip(line[19:min(20, length(line))])
                        submitting_raw  = strip(line[95:min(96, length(line))])
                        is_station_std  = isempty(arr_carrier_raw) && isempty(dep_carrier_raw) && isempty(submitting_raw)

                        if !is_station_std
                            arr_ok = isempty(arr_carrier_raw) || arr_carrier_raw ∈ carrier_filter
                            dep_ok = isempty(dep_carrier_raw) || dep_carrier_raw ∈ carrier_filter
                            sub_ok = isempty(submitting_raw)  || submitting_raw  ∈ carrier_filter
                            if !arr_ok || !dep_ok || !sub_ok
                                filtered += 1
                                continue
                            end
                        end
                    end

                    mct_id += 1
                    _append_mct!(appender, mct_id, line)
                end
            catch e
                skipped += 1
                @warn "Skipped malformed MCT record" mct_id=mct_id exception=e
            end
        end
    finally
        DuckDB.close(appender)
        close(io)
    end

    (skipped > 0 || filtered > 0) && @info "MCT ingest complete" loaded=mct_id filtered=filtered skipped=skipped
    nothing
end

function _append_mct!(appender::DuckDB.Appender, mct_id::Int, line::String)
    _s(a, b) = strip(line[a:min(b, length(line))])
    _sb(pos) = pos <= length(line) ? strip(string(line[pos])) : ""

    arr_stn      = String(_s(2, 4))
    dep_stn      = String(_s(11, 13))
    arr_carrier  = String(_s(14, 15))
    dep_carrier  = String(_s(19, 20))
    submitting   = String(_s(95, 96))

    # Station standard = no carrier specificity (both arr and dep carrier blank)
    is_station_standard = isempty(arr_carrier) && isempty(dep_carrier) && isempty(submitting)

    # Suppression
    supp_char = _sb(87)
    is_suppressed = supp_char == "Y"

    # Date fields
    eff_str = _s(72, 78)
    dis_str = _s(79, 85)
    eff = isempty(eff_str) ? Date(1900, 1, 1) : parse_ddmonyy(eff_str)
    dis = isempty(dis_str) ? Date(2099, 12, 31) : parse_ddmonyy(dis_str)

    # Append in DDL column order (39 columns):
    # mct_id, record_serial, arr_stn, dep_stn, mct_status, time_minutes,
    # arr_carrier, arr_cs_ind, arr_cs_op_carrier, dep_carrier, dep_cs_ind,
    # dep_cs_op_carrier, arr_acft_type, arr_acft_body, dep_acft_type, dep_acft_body,
    # arr_term, dep_term, prv_ctry, prv_stn, nxt_ctry, nxt_stn,
    # arr_flt_rng_start, arr_flt_rng_end, dep_flt_rng_start, dep_flt_rng_end,
    # prv_state, nxt_state, prv_rgn, nxt_rgn,
    # eff_date, dis_date, suppress, supp_rgn, supp_ctry, supp_state,
    # submitting_carrier, station_standard, specificity
    DuckDB.append(appender, Int32(mct_id))                             # mct_id
    DuckDB.append(appender, parse_serial(_s(195, 200)))                # record_serial
    DuckDB.append(appender, arr_stn)                                   # arr_stn
    DuckDB.append(appender, dep_stn)                                   # dep_stn
    DuckDB.append(appender, String(_s(9, 10)))                         # mct_status
    DuckDB.append(appender, parse_hhmm(_s(5, 8)))                      # time_minutes
    DuckDB.append(appender, arr_carrier)                               # arr_carrier
    DuckDB.append(appender, String(_sb(16)))                           # arr_cs_ind
    DuckDB.append(appender, String(_s(17, 18)))                        # arr_cs_op_carrier
    DuckDB.append(appender, dep_carrier)                               # dep_carrier
    DuckDB.append(appender, String(_sb(21)))                           # dep_cs_ind
    DuckDB.append(appender, String(_s(22, 23)))                        # dep_cs_op_carrier
    DuckDB.append(appender, String(_s(24, 26)))                        # arr_acft_type
    DuckDB.append(appender, String(_sb(27)))                           # arr_acft_body
    DuckDB.append(appender, String(_s(28, 30)))                        # dep_acft_type
    DuckDB.append(appender, String(_sb(31)))                           # dep_acft_body
    DuckDB.append(appender, String(_s(32, 33)))                        # arr_term
    DuckDB.append(appender, String(_s(34, 35)))                        # dep_term
    DuckDB.append(appender, String(_s(36, 37)))                        # prv_ctry
    DuckDB.append(appender, String(_s(38, 40)))                        # prv_stn
    DuckDB.append(appender, String(_s(41, 42)))                        # nxt_ctry
    DuckDB.append(appender, String(_s(43, 45)))                        # nxt_stn
    DuckDB.append(appender, _parse_int16(_s(46, 49)))                  # arr_flt_rng_start
    DuckDB.append(appender, _parse_int16(_s(50, 53)))                  # arr_flt_rng_end
    DuckDB.append(appender, _parse_int16(_s(54, 57)))                  # dep_flt_rng_start
    DuckDB.append(appender, _parse_int16(_s(58, 61)))                  # dep_flt_rng_end
    DuckDB.append(appender, String(_s(62, 63)))                        # prv_state
    DuckDB.append(appender, String(_s(64, 65)))                        # nxt_state
    DuckDB.append(appender, String(_s(66, 68)))                        # prv_rgn
    DuckDB.append(appender, String(_s(69, 71)))                        # nxt_rgn
    DuckDB.append(appender, eff)                                       # eff_date
    DuckDB.append(appender, dis)                                       # dis_date
    DuckDB.append(appender, is_suppressed)                             # suppress
    DuckDB.append(appender, String(_s(88, 90)))                        # supp_rgn
    DuckDB.append(appender, String(_s(91, 92)))                        # supp_ctry
    DuckDB.append(appender, String(_s(93, 94)))                        # supp_state
    DuckDB.append(appender, submitting)                                # submitting_carrier
    DuckDB.append(appender, is_station_standard)                       # station_standard
    DuckDB.append(appender, UInt32(0))                                 # specificity (computed later)
    DuckDB.end_row(appender)
end

# src/ingest/ssim.jl — Streaming SSIM ingest via DuckDB Appender
#
# Reads SSIM fixed-width records line by line (constant memory) and appends
# Type 3 (leg) and Type 4 (DEI) records to the DuckDB store.
# Supports transparent decompression via open_maybe_compressed.

using DuckDB
using Dates

# ── Private helpers ───────────────────────────────────────────────────────────

"""
    `_safe_char(line::String, pos::Int)::Char`

Return the character at byte position `pos` (1-indexed) in `line`, or `' '`
if `pos` is beyond the string length.
"""
function _safe_char(line::String, pos::Int)::Char
    pos <= length(line) ? line[pos] : ' '
end

"""
    `_safe_substr(line::String, from::Int, to::Int)::String`

Return the substring from byte position `from` to `to` (1-indexed, inclusive),
padding with spaces if the line is shorter than `to`.
"""
function _safe_substr(line::String, from::Int, to::Int)::String
    n = length(line)
    from > n && return repeat(' ', to - from + 1)
    to > n   && return line[from:n] * repeat(' ', to - n)
    line[from:to]
end

"""
    `_parse_int16(s::AbstractString)::Int16`

Strip and parse `s` as Int16; return 0 on failure.
"""
function _parse_int16(s::AbstractString)::Int16
    s = strip(String(s))
    n = tryparse(Int16, s)
    n === nothing ? Int16(0) : n
end

"""
    `_parse_uint8(s::AbstractString)::UInt8`

Strip and parse `s` as UInt8; return 0 on failure.
"""
function _parse_uint8(s::AbstractString)::UInt8
    s = strip(String(s))
    n = tryparse(UInt8, s)
    n === nothing ? UInt8(0) : n
end

"""
    `_infer_body_type(eqp::AbstractString)::Char`

Infer widebody ('W') or narrowbody ('N') from ICAO equipment code.
Returns ' ' for blank/unknown codes.
"""
function _infer_body_type(eqp::AbstractString)::Char
    eqp = strip(String(eqp))
    isempty(eqp) && return ' '
    c = eqp[1]
    (c == '7' && length(eqp) >= 2 && eqp[2] in ('4', '6', '7', '8', '9')) && return 'W'
    (c == '3' && length(eqp) >= 2 && eqp[2] in ('3', '5', '8')) && return 'W'
    'N'
end

# ── Type 3 (leg) appender ─────────────────────────────────────────────────────

"""
    `_append_type3!(appender, row_id::Int, line::String)`

Parse a SSIM Type 3 record and append one row to the `legs` Appender.

Column order must match the `legs` DDL exactly:
  row_id, record_serial, airline, flt_no, op_suffix, itin_var,
  itin_var_overflow, leg_seq, svc_type, org, dst, pax_dep_mins, pax_arr_mins,
  ac_dep_mins, ac_arr_mins, dep_utc_offset, arr_utc_offset, dep_date_var,
  arr_date_var, dep_term, arr_term, eqp, body_type, aircraft_owner,
  eff_date, disc_date, frequency, mct_dep, mct_arr, trc, trc_overflow,
  prbd, distance, wet_lease
"""
function _append_type3!(appender, row_id::Int, line::String)
    # ── Extract fields by SSIM byte position (1-indexed) ───────────────────
    op_suffix        = _safe_char(line, 2)
    airline          = strip(_safe_substr(line, 3, 5))
    flt_no           = _parse_int16(_safe_substr(line, 6, 9))
    itin_var         = _parse_uint8(_safe_substr(line, 10, 11))
    leg_seq          = _parse_uint8(_safe_substr(line, 12, 13))
    svc_type         = _safe_char(line, 14)
    eff_date         = parse_ddmonyy(_safe_substr(line, 15, 21))
    disc_date        = parse_ddmonyy(_safe_substr(line, 22, 28))
    frequency        = parse_frequency_bitmask(_safe_substr(line, 29, 35))
    org              = strip(_safe_substr(line, 37, 39))
    pax_dep_mins     = parse_hhmm(_safe_substr(line, 40, 43))
    ac_dep_mins      = parse_hhmm(_safe_substr(line, 44, 47))
    dep_utc_offset   = parse_utc_offset(_safe_substr(line, 48, 52))
    dep_term         = strip(_safe_substr(line, 53, 54))
    dst              = strip(_safe_substr(line, 55, 57))
    ac_arr_mins      = parse_hhmm(_safe_substr(line, 58, 61))
    pax_arr_mins     = parse_hhmm(_safe_substr(line, 62, 65))
    arr_utc_offset   = parse_utc_offset(_safe_substr(line, 66, 70))
    arr_term         = strip(_safe_substr(line, 71, 72))
    eqp              = strip(_safe_substr(line, 73, 75))
    prbd             = strip(_safe_substr(line, 76, 95))
    mct_dep          = _safe_char(line, 120)
    mct_arr          = _safe_char(line, 121)
    itin_var_overflow = _safe_char(line, 128)
    aircraft_owner   = strip(_safe_substr(line, 129, 131))
    op_disclosure    = _safe_char(line, 149)
    trc              = _safe_substr(line, 150, 160)
    trc_overflow     = _safe_char(line, 161)
    dep_date_var     = parse_date_var(_safe_substr(line, 193, 193))
    arr_date_var     = parse_date_var(_safe_substr(line, 194, 194))
    record_serial    = parse_serial(_safe_substr(line, 195, 200))

    body_type        = _infer_body_type(eqp)
    wet_lease        = (op_disclosure == 'Z' || op_disclosure == 'S')

    # ── Append in DDL column order ─────────────────────────────────────────
    # row_id, record_serial, airline, flt_no, op_suffix, itin_var,
    # itin_var_overflow, leg_seq, svc_type, org, dst,
    # pax_dep_mins, pax_arr_mins, ac_dep_mins, ac_arr_mins,
    # dep_utc_offset, arr_utc_offset, dep_date_var, arr_date_var,
    # dep_term, arr_term, eqp, body_type, aircraft_owner,
    # eff_date, disc_date, frequency, mct_dep, mct_arr, trc, trc_overflow,
    # prbd, distance, wet_lease
    DuckDB.append(appender, row_id)
    DuckDB.append(appender, record_serial)
    DuckDB.append(appender, airline)
    DuckDB.append(appender, flt_no)
    DuckDB.append(appender, string(op_suffix))
    DuckDB.append(appender, itin_var)
    DuckDB.append(appender, string(itin_var_overflow))
    DuckDB.append(appender, leg_seq)
    DuckDB.append(appender, string(svc_type))
    DuckDB.append(appender, org)
    DuckDB.append(appender, dst)
    DuckDB.append(appender, pax_dep_mins)
    DuckDB.append(appender, pax_arr_mins)
    DuckDB.append(appender, ac_dep_mins)
    DuckDB.append(appender, ac_arr_mins)
    DuckDB.append(appender, dep_utc_offset)
    DuckDB.append(appender, arr_utc_offset)
    DuckDB.append(appender, dep_date_var)
    DuckDB.append(appender, arr_date_var)
    DuckDB.append(appender, dep_term)
    DuckDB.append(appender, arr_term)
    DuckDB.append(appender, eqp)
    DuckDB.append(appender, string(body_type))
    DuckDB.append(appender, aircraft_owner)
    DuckDB.append(appender, eff_date)
    DuckDB.append(appender, disc_date)
    DuckDB.append(appender, frequency)
    DuckDB.append(appender, string(mct_dep))
    DuckDB.append(appender, string(mct_arr))
    DuckDB.append(appender, trc)
    DuckDB.append(appender, string(trc_overflow))
    DuckDB.append(appender, prbd)
    DuckDB.append(appender, Float32(0.0))   # distance: populated by post-ingest pipeline
    DuckDB.append(appender, wet_lease)
    DuckDB.end_row(appender)
end

# ── Type 4 (DEI) appender ────────────────────────────────────────────────────

"""
    `_append_type4!(appender, row_id::Int, line::String)`

Parse a SSIM Type 4 (DEI) record and append one row to the `dei` Appender.

Column order must match the `dei` DDL exactly:
  row_id, dei_code, board_point, off_point, data, record_serial
"""
function _append_type4!(appender, row_id::Int, line::String)
    dei_code_str  = _safe_substr(line, 31, 33)
    board_point   = strip(_safe_substr(line, 34, 36))
    off_point     = strip(_safe_substr(line, 37, 39))
    data          = length(line) >= 40 ? strip(_safe_substr(line, 40, min(194, length(line)))) : ""
    record_serial = parse_serial(_safe_substr(line, 195, 200))

    dei_code = _parse_int16(dei_code_str)

    # row_id, dei_code, board_point, off_point, data, record_serial
    DuckDB.append(appender, row_id)
    DuckDB.append(appender, dei_code)
    DuckDB.append(appender, board_point)
    DuckDB.append(appender, off_point)
    DuckDB.append(appender, data)
    DuckDB.append(appender, record_serial)
    DuckDB.end_row(appender)
end

# ── Public API ────────────────────────────────────────────────────────────────

"""
    `ingest_ssim!(store::DuckDBStore, path::String)`
---

# Description
- Stream-parse an SSIM fixed-width schedule file into the DuckDB store
- Processes Type 3 (leg) and Type 4 (DEI) records; skips Types 1, 2, 5
- Uses DuckDB Appenders for constant-memory, high-throughput ingest
- Transparent decompression (gzip, zstd, bzip2, xz) via `open_maybe_compressed`
- Each Type 3 line produces one row in `legs`; each Type 4 line produces one row in `dei`
- `row_id` is assigned sequentially from 1 for legs; DEI rows share the row_id
  of the most recently seen Type 3 record

# Arguments
1. `store::DuckDBStore`: initialized store (tables must already exist)
2. `path::String`: path to SSIM file (may be compressed)

# Returns
- `nothing`

# Examples
```julia
julia> store = DuckDBStore();
julia> ingest_ssim!(store, "data/schedule.ssim");
julia> table_stats(store).legs
6304
```
"""
function ingest_ssim!(store::DuckDBStore, path::String)
    io = open_maybe_compressed(path)
    legs_appender = DuckDB.Appender(store.db, "legs")
    dei_appender  = DuckDB.Appender(store.db, "dei")

    leg_row_id   = 0
    current_row_id = 0

    try
        for line in eachline(io)
            isempty(line) && continue
            rec_type = line[1]

            if rec_type == '3' && length(line) >= 200
                leg_row_id += 1
                current_row_id = leg_row_id
                _append_type3!(legs_appender, current_row_id, line)

            elseif rec_type == '4' && length(line) >= 39
                # DEI row shares the row_id of the most recent Type 3
                _append_type4!(dei_appender, current_row_id, line)
            end
            # Types 1, 2, 5 are silently skipped
        end
    finally
        DuckDB.close(legs_appender)
        DuckDB.close(dei_appender)
        close(io)
    end

    return nothing
end

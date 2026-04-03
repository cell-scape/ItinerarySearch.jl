# src/audit/mct_replay.jl — Misconnect replayer: CSV parser and comparison engine
#
# Reads a misconnect CSV, replays each row through MCTLookup, and emits a
# comparison report showing whether our MCT resolves each misconnection.

# ── Row parsing ──────────────────────────────────────────────────────────────

"""
    `function parse_misconnect_row(row)::NamedTuple`
---

# Description
- Extract MCT lookup parameters from a single misconnect CSV row
- Handles `missing` values in all fields with safe coalescing
- Maps CSV column names to `lookup_mct_traced` keyword argument names

# Arguments
1. `row`: a `DataFrameRow` or similar with misconnect CSV columns

# Returns
- `NamedTuple` with all fields needed for `lookup_mct_traced` plus comparison fields
"""
function parse_misconnect_row(row)
    _s(val) = ismissing(val) ? "" : string(val)
    _c(val) = begin
        s = strip(_s(val))
        isempty(s) ? ' ' : first(s)
    end
    _flt(val) = begin
        s = strip(_s(val))
        isempty(s) ? FlightNumber(0) : FlightNumber(parse(Int, s))
    end
    _num(val) = begin
        v = coalesce(val, NaN)
        v isa AbstractString ? (isempty(strip(v)) ? NaN : parse(Float64, v)) : Float64(v)
    end
    _int(val) = begin
        v = coalesce(val, 0)
        v isa AbstractString ? (isempty(strip(v)) ? 0 : parse(Int, v)) : Int(v)
    end

    # Status mapping
    raw_status = strip(_s(row.international_domestic_status))
    status = if raw_status == "DD"
        MCT_DD
    elseif raw_status == "DI"
        MCT_DI
    elseif raw_status == "ID"
        MCT_ID
    elseif raw_status == "II"
        MCT_II
    else
        MCT_DD  # fallback
    end

    # Connect station = inbound arrival = outbound departure
    arr_station_str = strip(_s(row.inbound_arrival_station))
    dep_station_str = strip(_s(row.outbound_departure_station))
    arr_station = isempty(arr_station_str) ? NO_STATION : StationCode(arr_station_str)
    dep_station = isempty(dep_station_str) ? NO_STATION : StationCode(dep_station_str)

    # Origin/destination stations (prv_stn = origin of arriving flight, nxt_stn = destination of departing flight)
    prv_stn_str = strip(_s(row.inbound_departure_station))
    nxt_stn_str = strip(_s(row.outbound_arrival_station))
    prv_stn = isempty(prv_stn_str) ? NO_STATION : StationCode(prv_stn_str)
    nxt_stn = isempty(nxt_stn_str) ? NO_STATION : StationCode(nxt_stn_str)

    # Carriers
    arr_carrier_str = strip(_s(row.inbound_carrier))
    dep_carrier_str = strip(_s(row.outbound_carrier))
    arr_carrier = isempty(arr_carrier_str) ? NO_AIRLINE : AirlineCode(arr_carrier_str)
    dep_carrier = isempty(dep_carrier_str) ? NO_AIRLINE : AirlineCode(dep_carrier_str)

    # Terminals
    arr_term_str = strip(_s(row.inbound_arrival_terminal))
    dep_term_str = strip(_s(row.outbound_departure_terminal))
    arr_term = InlineString3(arr_term_str)
    dep_term = InlineString3(dep_term_str)

    # Body type — first char
    arr_body = _c(row.inbound_aircraft_bodytype)
    dep_body = _c(row.outbound_aircraft_bodytype)

    # Aircraft type
    arr_acft_str = strip(_s(row.inbound_aircraft_type))
    dep_acft_str = strip(_s(row.outbound_aircraft_type))
    arr_acft_type = InlineString7(arr_acft_str)
    dep_acft_type = InlineString7(dep_acft_str)

    # Flight numbers
    arr_flt_no = _flt(row.inbound_flight_number)
    dep_flt_no = _flt(row.outbound_flight_number)

    # Operating carriers
    arr_op_str = strip(_s(row.inbound_operating_carrier))
    dep_op_str = strip(_s(row.outbound_operating_carrier))
    arr_op_carrier = isempty(arr_op_str) ? NO_AIRLINE : AirlineCode(arr_op_str)
    dep_op_carrier = isempty(dep_op_str) ? NO_AIRLINE : AirlineCode(dep_op_str)

    # Codeshare indicators
    arr_is_codeshare = uppercase(strip(_s(row.inbound_codeshare_indicator))) == "Y"
    dep_is_codeshare = uppercase(strip(_s(row.outbound_codeshare_indicator))) == "Y"

    # Geography
    prv_country_str = strip(_s(row.inbound_departure_country))
    nxt_country_str = strip(_s(row.outbound_departure_country))
    prv_country = InlineString3(prv_country_str)
    nxt_country = InlineString3(nxt_country_str)

    prv_state_str = strip(_s(row.inbound_departure_state))
    nxt_state_str = strip(_s(row.outbound_departure_state))
    prv_state = InlineString3(prv_state_str)
    nxt_state = InlineString3(nxt_state_str)

    # Target date from inbound_arrival_date
    target_date = begin
        ds = strip(_s(row.inbound_arrival_date))
        if isempty(ds)
            UInt32(0)
        else
            d = Date(ds)
            pack_date(d)
        end
    end

    # Their MCT values
    their_mct = Minutes(_int(row.mct))
    their_mctrec = _int(row.mctrec)
    cnx_time = Minutes(round(Int, _num(row.connection_time)))
    their_mct_diff = _num(row.mct_diff)

    # Record locator
    rcrd_loc = _s(row.rcrd_loc)

    (;
        rcrd_loc,
        arr_carrier, dep_carrier,
        arr_station, dep_station,
        prv_stn, nxt_stn,
        status,
        arr_term, dep_term,
        arr_body, dep_body,
        arr_acft_type, dep_acft_type,
        arr_flt_no, dep_flt_no,
        arr_op_carrier, dep_op_carrier,
        arr_is_codeshare, dep_is_codeshare,
        prv_country, nxt_country,
        prv_state, nxt_state,
        target_date,
        their_mct, their_mctrec,
        cnx_time, their_mct_diff,
    )
end

# ── Replay engine ────────────────────────────────────────────────────────────

const _REPLAY_COLUMNS = [
    "rcrd_loc", "connect_station", "arr_carrier", "dep_carrier", "status",
    "arr_terminal", "dep_terminal", "arr_body", "dep_body",
    "arr_acft_type", "dep_acft_type", "arr_flt_no", "dep_flt_no",
    "arr_is_codeshare", "dep_is_codeshare",
    "their_mct", "their_mctrec", "our_mct", "our_mct_id", "our_source",
    "our_specificity", "our_matched_fields", "time_match",
    "cnx_time", "their_mct_diff", "our_mct_diff", "our_resolves",
]

"""
    `function replay_misconnects(path::AbstractString, lookup::MCTLookup; output_io::IO=stdout, detail::Symbol=:summary)::Nothing`
---

# Description
- Read a misconnect CSV and replay each row through the given MCTLookup
- Compares our MCT result against their reported MCT and connection time
- Writes comparison output as CSV (summary) or JSONL (detailed)
- The headline metric is `our_resolves`: whether our MCT <= connection time

# Arguments
1. `path::AbstractString`: path to misconnect CSV file
2. `lookup::MCTLookup`: pre-built MCT lookup structure

# Keyword Arguments
- `output_io::IO=stdout`: where to write comparison output
- `detail::Symbol=:summary`: `:summary` (CSV) or `:detailed` (JSONL)

# Returns
- `::Nothing`
"""
function replay_misconnects(
    path::AbstractString,
    lookup::MCTLookup;
    output_io::IO = stdout,
    detail::Symbol = :summary,
    airports::Dict{StationCode,StationRecord} = Dict{StationCode,StationRecord}(),
)::Nothing
    df = CSV.read(path, DataFrames.DataFrame; stringtype=String)
    _replay_dataframe(df, lookup; output_io, detail, airports)
    return nothing
end

"""
    `function replay_misconnects(misconnect_path::AbstractString, mct_path::AbstractString; kwargs...)::Nothing`
---

# Description
- Convenience wrapper that loads an MCT file into a DuckDB store,
  materializes the MCTLookup, then replays the misconnect CSV
- Useful for standalone comparison without an existing lookup

# Arguments
1. `misconnect_path::AbstractString`: path to misconnect CSV
2. `mct_path::AbstractString`: path to SSIM8 MCT file

# Keyword Arguments
- Same as the primary `replay_misconnects` method

# Returns
- `::Nothing`
"""
function replay_misconnects(
    misconnect_path::AbstractString,
    mct_path::AbstractString;
    output_io::IO = stdout,
    detail::Symbol = :summary,
)::Nothing
    store = DuckDBStore()
    try
        ingest_mct!(store, mct_path)
        lookup = materialize_mct_lookup(store)
        replay_misconnects(misconnect_path, lookup; output_io, detail)
    finally
        close(store)
    end
    return nothing
end

# ── Internal replay logic ────────────────────────────────────────────────────

function _replay_dataframe(
    df::DataFrames.DataFrame,
    lookup::MCTLookup;
    output_io::IO = stdout,
    detail::Symbol = :summary,
    airports::Dict{StationCode,StationRecord} = Dict{StationCode,StationRecord}(),
)
    if detail == :summary
        println(output_io, join(_REPLAY_COLUMNS, ","))
    end

    for row in eachrow(df)
        parsed = parse_misconnect_row(row)

        # Resolve region from airports dict
        prv_region = InlineStrings.InlineString3("")
        nxt_region = InlineStrings.InlineString3("")
        prv_info = get(airports, parsed.arr_station, nothing)
        nxt_info = get(airports, parsed.dep_station, nothing)
        prv_info !== nothing && (prv_region = prv_info.region)
        nxt_info !== nothing && (nxt_region = nxt_info.region)

        trace = lookup_mct_traced(
            lookup,
            parsed.arr_carrier,
            parsed.dep_carrier,
            parsed.arr_station,
            parsed.dep_station,
            parsed.status;
            arr_body = parsed.arr_body,
            dep_body = parsed.dep_body,
            arr_term = parsed.arr_term,
            dep_term = parsed.dep_term,
            arr_acft_type = parsed.arr_acft_type,
            dep_acft_type = parsed.dep_acft_type,
            prv_stn = parsed.prv_stn,
            nxt_stn = parsed.nxt_stn,
            arr_flt_no = parsed.arr_flt_no,
            dep_flt_no = parsed.dep_flt_no,
            arr_op_carrier = parsed.arr_op_carrier,
            dep_op_carrier = parsed.dep_op_carrier,
            arr_is_codeshare = parsed.arr_is_codeshare,
            dep_is_codeshare = parsed.dep_is_codeshare,
            prv_country = parsed.prv_country,
            nxt_country = parsed.nxt_country,
            prv_state = parsed.prv_state,
            nxt_state = parsed.nxt_state,
            prv_region = prv_region,
            nxt_region = nxt_region,
            target_date = parsed.target_date,
        )

        r = trace.result
        our_mct = Int(r.time)
        our_mct_id = Int(r.mct_id)
        our_source = _source_str(r.source)
        our_specificity = string(r.specificity, base=16)
        our_matched_fields = decode_matched_fields(r.matched_fields)
        time_match = our_mct == Int(parsed.their_mct)
        cnx_int = Int(parsed.cnx_time)
        our_mct_diff = cnx_int - our_mct
        our_resolves = our_mct <= cnx_int

        if detail == :summary
            connect_station = String(parsed.arr_station)
            fields = [
                parsed.rcrd_loc,
                connect_station,
                String(parsed.arr_carrier),
                String(parsed.dep_carrier),
                _status_str(parsed.status),
                String(parsed.arr_term),
                String(parsed.dep_term),
                string(parsed.arr_body == ' ' ? "" : parsed.arr_body),
                string(parsed.dep_body == ' ' ? "" : parsed.dep_body),
                String(parsed.arr_acft_type),
                String(parsed.dep_acft_type),
                string(Int(parsed.arr_flt_no)),
                string(Int(parsed.dep_flt_no)),
                string(parsed.arr_is_codeshare),
                string(parsed.dep_is_codeshare),
                string(parsed.their_mct),
                string(parsed.their_mctrec),
                string(our_mct),
                string(our_mct_id),
                our_source,
                our_specificity,
                "\"" * our_matched_fields * "\"",
                string(time_match),
                string(cnx_int),
                string(isnan(parsed.their_mct_diff) ? "" : string(Int(round(parsed.their_mct_diff)))),
                string(our_mct_diff),
                string(our_resolves),
            ]
            println(output_io, join(fields, ","))
        else
            obj = Dict{String,Any}(
                "rcrd_loc" => parsed.rcrd_loc,
                "connect_station" => String(parsed.arr_station),
                "arr_carrier" => String(parsed.arr_carrier),
                "dep_carrier" => String(parsed.dep_carrier),
                "status" => _status_str(parsed.status),
                "arr_terminal" => String(parsed.arr_term),
                "dep_terminal" => String(parsed.dep_term),
                "arr_body" => string(parsed.arr_body == ' ' ? "" : parsed.arr_body),
                "dep_body" => string(parsed.dep_body == ' ' ? "" : parsed.dep_body),
                "arr_acft_type" => String(parsed.arr_acft_type),
                "dep_acft_type" => String(parsed.dep_acft_type),
                "arr_flt_no" => Int(parsed.arr_flt_no),
                "dep_flt_no" => Int(parsed.dep_flt_no),
                "arr_is_codeshare" => parsed.arr_is_codeshare,
                "dep_is_codeshare" => parsed.dep_is_codeshare,
                "their_mct" => parsed.their_mct,
                "their_mctrec" => parsed.their_mctrec,
                "our_mct" => our_mct,
                "our_mct_id" => our_mct_id,
                "our_source" => our_source,
                "our_specificity" => our_specificity,
                "our_matched_fields" => our_matched_fields,
                "time_match" => time_match,
                "cnx_time" => cnx_int,
                "their_mct_diff" => isnan(parsed.their_mct_diff) ? nothing : Int(round(parsed.their_mct_diff)),
                "our_mct_diff" => our_mct_diff,
                "our_resolves" => our_resolves,
                "candidates" => length(trace.candidates),
            )
            println(output_io, JSON3.write(obj))
        end
    end
end

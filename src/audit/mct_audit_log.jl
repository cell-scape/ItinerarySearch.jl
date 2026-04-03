# src/audit/mct_audit_log.jl — File-based MCT audit log writer

mutable struct MCTAuditLog
    io::IO
    config::MCTAuditConfig
    written::Int
    header_written::Bool
end

const _SUMMARY_COLUMNS = [
    "arr_carrier", "dep_carrier", "arr_station", "dep_station", "status",
    "arr_terminal", "dep_terminal", "arr_body", "dep_body",
    "arr_acft_type", "dep_acft_type", "arr_flt_no", "dep_flt_no",
    "codeshare_mode", "arr_is_codeshare", "dep_is_codeshare",
    "mct_time", "mct_source", "mct_id", "specificity", "matched_fields_decoded",
    "suppressed", "cnx_time", "pass_fail",
]

function _source_str(s::MCTSource)::String
    s == SOURCE_EXCEPTION ? "exception" :
    s == SOURCE_STATION_STANDARD ? "station_standard" : "global_default"
end

function _status_str(s::MCTStatus)::String
    s == MCT_DD ? "DD" : s == MCT_DI ? "DI" : s == MCT_ID ? "ID" : "II"
end

function open_audit_log(io::IO, config::MCTAuditConfig)::MCTAuditLog
    MCTAuditLog(io, config, 0, false)
end

function open_audit_log(config::MCTAuditConfig)::MCTAuditLog
    io = isempty(config.output_path) ? stdout : open(config.output_path, "w")
    MCTAuditLog(io, config, 0, false)
end

function write_audit_entry!(log::MCTAuditLog, trace::MCTTrace; cnx_time::Minutes=Minutes(0))::Bool
    if log.config.max_connections > 0 && log.written >= log.config.max_connections
        return false
    end
    if log.config.detail == :summary
        _write_summary_row!(log, trace, cnx_time)
    else
        _write_detailed_row!(log, trace, cnx_time)
    end
    log.written += 1
    return true
end

function _write_summary_row!(log::MCTAuditLog, trace::MCTTrace, cnx_time::Minutes)
    if !log.header_written
        println(log.io, join(_SUMMARY_COLUMNS, ","))
        log.header_written = true
    end
    r = trace.result
    pass_fail = cnx_time >= r.time ? "pass" : "fail"
    fields = [
        String(trace.arr_carrier),
        String(trace.dep_carrier),
        String(trace.arr_station),
        String(trace.dep_station),
        _status_str(trace.status),
        String(trace.arr_term),
        String(trace.dep_term),
        string(trace.arr_body == ' ' ? "" : trace.arr_body),
        string(trace.dep_body == ' ' ? "" : trace.dep_body),
        String(trace.arr_acft_type),
        String(trace.dep_acft_type),
        string(Int(trace.arr_flt_no)),
        string(Int(trace.dep_flt_no)),
        string(trace.codeshare_mode),
        string(trace.arr_is_codeshare),
        string(trace.dep_is_codeshare),
        string(Int(r.time)),
        _source_str(r.source),
        string(Int(r.mct_id)),
        string(r.specificity, base=16),
        "\"" * decode_matched_fields(r.matched_fields) * "\"",
        string(r.suppressed),
        string(Int(cnx_time)),
        pass_fail,
    ]
    println(log.io, join(fields, ","))
end

function _write_detailed_row!(log::MCTAuditLog, trace::MCTTrace, cnx_time::Minutes)
    r = trace.result
    max_cand = log.config.max_candidates
    cands = if max_cand > 0 && length(trace.candidates) > max_cand
        trace.candidates[1:max_cand]
    else
        trace.candidates
    end
    obj = Dict{String,Any}(
        "arr_carrier" => String(trace.arr_carrier),
        "dep_carrier" => String(trace.dep_carrier),
        "arr_station" => String(trace.arr_station),
        "dep_station" => String(trace.dep_station),
        "status" => _status_str(trace.status),
        "codeshare_mode" => string(trace.codeshare_mode),
        "mct_time" => Int(r.time),
        "mct_source" => _source_str(r.source),
        "mct_id" => Int(r.mct_id),
        "specificity" => string(r.specificity, base=16),
        "matched_fields" => decode_matched_fields(r.matched_fields),
        "suppressed" => r.suppressed,
        "cnx_time" => Int(cnx_time),
        "pass_fail" => cnx_time >= r.time ? "pass" : "fail",
        "candidates" => [
            Dict{String,Any}(
                "mct_id" => Int(c.record.mct_id),
                "time" => Int(c.record.time),
                "specificity" => string(c.record.specificity, base=16),
                "matched" => c.matched,
                "skip_reason" => string(c.skip_reason),
                "pass" => string(c.pass),
                "specified_fields" => decode_matched_fields(c.record.specified),
            ) for c in cands
        ],
    )
    if trace.marketing_result !== EMPTY_MCT_RESULT
        obj["marketing_result"] = Dict(
            "time" => Int(trace.marketing_result.time),
            "mct_id" => Int(trace.marketing_result.mct_id),
            "source" => _source_str(trace.marketing_result.source),
        )
    end
    if trace.operating_result !== EMPTY_MCT_RESULT
        obj["operating_result"] = Dict(
            "time" => Int(trace.operating_result.time),
            "mct_id" => Int(trace.operating_result.mct_id),
            "source" => _source_str(trace.operating_result.source),
        )
    end
    println(log.io, JSON3.write(obj))
end

function close_audit_log(log::MCTAuditLog)
    flush(log.io)
    log.io !== stdout && log.io isa IOStream && close(log.io)
end

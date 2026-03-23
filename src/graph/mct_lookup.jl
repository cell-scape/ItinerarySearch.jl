# src/graph/mct_lookup.jl — In-memory hierarchical MCT lookup (SSIM8 cascade)

# ── Bit-position constants for the MCTRecord.specified bitmask ────────────────

"""
    MCT_BIT_ARR_CARRIER … MCT_BIT_ARR_BODY — presence bitmask constants

Each constant marks which matching field was explicitly set in the original
SSIM8 MCT record.  A bit that is *not* set means "wildcard — accept any value
in this position."  Use the constants with bitwise AND/OR to test or build the
`specified` field of an `MCTRecord`.
"""
const MCT_BIT_ARR_CARRIER = UInt32(1 << 0)
const MCT_BIT_DEP_CARRIER = UInt32(1 << 1)
const MCT_BIT_ARR_TERM    = UInt32(1 << 2)
const MCT_BIT_DEP_TERM    = UInt32(1 << 3)
const MCT_BIT_PRV_STN     = UInt32(1 << 4)
const MCT_BIT_NXT_STN     = UInt32(1 << 5)
const MCT_BIT_PRV_COUNTRY = UInt32(1 << 6)
const MCT_BIT_NXT_COUNTRY = UInt32(1 << 7)
const MCT_BIT_PRV_REGION  = UInt32(1 << 8)
const MCT_BIT_NXT_REGION  = UInt32(1 << 9)
const MCT_BIT_DEP_BODY    = UInt32(1 << 10)
const MCT_BIT_ARR_BODY    = UInt32(1 << 11)
const MCT_BIT_ARR_CS_IND    = UInt32(1 << 12)
const MCT_BIT_ARR_CS_OP     = UInt32(1 << 13)
const MCT_BIT_DEP_CS_IND    = UInt32(1 << 14)
const MCT_BIT_DEP_CS_OP     = UInt32(1 << 15)
const MCT_BIT_ARR_ACFT_TYPE = UInt32(1 << 16)
const MCT_BIT_DEP_ACFT_TYPE = UInt32(1 << 17)
const MCT_BIT_ARR_FLT_RNG   = UInt32(1 << 18)
const MCT_BIT_DEP_FLT_RNG   = UInt32(1 << 19)
const MCT_BIT_PRV_STATE     = UInt32(1 << 20)
const MCT_BIT_NXT_STATE     = UInt32(1 << 21)

# ── MCTRecord ─────────────────────────────────────────────────────────────────

"""
    struct MCTRecord

MCTRecord — Single MCT record from the SSIM8 table, materialized in memory.

Matching fields use sentinel constants (`NO_STATION`, `NO_AIRLINE`, etc.) to
indicate "not specified."  The `specified::UInt32` bitmask packs presence
flags — bit N set means field N was specified in the original data and must
match.  Fields not present in `specified` are wildcards and always match.

# Fields
- `arr_carrier::AirlineCode` — arriving flight carrier (wildcard if not specified)
- `dep_carrier::AirlineCode` — departing flight carrier (wildcard if not specified)
- `arr_term::InlineString3` — arrival terminal (wildcard if not specified)
- `dep_term::InlineString3` — departure terminal (wildcard if not specified)
- `prv_stn::StationCode` — origin of arriving flight (wildcard if not specified)
- `nxt_stn::StationCode` — destination of departing flight (wildcard if not specified)
- `prv_country::InlineString3` — country of origin of arriving flight
- `nxt_country::InlineString3` — country of destination of departing flight
- `prv_region::InlineString3` — IATA region of origin
- `nxt_region::InlineString3` — IATA region of destination
- `dep_body::Char` — departing aircraft body type ('W'ide / 'N'arrow)
- `arr_body::Char` — arriving aircraft body type
- `arr_cs_ind::Char` — 'Y' if arriving flight is a codeshare (SSIM8 byte 16)
- `arr_cs_op_carrier::AirlineCode` — codeshare operating carrier for arriving flight (bytes 17-19)
- `dep_cs_ind::Char` — 'Y' if departing flight is a codeshare (SSIM8 byte 20)
- `dep_cs_op_carrier::AirlineCode` — codeshare operating carrier for departing flight (bytes 21-23)
- `arr_acft_type::InlineString7` — arriving aircraft IATA type code (bytes 24-26, mutually exclusive with arr_body)
- `dep_acft_type::InlineString7` — departing aircraft IATA type code (bytes 28-30, mutually exclusive with dep_body)
- `arr_flt_rng_start::FlightNumber` — start of arriving flight number range (bytes 46-49)
- `arr_flt_rng_end::FlightNumber` — end of arriving flight number range (bytes 50-53)
- `dep_flt_rng_start::FlightNumber` — start of departing flight number range (bytes 54-57)
- `dep_flt_rng_end::FlightNumber` — end of departing flight number range (bytes 58-61)
- `prv_state::InlineString3` — state/province of origin of arriving flight (bytes 62-63)
- `nxt_state::InlineString3` — state/province of destination of departing flight (bytes 64-65)
- `eff_date::UInt32` — effective date packed as YYYYMMDD; 0 = no restriction (bytes 72-78)
- `dis_date::UInt32` — discontinue date packed as YYYYMMDD; 0 = no restriction (bytes 79-85)
- `supp_region::InlineString3` — suppression geography: IATA region code (bytes 88-89)
- `supp_country::InlineString3` — suppression geography: country code (bytes 90-91)
- `supp_state::InlineString3` — suppression geography: state/province code (bytes 92-93)
- `specified::UInt32` — bitmask of fields that must match (see `MCT_BIT_*` constants)
- `time::Minutes` — MCT in minutes (0 if suppressed)
- `suppressed::Bool` — if true this record suppresses MCT (connection not permitted)
- `station_standard::Bool` — true for station-standard (no carrier specificity) records
- `specificity::UInt32` — pre-sorted descending weight; higher = more specific
- `mct_id::Int32` — primary key of the originating `mct` table row (0 = unknown/default)
"""
@kwdef struct MCTRecord
    # Matching fields
    arr_carrier::AirlineCode  = NO_AIRLINE
    dep_carrier::AirlineCode  = NO_AIRLINE
    arr_term::InlineString3   = InlineString3("")
    dep_term::InlineString3   = InlineString3("")
    prv_stn::StationCode      = NO_STATION
    nxt_stn::StationCode      = NO_STATION
    prv_country::InlineString3 = InlineString3("")
    nxt_country::InlineString3 = InlineString3("")
    prv_region::InlineString3  = InlineString3("")
    nxt_region::InlineString3  = InlineString3("")
    dep_body::Char             = ' '
    arr_body::Char             = ' '

    # ── Codeshare (SSIM8 bytes 16-23) ──
    arr_cs_ind::Char = ' '                          # 'Y' = codeshare MCT
    arr_cs_op_carrier::AirlineCode = NO_AIRLINE
    dep_cs_ind::Char = ' '
    dep_cs_op_carrier::AirlineCode = NO_AIRLINE

    # ── Aircraft Type (SSIM8 bytes 24-30, mutually exclusive with Body) ──
    arr_acft_type::InlineString7 = InlineString7("")
    dep_acft_type::InlineString7 = InlineString7("")

    # ── Flight Number Ranges (SSIM8 bytes 46-61) ──
    arr_flt_rng_start::FlightNumber = FlightNumber(0)
    arr_flt_rng_end::FlightNumber = FlightNumber(0)
    dep_flt_rng_start::FlightNumber = FlightNumber(0)
    dep_flt_rng_end::FlightNumber = FlightNumber(0)

    # ── State Geography (SSIM8 bytes 62-65) ──
    prv_state::InlineString3 = InlineString3("")
    nxt_state::InlineString3 = InlineString3("")

    # ── Date Validity (SSIM8 bytes 72-85) ──
    eff_date::UInt32 = UInt32(0)    # packed YYYYMMDD, 0 = no restriction
    dis_date::UInt32 = UInt32(0)

    # ── Suppression Geography (SSIM8 bytes 88-94) ──
    supp_region::InlineString3 = InlineString3("")
    supp_country::InlineString3 = InlineString3("")
    supp_state::InlineString3 = InlineString3("")

    # Presence bitmask (bit positions defined by MCT_BIT_* constants)
    specified::UInt32 = UInt32(0)

    # Result
    time::Minutes      = Minutes(0)
    suppressed::Bool   = false
    station_standard::Bool = false
    specificity::UInt32 = UInt32(0)    # pre-sorted descending
    mct_id::Int32 = Int32(0)           # PK from mct table (0 = unknown/default)
end

# ── MCTLookup ─────────────────────────────────────────────────────────────────

"""
    struct MCTLookup

MCTLookup — In-memory hierarchical MCT lookup structure.

`stations` maps station code → 4-element `NTuple` of `Vector{MCTRecord}`, one
per `MCTStatus` (index 1 = `MCT_DD`, 2 = `MCT_DI`, 3 = `MCT_ID`, 4 = `MCT_II`).
Records within each vector are sorted by descending `specificity` for
first-match-wins lookup.

`global_defaults` provides the fallback MCT time (minutes) for each status when
no station-level record matches.

# Fields
- `stations::Dict{StationCode, NTuple{4, Vector{MCTRecord}}}` — per-station records
- `global_defaults::NTuple{4, Minutes}` — IATA fallback times (DD=60, DI=90, ID=90, II=120)

# Examples
```julia
julia> lookup = MCTLookup();
julia> result = lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                           StationCode("ORD"), MCT_DD);
julia> result.source == SOURCE_GLOBAL_DEFAULT
true
```
"""
@kwdef struct MCTLookup
    stations::Dict{StationCode, NTuple{4, Vector{MCTRecord}}} =
        Dict{StationCode, NTuple{4, Vector{MCTRecord}}}()
    global_defaults::NTuple{4, Minutes} =
        (Minutes(60), Minutes(90), Minutes(90), Minutes(120))
end

# ── Specificity computation ───────────────────────────────────────────────────

"""
    `function _compute_specificity(rec::MCTRecord)::UInt32`
---

# Description
- Compute the SSIM8 specificity weight for an `MCTRecord`
- Higher bits correspond to more discriminating fields (carriers outrank
  terminals, terminals outrank station-to-station pairs, etc.)
- The returned value is stored in `MCTRecord.specificity` and used for
  descending sort so that the most-specific matching record is tried first

# Arguments
1. `rec::MCTRecord`: the record whose `specified` bitmask drives the computation

# Returns
- `::UInt32`: specificity weight (0 = completely generic, larger = more specific)
"""
function _compute_specificity(rec::MCTRecord)::UInt32
    sp = rec.specified
    s = UInt32(0)
    # Priority 2-4: Dep codeshare + carrier
    (sp & MCT_BIT_DEP_CS_IND)    != 0 && (s += UInt32(1) << 29)
    (sp & MCT_BIT_DEP_CARRIER)   != 0 && (s += UInt32(1) << 28)
    (sp & MCT_BIT_DEP_CS_OP)     != 0 && (s += UInt32(1) << 27)
    # Priority 5-7: Arr codeshare + carrier
    (sp & MCT_BIT_ARR_CS_IND)    != 0 && (s += UInt32(1) << 26)
    (sp & MCT_BIT_ARR_CARRIER)   != 0 && (s += UInt32(1) << 25)
    (sp & MCT_BIT_ARR_CS_OP)     != 0 && (s += UInt32(1) << 24)
    # Priority 8-11: Flight number ranges
    (sp & MCT_BIT_DEP_FLT_RNG)   != 0 && (s += UInt32(1) << 23)
    (sp & MCT_BIT_ARR_FLT_RNG)   != 0 && (s += UInt32(1) << 22)
    # Priority 12-13: Terminals
    (sp & MCT_BIT_DEP_TERM)      != 0 && (s += UInt32(1) << 21)
    (sp & MCT_BIT_ARR_TERM)      != 0 && (s += UInt32(1) << 20)
    # Priority 14-15: Stations
    (sp & MCT_BIT_NXT_STN)       != 0 && (s += UInt32(1) << 19)
    (sp & MCT_BIT_PRV_STN)       != 0 && (s += UInt32(1) << 18)
    # Priority 16-17: States
    (sp & MCT_BIT_NXT_STATE)     != 0 && (s += UInt32(1) << 17)
    (sp & MCT_BIT_PRV_STATE)     != 0 && (s += UInt32(1) << 16)
    # Priority 18-19: Countries
    (sp & MCT_BIT_NXT_COUNTRY)   != 0 && (s += UInt32(1) << 15)
    (sp & MCT_BIT_PRV_COUNTRY)   != 0 && (s += UInt32(1) << 14)
    # Priority 20-21: Regions
    (sp & MCT_BIT_NXT_REGION)    != 0 && (s += UInt32(1) << 13)
    (sp & MCT_BIT_PRV_REGION)    != 0 && (s += UInt32(1) << 12)
    # Priority 22-23: Aircraft type
    (sp & MCT_BIT_DEP_ACFT_TYPE) != 0 && (s += UInt32(1) << 11)
    (sp & MCT_BIT_ARR_ACFT_TYPE) != 0 && (s += UInt32(1) << 10)
    # Priority 24-25: Aircraft body
    (sp & MCT_BIT_DEP_BODY)      != 0 && (s += UInt32(1) << 9)
    (sp & MCT_BIT_ARR_BODY)      != 0 && (s += UInt32(1) << 8)
    # Priority 26-27: Effective dates
    (rec.eff_date != UInt32(0))          && (s += UInt32(1) << 7)
    return s
end

# ── Field matching ────────────────────────────────────────────────────────────

"""
    `function _mct_record_matches(rec::MCTRecord, arr_carrier, dep_carrier, arr_body, dep_body, prv_stn, nxt_stn, arr_term, dep_term, prv_country, nxt_country, arr_op_carrier, dep_op_carrier, arr_is_codeshare, dep_is_codeshare, arr_acft_type, dep_acft_type, arr_flt_no, dep_flt_no, prv_state, nxt_state)::Bool`
---

# Description
- Test whether `rec` matches the given connection attributes
- For each bit set in `rec.specified`, the corresponding field must equal the
  supplied value; fields whose bit is *not* set are wildcards and always match
- Used by `lookup_mct` in the first-match-wins pass over sorted records
- Covers the full SSIM8 matching hierarchy: carriers, body types, terminals,
  station pairs, codeshare indicators, flight number ranges, aircraft types,
  states, countries, and regions

# Arguments
1. `rec::MCTRecord`: the candidate record
2. `arr_carrier::AirlineCode`: arriving flight carrier code
3. `dep_carrier::AirlineCode`: departing flight carrier code
4. `arr_body::Char`: arriving aircraft body type
5. `dep_body::Char`: departing aircraft body type
6. `prv_stn::StationCode`: origin of arriving flight
7. `nxt_stn::StationCode`: destination of departing flight
8. `arr_term::InlineString3`: arrival terminal
9. `dep_term::InlineString3`: departure terminal
10. `prv_country::InlineString3`: country of arriving flight's origin
11. `nxt_country::InlineString3`: country of departing flight's destination
12. `arr_op_carrier::AirlineCode`: operating carrier of arriving flight (for codeshare)
13. `dep_op_carrier::AirlineCode`: operating carrier of departing flight (for codeshare)
14. `arr_is_codeshare::Bool`: true if arriving flight is a codeshare
15. `dep_is_codeshare::Bool`: true if departing flight is a codeshare
16. `arr_acft_type::InlineString7`: arriving aircraft IATA type code
17. `dep_acft_type::InlineString7`: departing aircraft IATA type code
18. `arr_flt_no::FlightNumber`: arriving flight number
19. `dep_flt_no::FlightNumber`: departing flight number
20. `prv_state::InlineString3`: state/province of arriving flight's origin
21. `nxt_state::InlineString3`: state/province of departing flight's destination

# Returns
- `::Bool`: `true` if all specified fields match
"""
@inline function _mct_record_matches(
    rec::MCTRecord,
    arr_carrier::AirlineCode,
    dep_carrier::AirlineCode,
    arr_body::Char,
    dep_body::Char,
    prv_stn::StationCode,
    nxt_stn::StationCode,
    arr_term::InlineString3,
    dep_term::InlineString3,
    prv_country::InlineString3,
    nxt_country::InlineString3,
    arr_op_carrier::AirlineCode,
    dep_op_carrier::AirlineCode,
    arr_is_codeshare::Bool,
    dep_is_codeshare::Bool,
    arr_acft_type::InlineString7,
    dep_acft_type::InlineString7,
    arr_flt_no::FlightNumber,
    dep_flt_no::FlightNumber,
    prv_state::InlineString3,
    nxt_state::InlineString3,
)::Bool
    sp = rec.specified
    sp == UInt32(0) && return true   # fast path: pure wildcard record
    (sp & MCT_BIT_ARR_CARRIER) != 0 && rec.arr_carrier != arr_carrier && return false
    (sp & MCT_BIT_DEP_CARRIER) != 0 && rec.dep_carrier != dep_carrier && return false
    (sp & MCT_BIT_ARR_BODY)    != 0 && rec.arr_body    != arr_body    && return false
    (sp & MCT_BIT_DEP_BODY)    != 0 && rec.dep_body    != dep_body    && return false
    (sp & MCT_BIT_PRV_STN)     != 0 && rec.prv_stn     != prv_stn     && return false
    (sp & MCT_BIT_NXT_STN)     != 0 && rec.nxt_stn     != nxt_stn     && return false
    (sp & MCT_BIT_ARR_TERM)    != 0 && rec.arr_term    != arr_term    && return false
    (sp & MCT_BIT_DEP_TERM)    != 0 && rec.dep_term    != dep_term    && return false
    # Region matching: deferred to Task 4 (no region arg passed yet); treat as wildcard

    # Codeshare matching: 'Y' means MCT applies to codeshare flights
    if (sp & MCT_BIT_DEP_CS_IND) != 0
        rec.dep_cs_ind == 'Y' && !dep_is_codeshare && return false
    end
    if (sp & MCT_BIT_DEP_CS_OP) != 0
        rec.dep_cs_op_carrier != dep_op_carrier && return false
    end
    if (sp & MCT_BIT_ARR_CS_IND) != 0
        rec.arr_cs_ind == 'Y' && !arr_is_codeshare && return false
    end
    if (sp & MCT_BIT_ARR_CS_OP) != 0
        rec.arr_cs_op_carrier != arr_op_carrier && return false
    end

    # Flight number range matching
    if (sp & MCT_BIT_ARR_FLT_RNG) != 0
        (arr_flt_no < rec.arr_flt_rng_start || arr_flt_no > rec.arr_flt_rng_end) && return false
    end
    if (sp & MCT_BIT_DEP_FLT_RNG) != 0
        (dep_flt_no < rec.dep_flt_rng_start || dep_flt_no > rec.dep_flt_rng_end) && return false
    end

    # Aircraft type matching
    (sp & MCT_BIT_ARR_ACFT_TYPE) != 0 && rec.arr_acft_type != arr_acft_type && return false
    (sp & MCT_BIT_DEP_ACFT_TYPE) != 0 && rec.dep_acft_type != dep_acft_type && return false

    # State matching
    (sp & MCT_BIT_PRV_STATE) != 0 && rec.prv_state != prv_state && return false
    (sp & MCT_BIT_NXT_STATE) != 0 && rec.nxt_state != nxt_state && return false

    # Country matching (fixed — was incorrectly comparing against carrier)
    (sp & MCT_BIT_PRV_COUNTRY) != 0 && rec.prv_country != prv_country && return false
    (sp & MCT_BIT_NXT_COUNTRY) != 0 && rec.nxt_country != nxt_country && return false

    return true
end

# ── MCT lookup cascade ────────────────────────────────────────────────────────

"""
    `function lookup_mct(lookup::MCTLookup, arr_carrier, dep_carrier, station, status; kwargs...)::MCTResult`
---

# Description
- Perform a hierarchical SSIM8 MCT cascade lookup for a connection at `station`
- Cascade order:
  1. Station-specific exception records (non-suppressed, non-standard), tried in
     descending specificity order — first match wins
  2. Station-specific suppression records — if any matching suppression is found,
     return a suppressed `MCTResult` with `time = 0`
  3. Station standard record — the generic station-level default
  4. Global default — `lookup.global_defaults[Int(status)]`
- The `status` integer value (1–4) directly indexes the `NTuple` of record vectors

# Arguments
1. `lookup::MCTLookup`: the populated in-memory lookup structure
2. `arr_carrier::AirlineCode`: arriving flight carrier
3. `dep_carrier::AirlineCode`: departing flight carrier
4. `station::StationCode`: connecting station code
5. `status::MCTStatus`: connection traffic type (MCT_DD, MCT_DI, MCT_ID, MCT_II)

# Keyword Arguments
- `arr_body::Char=' '`: arriving aircraft body type
- `dep_body::Char=' '`: departing aircraft body type
- `prv_stn::StationCode=NO_STATION`: origin of arriving flight
- `nxt_stn::StationCode=NO_STATION`: destination of departing flight
- `arr_term::InlineString3=InlineString3("")`: arrival terminal
- `dep_term::InlineString3=InlineString3("")`: departure terminal

# Returns
- `::MCTResult`: MCT time, source label, specificity score, and suppression flag

# Examples
```julia
julia> lookup = MCTLookup();
julia> result = lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                           StationCode("ORD"), MCT_DD);
julia> result.time == Minutes(60)
true
```
"""
function lookup_mct(
    lookup::MCTLookup,
    arr_carrier::AirlineCode,
    dep_carrier::AirlineCode,
    station::StationCode,
    status::MCTStatus;
    arr_body::Char = ' ',
    dep_body::Char = ' ',
    prv_stn::StationCode = NO_STATION,
    nxt_stn::StationCode = NO_STATION,
    arr_term::InlineString3 = InlineString3(""),
    dep_term::InlineString3 = InlineString3(""),
)::MCTResult
    status_idx = Int(status)   # 1=DD, 2=DI, 3=ID, 4=II

    # ── No station-level records: return global default ──────────────────────
    if !haskey(lookup.stations, station)
        return MCTResult(
            time           = lookup.global_defaults[status_idx],
            queried_status = status,
            matched_status = status,
            suppressed     = false,
            source         = SOURCE_GLOBAL_DEFAULT,
            specificity    = UInt32(0),
            mct_id         = Int32(0),
        )
    end

    records = lookup.stations[station][status_idx]

    # ── Pass 1: exceptions (non-standard, non-suppressed) ────────────────────
    # Records are pre-sorted by descending specificity so the first match is
    # the most specific one.
    for rec in records
        rec.station_standard && continue   # skip standard records in this pass
        rec.suppressed && continue         # skip suppression records in this pass
        _mct_record_matches(
            rec, arr_carrier, dep_carrier, arr_body, dep_body,
            prv_stn, nxt_stn, arr_term, dep_term,
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE,
            false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        ) || continue
        return MCTResult(
            time           = rec.time,
            queried_status = status,
            matched_status = status,
            suppressed     = false,
            source         = SOURCE_EXCEPTION,
            specificity    = rec.specificity,
            mct_id         = rec.mct_id,
        )
    end

    # ── Pass 2: suppressions ─────────────────────────────────────────────────
    for rec in records
        rec.suppressed || continue
        _mct_record_matches(
            rec, arr_carrier, dep_carrier, arr_body, dep_body,
            prv_stn, nxt_stn, arr_term, dep_term,
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE,
            false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        ) || continue
        return MCTResult(
            time           = Minutes(0),
            queried_status = status,
            matched_status = status,
            suppressed     = true,
            source         = SOURCE_EXCEPTION,
            specificity    = rec.specificity,
            mct_id         = rec.mct_id,
        )
    end

    # ── Pass 3: station standard ──────────────────────────────────────────────
    for rec in records
        rec.station_standard || continue
        # Station standards are wildcards by definition; skip match check.
        return MCTResult(
            time           = rec.time,
            queried_status = status,
            matched_status = status,
            suppressed     = false,
            source         = SOURCE_STATION_STANDARD,
            specificity    = rec.specificity,
            mct_id         = rec.mct_id,
        )
    end

    # ── Pass 4: global default ────────────────────────────────────────────────
    MCTResult(
        time           = lookup.global_defaults[status_idx],
        queried_status = status,
        matched_status = status,
        suppressed     = false,
        source         = SOURCE_GLOBAL_DEFAULT,
        specificity    = UInt32(0),
        mct_id         = Int32(0),
    )
end

# ── DuckDB materialization ────────────────────────────────────────────────────

"""
    `function _mct_status_from_string(s::AbstractString)::MCTStatus`

Convert a 2-character status string ("DD", "DI", "ID", "II") to the
corresponding `MCTStatus` enum value.  Returns `MCT_DD` for unrecognised input.
"""
function _mct_status_from_string(s::AbstractString)::MCTStatus
    s == "DD" && return MCT_DD
    s == "DI" && return MCT_DI
    s == "ID" && return MCT_ID
    s == "II" && return MCT_II
    MCT_DD
end

"""
    `function _build_mct_record(r)::Tuple{StationCode, MCTStatus, MCTRecord}`

Convert a single DuckDB `mct` table row `r` into a station key, status index,
and populated `MCTRecord`.  Called during `materialize_mct_lookup`.
"""
function _build_mct_record(r)::Tuple{StationCode, MCTStatus, MCTRecord}
    arr_stn_str = _safe_string(r.arr_stn)
    status_str  = _safe_string(r.mct_status)
    status      = _mct_status_from_string(status_str)

    # Station key: prefer arr_stn; fall back to dep_stn for dep-station records
    station_key = StationCode(isempty(arr_stn_str) ? _safe_string(r.dep_stn) : arr_stn_str)

    arr_carrier_str = strip(_safe_string(r.arr_carrier))
    dep_carrier_str = strip(_safe_string(r.dep_carrier))
    arr_term_str    = strip(_safe_string(r.arr_term))
    dep_term_str    = strip(_safe_string(r.dep_term))
    prv_stn_str     = strip(_safe_string(r.prv_stn))
    nxt_stn_str     = strip(_safe_string(r.nxt_stn))
    prv_ctry_str    = strip(_safe_string(r.prv_ctry))
    nxt_ctry_str    = strip(_safe_string(r.nxt_ctry))
    prv_rgn_str     = strip(_safe_string(r.prv_rgn))
    nxt_rgn_str     = strip(_safe_string(r.nxt_rgn))
    arr_body_str    = strip(_safe_string(r.arr_acft_body))
    dep_body_str    = strip(_safe_string(r.dep_acft_body))

    # Build specified bitmask
    sp = UInt32(0)
    !isempty(arr_carrier_str) && (sp |= MCT_BIT_ARR_CARRIER)
    !isempty(dep_carrier_str) && (sp |= MCT_BIT_DEP_CARRIER)
    !isempty(arr_term_str)    && (sp |= MCT_BIT_ARR_TERM)
    !isempty(dep_term_str)    && (sp |= MCT_BIT_DEP_TERM)
    !isempty(prv_stn_str)     && (sp |= MCT_BIT_PRV_STN)
    !isempty(nxt_stn_str)     && (sp |= MCT_BIT_NXT_STN)
    !isempty(prv_ctry_str)    && (sp |= MCT_BIT_PRV_COUNTRY)
    !isempty(nxt_ctry_str)    && (sp |= MCT_BIT_NXT_COUNTRY)
    !isempty(prv_rgn_str)     && (sp |= MCT_BIT_PRV_REGION)
    !isempty(nxt_rgn_str)     && (sp |= MCT_BIT_NXT_REGION)
    !isempty(arr_body_str)    && (sp |= MCT_BIT_ARR_BODY)
    !isempty(dep_body_str)    && (sp |= MCT_BIT_DEP_BODY)

    mct_id_val = Int32(_safe_missing(r.mct_id, 0))

    rec_partial = MCTRecord(
        arr_carrier    = isempty(arr_carrier_str) ? NO_AIRLINE : AirlineCode(arr_carrier_str),
        dep_carrier    = isempty(dep_carrier_str) ? NO_AIRLINE : AirlineCode(dep_carrier_str),
        arr_term       = InlineString3(isempty(arr_term_str) ? "" : arr_term_str),
        dep_term       = InlineString3(isempty(dep_term_str) ? "" : dep_term_str),
        prv_stn        = isempty(prv_stn_str) ? NO_STATION : StationCode(prv_stn_str),
        nxt_stn        = isempty(nxt_stn_str) ? NO_STATION : StationCode(nxt_stn_str),
        prv_country    = InlineString3(isempty(prv_ctry_str) ? "" : prv_ctry_str),
        nxt_country    = InlineString3(isempty(nxt_ctry_str) ? "" : nxt_ctry_str),
        prv_region     = InlineString3(isempty(prv_rgn_str) ? "" : prv_rgn_str),
        nxt_region     = InlineString3(isempty(nxt_rgn_str) ? "" : nxt_rgn_str),
        arr_body       = isempty(arr_body_str) ? ' ' : arr_body_str[1],
        dep_body       = isempty(dep_body_str) ? ' ' : dep_body_str[1],
        specified      = sp,
        time           = Int16(_safe_missing(r.time_minutes, 0)),
        suppressed     = Bool(_safe_missing(r.suppress, false)),
        station_standard = Bool(_safe_missing(r.station_standard, false)),
        specificity    = UInt32(0),   # recomputed below
        mct_id         = mct_id_val,
    )

    # Recompute specificity now that specified bitmask is set
    spec = _compute_specificity(rec_partial)
    rec = MCTRecord(
        arr_carrier    = rec_partial.arr_carrier,
        dep_carrier    = rec_partial.dep_carrier,
        arr_term       = rec_partial.arr_term,
        dep_term       = rec_partial.dep_term,
        prv_stn        = rec_partial.prv_stn,
        nxt_stn        = rec_partial.nxt_stn,
        prv_country    = rec_partial.prv_country,
        nxt_country    = rec_partial.nxt_country,
        prv_region     = rec_partial.prv_region,
        nxt_region     = rec_partial.nxt_region,
        arr_body       = rec_partial.arr_body,
        dep_body       = rec_partial.dep_body,
        specified      = sp,
        time           = rec_partial.time,
        suppressed     = rec_partial.suppressed,
        station_standard = rec_partial.station_standard,
        specificity    = spec,
        mct_id         = mct_id_val,
    )

    (station_key, status, rec)
end

"""
    `function materialize_mct_lookup(store::DuckDBStore, active_stations::Set{StationCode}; constraints::SearchConstraints=SearchConstraints())::MCTLookup`
---

# Description
- Bulk-fetch all MCT records relevant to `active_stations` from the DuckDB `mct`
  table and materialise them into an in-memory `MCTLookup`
- Records are grouped by station code and `MCTStatus`, then sorted within each
  group by descending `specificity` so that `lookup_mct` can do first-match-wins
  iteration without any sorting at query time
- Only records whose `arr_stn` OR `dep_stn` appears in `active_stations` are
  fetched, bounding memory for large MCT datasets
- Suppression records (`suppress = true`) are always included regardless of
  `min_mct_override` / `max_mct_override`, because they block connections
  semantically and must not be filtered out

# Arguments
1. `store::DuckDBStore`: populated store (must have MCT records loaded)
2. `active_stations::Set{StationCode}`: set of station codes present in the graph

# Keyword Arguments
- `constraints::SearchConstraints=SearchConstraints()`: search constraints whose
  `defaults.min_mct_override` and `defaults.max_mct_override` bound the
  `time_minutes` of non-suppression records fetched; `NO_MINUTES` (-1) disables
  the corresponding bound

# Returns
- `::MCTLookup`: fully populated lookup structure ready for `lookup_mct` calls

# Examples
```julia
julia> lookup = materialize_mct_lookup(store, Set([StationCode("ORD")]));
julia> haskey(lookup.stations, StationCode("ORD"))
true
```
"""
function materialize_mct_lookup(
    store::DuckDBStore,
    active_stations::Set{StationCode};
    constraints::SearchConstraints = SearchConstraints(),
)::MCTLookup
    # Build an IN-list of station codes for the SQL predicate.
    # For large sets we use a parameterised ANY approach; for simplicity here
    # we embed quoted literals (codes are short, controlled IATA strings).
    if isempty(active_stations)
        return MCTLookup()
    end

    min_mct = Int(constraints.defaults.min_mct_override)
    max_mct = Int(constraints.defaults.max_mct_override)

    quoted = join(["'" * String(s) * "'" for s in active_stations], ", ")
    sql = """
        SELECT
            mct_id,
            arr_stn, dep_stn, mct_status, time_minutes, suppress, station_standard,
            arr_carrier, dep_carrier,
            arr_acft_body, dep_acft_body,
            arr_term, dep_term,
            prv_stn, nxt_stn,
            prv_ctry, nxt_ctry,
            prv_rgn, nxt_rgn
        FROM mct
        WHERE (arr_stn IN ($quoted) OR dep_stn IN ($quoted))
          AND (suppress = true OR (
              (time_minutes >= ? OR ? = -1)
              AND (time_minutes <= ? OR ? = -1)
          ))
    """

    result = DBInterface.execute(store.db, sql, [min_mct, min_mct, max_mct, max_mct])

    # Accumulate into a staging dict: station → status_idx → Vector{MCTRecord}
    staging = Dict{StationCode, NTuple{4, Vector{MCTRecord}}}()

    for r in result
        station_key, status, rec = _build_mct_record(r)
        station_key == NO_STATION && continue   # malformed row: skip

        if !haskey(staging, station_key)
            staging[station_key] = (
                Vector{MCTRecord}(),
                Vector{MCTRecord}(),
                Vector{MCTRecord}(),
                Vector{MCTRecord}(),
            )
        end

        status_idx = Int(status)
        push!(staging[station_key][status_idx], rec)
    end

    # Sort each vector by descending specificity for first-match-wins lookup
    for (_, vecs) in staging
        for vec in vecs
            sort!(vec; by = r -> r.specificity, rev = true)
        end
    end

    MCTLookup(stations = staging)
end

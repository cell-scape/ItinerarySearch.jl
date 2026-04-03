# MCT Lookup: Implementation Guide

How ItinerarySearch.jl implements the SSIM Chapter 8 Minimum Connecting Time cascade, from file ingest through in-memory lookup.

**Reference documents:**
- SSIM 2021, Chapter 8 — *Presentation, Application, and Transfer of Minimum Connecting Time (MCT) Data* (`docs/reference/ssim2021_chapter-8-mctstandards.pdf`)
- MCT Technical Guide v2.2 (`docs/reference/minimum-connecting-time-technical-guide_version-2.2.pdf`)
- MCT Record Layouts (`docs/reference/mct_record_layouts.md`)
- MCT Connection Building Rules (`docs/reference/mct_connection_building.md`)

**Source files:**
- `src/ingest/mct.jl` — Fixed-width file parsing and DuckDB ingest
- `src/graph/mct_lookup.jl` — In-memory MCTLookup structure, specificity computation, field matching, and the 3-pass lookup cascade
- `src/graph/rules_cnx.jl` — MCTRule callable with codeshare multi-pass and Schengen resolution

---

## Background: What MCT Solves

An MCT is "the shortest time interval required in order to transfer a passenger and his luggage from one flight to a connecting flight, in a specific location" (SSIM Ch. 8, Section 8.1). Airlines file MCTs bilaterally via data aggregators, and IATA publishes station-level defaults. The MCT system must determine, for every potential connection at every airport, what minimum connecting time applies.

Chapter 8 defines a **three-tier hierarchy** of MCT records (Section 8.3):

1. **MCT Exceptions** — carrier-specific rules filed bilaterally between airlines. The most specific tier. Can set shorter or longer times than the station standard.
2. **Station Standards** — airport-level defaults published by IATA with no carrier specificity. Override global defaults.
3. **Global Defaults** — hardcoded IATA fallback values when no station-level records exist.

Within the exception tier, Chapter 8 Section 8.6 defines a **28-level priority hierarchy** that determines which of multiple matching exception records takes precedence. A record that specifies departure carrier + terminal is more specific than one that specifies only departure carrier.

Additionally, **suppression records** (`Suppression Indicator = Y`) can block connections entirely for specific combinations.

---

## Phase 1: Ingest — Parsing the MCT File

**Source:** `src/ingest/mct.jl`

The MCT file is fixed-width, 200 bytes per line. Record types (byte 1):
- `1` — Header (skipped)
- `2` — MCT rule (the actual data, parsed below)
- `3` — Connection Building Filter partner list (post-01NOV22)
- `4` — Trailer (post-01NOV22; was Type 3 before)

See `docs/reference/mct_record_layouts.md` for the complete byte-position layout.

### Entry point: `ingest_mct!`

```
src/ingest/mct.jl:76-139
```

```julia
function ingest_mct!(store::DuckDBStore, path::String; ...)
    io = open_maybe_compressed(path)       # Transparent gzip/zstd/bzip2/xz
    appender = DuckDB.Appender(store.db, "mct")

    for line in eachline(io)
        rt = line[1]
        if rt == '2' && length(line) >= 96  # Type 2 = MCT rule
            arr_stn_raw = strip(line[2:4])  # Bytes 2-4: arrival station
            dep_stn_raw = strip(line[11:13]) # Bytes 11-13: departure station
            # ... optional station/carrier filtering ...
            _append_mct!(appender, mct_id, line)
        end
    end
end
```

Each Type 2 line is parsed by `_append_mct!` (`mct.jl:141-213`), which extracts all 39 columns by byte position. Key fields extracted (byte positions from `mct_record_layouts.md`, corresponding to the SSIM Ch. 8, Section 8.9.2 Record Type 2):

| Bytes | Field | Code variable |
|-------|-------|---------------|
| 2-4 | Arrival Station | `arr_stn` |
| 5-8 | Time (HHMM) | `time_minutes` (converted to integer minutes) |
| 9-10 | Int'l/Domestic Status | `mct_status` (DD, DI, ID, or II) |
| 11-13 | Departure Station | `dep_stn` |
| 14-15 | Arrival Carrier | `arr_carrier` |
| 16 | Arrival Codeshare Indicator | `arr_cs_ind` |
| 17-18 | Arrival Codeshare Operating Carrier | `arr_cs_op_carrier` |
| 19-20 | Departure Carrier | `dep_carrier` |
| 21 | Departure Codeshare Indicator | `dep_cs_ind` |
| 22-23 | Departure Codeshare Operating Carrier | `dep_cs_op_carrier` |
| 24-26 | Arrival Aircraft Type | `arr_acft_type` |
| 27 | Arrival Aircraft Body | `arr_acft_body` |
| 28-30 | Departure Aircraft Type | `dep_acft_type` |
| 31 | Departure Aircraft Body | `dep_acft_body` |
| 32-33 | Arrival Terminal | `arr_term` |
| 34-35 | Departure Terminal | `dep_term` |
| 36-37 | Previous Country | `prv_ctry` |
| 38-40 | Previous Station | `prv_stn` |
| 41-42 | Next Country | `nxt_ctry` |
| 43-45 | Next Station | `nxt_stn` |
| 46-49 | Arrival Flt Range Start | `arr_flt_rng_start` |
| 50-53 | Arrival Flt Range End | `arr_flt_rng_end` |
| 54-57 | Departure Flt Range Start | `dep_flt_rng_start` |
| 58-61 | Departure Flt Range End | `dep_flt_rng_end` |
| 62-63 | Previous State | `prv_state` |
| 64-65 | Next State | `nxt_state` |
| 66-68 | Previous Region | `prv_rgn` |
| 69-71 | Next Region | `nxt_rgn` |
| 72-78 | Effective From Date | `eff_date` |
| 79-85 | Effective To Date | `dis_date` |
| 87 | Suppression Indicator | `suppress` |
| 88-90 | Suppression Region | `supp_rgn` |
| 91-92 | Suppression Country | `supp_ctry` |
| 93-94 | Suppression State | `supp_state` |
| 95-96 | Submitting Carrier | `submitting_carrier` |

### Station Standard Detection

A critical classification happens during ingest (`mct.jl:152`):

```julia
is_station_standard = isempty(arr_carrier) && isempty(dep_carrier) && isempty(submitting)
```

Per SSIM Ch. 8, Section 8.6: a **station standard** has no carrier specificity — it is the airport-published default. Any record with at least one carrier field populated is an **exception**, filed bilaterally between carriers. This flag controls which pass of the lookup cascade handles the record.

The `specificity` column is stored as `0` during ingest and computed during materialization (Phase 2).

### Optional Filtering

When the graph builder calls `ingest_mct!`, it can pass `station_filter` and `carrier_filter` sets to pre-filter records. Records for stations not in the current schedule, or carrier-specific records referencing carriers not in the schedule, are skipped. Station standards are always kept regardless of carrier filter.

---

## Phase 2: Materialization — Building the In-Memory Lookup

**Source:** `src/graph/mct_lookup.jl`

Before search begins, `materialize_mct_lookup` (`mct_lookup.jl:838-909`) bulk-fetches all relevant MCT records from DuckDB and builds an optimized in-memory structure.

### The MCTLookup Structure

```
mct_lookup.jl:222-228
```

```julia
@kwdef struct MCTLookup
    stations::Dict{Tuple{StationCode,StationCode}, NTuple{4, Vector{MCTRecord}}}
    global_defaults::NTuple{4, Minutes} = (60, 90, 90, 120)  # DD, DI, ID, II
    inter_station_default::Minutes = 240                       # 4 hours
end
```

Records are indexed by **station pair** `(arr_stn, dep_stn)`, then subdivided into 4 vectors — one per MCTStatus (DD=1, DI=2, ID=3, II=4). This structure enables O(1) lookup to the correct set of candidate records for any connection.

When both stations in the key are the same, it is an **intra-station** connection (the normal case — passenger connects within one airport). When they differ, it is an **inter-station** connection (multi-airport city, e.g., arriving at JFK and departing from EWR in the New York metro area).

The global defaults correspond to SSIM Ch. 8's opening section values:
- DD (Domestic-Domestic): 60 min
- DI (Domestic-International): 90 min
- ID (International-Domestic): 90 min
- II (International-International): 120 min
- Inter-station (all types): 240 min (4 hours)

### The MCTRecord Struct

```
mct_lookup.jl:131-184
```

Each in-memory record stores all matching fields plus metadata:

```julia
@kwdef struct MCTRecord
    # Matching fields (sentinels like NO_AIRLINE mean "not specified" / wildcard)
    arr_carrier::AirlineCode  = NO_AIRLINE
    dep_carrier::AirlineCode  = NO_AIRLINE
    arr_term::InlineString3   = InlineString3("")
    dep_term::InlineString3   = InlineString3("")
    prv_stn::StationCode      = NO_STATION
    nxt_stn::StationCode      = NO_STATION
    # ... 16 more matching fields (codeshare, aircraft, geography, dates) ...

    specified::UInt32 = UInt32(0)    # Bitmask: which fields must match
    time::Minutes     = Minutes(0)   # MCT in minutes (0 if suppressed)
    suppressed::Bool  = false        # Blocks connection entirely
    station_standard::Bool = false   # No carrier specificity
    specificity::UInt32 = UInt32(0)  # Pre-computed sort weight
    record_serial::UInt32 = UInt32(0) # SSIM file serial — tiebreaker at equal specificity
    mct_id::Int32 = Int32(0)         # PK from DuckDB mct table
end
```

The `specified` bitmask is the key design choice. A blank field in the MCT file means "wildcard — accept any value." A populated field means "must match exactly." The bitmask encodes which fields were populated, using 22 bit positions defined as constants:

```
mct_lookup.jl:54-75
```

```julia
const MCT_BIT_ARR_CARRIER   = UInt32(1 << 0)
const MCT_BIT_DEP_CARRIER   = UInt32(1 << 1)
const MCT_BIT_ARR_TERM      = UInt32(1 << 2)
const MCT_BIT_DEP_TERM      = UInt32(1 << 3)
const MCT_BIT_PRV_STN       = UInt32(1 << 4)
const MCT_BIT_NXT_STN       = UInt32(1 << 5)
const MCT_BIT_PRV_COUNTRY   = UInt32(1 << 6)
const MCT_BIT_NXT_COUNTRY   = UInt32(1 << 7)
const MCT_BIT_PRV_REGION    = UInt32(1 << 8)
const MCT_BIT_NXT_REGION    = UInt32(1 << 9)
const MCT_BIT_DEP_BODY      = UInt32(1 << 10)
const MCT_BIT_ARR_BODY      = UInt32(1 << 11)
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
```

### Building the Specified Bitmask

When each DuckDB row is converted to an `MCTRecord` via `_build_mct_record` (`mct_lookup.jl:673-800`), the bitmask is built by testing each field for non-empty content:

```julia
sp = UInt32(0)
!isempty(arr_carrier_str) && (sp |= MCT_BIT_ARR_CARRIER)
!isempty(dep_carrier_str) && (sp |= MCT_BIT_DEP_CARRIER)
!isempty(arr_term_str)    && (sp |= MCT_BIT_ARR_TERM)
!isempty(dep_term_str)    && (sp |= MCT_BIT_DEP_TERM)
!isempty(prv_stn_str)     && (sp |= MCT_BIT_PRV_STN)
!isempty(nxt_stn_str)     && (sp |= MCT_BIT_NXT_STN)
# ... etc for all 22 fields
```

### Computing Specificity

```
mct_lookup.jl:249-287
```

`_compute_specificity` converts the bitmask into a weighted score that encodes the Chapter 8 Section 8.6 priority hierarchy. Higher bits in the output UInt32 correspond to higher-priority fields in the SSIM hierarchy:

```julia
function _compute_specificity(specified::UInt32, eff_date::UInt32)::UInt32
    s = UInt32(0)
    # Priority 1-3: Departure codeshare indicator + carrier + operating carrier
    (sp & MCT_BIT_DEP_CS_IND)    != 0 && (s += UInt32(1) << 29)
    (sp & MCT_BIT_DEP_CARRIER)   != 0 && (s += UInt32(1) << 28)
    (sp & MCT_BIT_DEP_CS_OP)     != 0 && (s += UInt32(1) << 27)
    # Priority 4-6: Arrival codeshare indicator + carrier + operating carrier
    (sp & MCT_BIT_ARR_CS_IND)    != 0 && (s += UInt32(1) << 26)
    (sp & MCT_BIT_ARR_CARRIER)   != 0 && (s += UInt32(1) << 25)
    (sp & MCT_BIT_ARR_CS_OP)     != 0 && (s += UInt32(1) << 24)
    # Priority 7-10: Flight number ranges
    (sp & MCT_BIT_DEP_FLT_RNG)   != 0 && (s += UInt32(1) << 23)
    (sp & MCT_BIT_ARR_FLT_RNG)   != 0 && (s += UInt32(1) << 22)
    # Priority 11-12: Terminals
    (sp & MCT_BIT_DEP_TERM)      != 0 && (s += UInt32(1) << 21)
    (sp & MCT_BIT_ARR_TERM)      != 0 && (s += UInt32(1) << 20)
    # Priority 13-14: Next/Previous station
    (sp & MCT_BIT_NXT_STN)       != 0 && (s += UInt32(1) << 19)
    (sp & MCT_BIT_PRV_STN)       != 0 && (s += UInt32(1) << 18)
    # Priority 15-16: Next/Previous state
    (sp & MCT_BIT_NXT_STATE)     != 0 && (s += UInt32(1) << 17)
    (sp & MCT_BIT_PRV_STATE)     != 0 && (s += UInt32(1) << 16)
    # Priority 17-18: Next/Previous country
    (sp & MCT_BIT_NXT_COUNTRY)   != 0 && (s += UInt32(1) << 15)
    (sp & MCT_BIT_PRV_COUNTRY)   != 0 && (s += UInt32(1) << 14)
    # Priority 19-20: Next/Previous region
    (sp & MCT_BIT_NXT_REGION)    != 0 && (s += UInt32(1) << 13)
    (sp & MCT_BIT_PRV_REGION)    != 0 && (s += UInt32(1) << 12)
    # Priority 21-22: Aircraft type
    (sp & MCT_BIT_DEP_ACFT_TYPE) != 0 && (s += UInt32(1) << 11)
    (sp & MCT_BIT_ARR_ACFT_TYPE) != 0 && (s += UInt32(1) << 10)
    # Priority 23-24: Aircraft body (W/N)
    (sp & MCT_BIT_DEP_BODY)      != 0 && (s += UInt32(1) << 9)
    (sp & MCT_BIT_ARR_BODY)      != 0 && (s += UInt32(1) << 8)
    # Priority 25-26: Effective dates
    (eff_date != UInt32(0))              && (s += UInt32(1) << 7)
    return s
end
```

This encoding mirrors the SSIM Ch. 8, Section 8.6 hierarchy table (p. 394-395), which states:

> *"The priority order is listed from the most applicable to the least applicable in ascending order, irrespective of the number of data elements. For example, priority 11 (Departure Terminal) will take precedence over priority 12 (Arrival Terminal). When the priority between two MCT records is determined by the hierarchy, the most applicable column that contains data for exactly one record is used. The record with the non-empty column takes priority over the one with the empty column."*

The mapping from Chapter 8 priority numbers to code bit positions:

| Ch. 8 Priority | Data Element | Code Bit Position |
|---|---|---|
| # (implicit) | Int'l/Domestic Status | Handled by status_idx (separate vector per status) |
| 1 | Departure Codeshare Indicator | bit 29 |
| 2 | Departure Carrier | bit 28 |
| 3 | Departure Codeshare Operating Carrier | bit 27 |
| 4 | Arrival Codeshare Indicator | bit 26 |
| 5 | Arrival Carrier | bit 25 |
| 6 | Arrival Codeshare Operating Carrier | bit 24 |
| 7-8 | Departure Flight Number Range | bit 23 |
| 9-10 | Arrival Flight Number Range | bit 22 |
| 11 | Departure Terminal | bit 21 |
| 12 | Arrival Terminal | bit 20 |
| 13 | Next Station | bit 19 |
| 14 | Previous Station | bit 18 |
| 15 | Next State | bit 17 |
| 16 | Previous State | bit 16 |
| 17 | Next Country | bit 15 |
| 18 | Previous Country | bit 14 |
| 19 | Next Region | bit 13 |
| 20 | Previous Region | bit 12 |
| 21 | Departure Aircraft Type | bit 11 |
| 22 | Arrival Aircraft Type | bit 10 |
| 23 | Departure Aircraft Body | bit 9 |
| 24 | Arrival Aircraft Body | bit 8 |
| 25-26 | Effective Dates | bit 7 |
| 27 | Departure Station | (implicit — part of the Dict key) |
| 28 | Arrival Station | (implicit — part of the Dict key) |

International/Domestic status (the unnumbered top priority) is handled structurally: records are partitioned into 4 separate vectors (DD, DI, ID, II), so the status is already resolved before any specificity comparison.

Departure and Arrival station (priorities 27-28) are also structural — they form the Dict key `(arr_stn, dep_stn)` that selects which vectors to search.

### Pre-Sorting for First-Match-Wins

After all records are loaded, each vector is sorted by a composite key of `(specificity, record_serial)`, both descending (`mct_lookup.jl:889-896`):

```julia
for (_, vecs) in staging
    for vec in vecs
        sort!(vec; by = r -> (r.specificity, r.record_serial), rev = true)
    end
end
```

This pre-sort is the key optimization: during the lookup cascade, the first matching record is guaranteed to be the most specific applicable one, so there is no need for further comparison or scoring at query time.

When two records have equal specificity, the serial number breaks ties. The direction is configurable via `SearchConfig.mct_serial_ascending`:

- **`true` (default)**: lower serial (earlier record) wins — this matches the team's existing expectations
- **`false`**: higher serial (later record) wins — per the literal reading of SSIM Ch. 8, Section 8.5.2: *"Whenever new data is received, the information contained supersedes previously received data."*

In practice the difference is small (~1,800 connections out of 1.8M on the full UA schedule), but configurable for validation.

---

## Phase 3: The Lookup Cascade — `lookup_mct`

**Source:** `src/graph/mct_lookup.jl:479-645`

This function is called during the O(n²) connection build for every candidate connection between an arriving leg and a departing leg at a station. It implements the SSIM Chapter 8 cascade in 3 passes.

### Function Signature

```julia
function lookup_mct(
    lookup::MCTLookup,
    arr_carrier::AirlineCode,      # Marketing carrier of arriving flight
    dep_carrier::AirlineCode,      # Marketing carrier of departing flight
    arr_station::StationCode,      # Station where arriving flight lands
    dep_station::StationCode,      # Station where departing flight departs
    status::MCTStatus;             # DD, DI, ID, or II
    # 17 keyword arguments for optional matching fields:
    arr_body, dep_body,            # Aircraft body type (W/N)
    prv_stn, nxt_stn,             # Origin/destination of arr/dep flights
    arr_term, dep_term,            # Arrival/departure terminals
    arr_op_carrier, dep_op_carrier, # Operating carriers (from DEI 50)
    arr_is_codeshare, dep_is_codeshare, # Is flight a codeshare?
    arr_acft_type, dep_acft_type,  # IATA aircraft type codes
    arr_flt_no, dep_flt_no,        # Flight numbers
    prv_country, nxt_country,      # Countries of origin/destination
    prv_state, nxt_state,          # States of origin/destination
    prv_region, nxt_region,        # IATA regions of origin/destination
    target_date,                   # Connection date (packed YYYYMMDD)
)::MCTResult
```

### Step 0: Station-Pair Lookup

```
mct_lookup.jl:516-532
```

```julia
key = (arr_station, dep_station)
if !haskey(lookup.stations, key)
    default_time = arr_station == dep_station ?
        lookup.global_defaults[status_idx] :    # DD=60, DI=90, ID=90, II=120
        lookup.inter_station_default            # 240 min (4 hours)
    return MCTResult(source = SOURCE_GLOBAL_DEFAULT, ...)
end
records = lookup.stations[key][status_idx]
```

If no records exist for this station pair and status, the function immediately returns the appropriate global default. For most smaller stations this is the common path.

### Pass 1: Exceptions and Suppressions (unified by specificity)

```
mct_lookup.jl:536-595
```

```julia
for rec in records
    rec.station_standard && continue   # Skip standards — handled in Pass 2

    # Date validity: skip records outside their effective window
    if rec.eff_date != UInt32(0) && target_date != UInt32(0)
        (target_date < rec.eff_date || target_date > rec.dis_date) && continue
    end

    # Field matching: every specified field must match
    _mct_record_matches(rec, arr_carrier, dep_carrier, ...) || continue

    if rec.suppressed
        # Suppression geography scope — only suppress if connection is in scope
        if rec.supp_region != ""
            rec.supp_region != prv_region && rec.supp_region != nxt_region && continue
        end
        # ... country and state checks ...
        return MCTResult(suppressed = true, time = 0, source = SOURCE_EXCEPTION, ...)
    end

    return MCTResult(source = SOURCE_EXCEPTION, time = rec.time, ...)
end
```

Exceptions and suppressions are scanned together in a single loop, sorted by descending `(specificity, record_serial)`. The **first match wins**, whether it's a time-setting exception or a connection-blocking suppression. This is correct per SSIM Ch. 8, Section 8.6, which lists the suppression indicator as `#` — *"not part of the hierarchy"* — meaning suppression is an attribute of a record, not a separate tier.

A suppression with carrier + codeshare indicator + flight number range specificity will correctly outrank a less-specific exception for the same carrier. For example, at BSB (Brasilia), AD files a 120-minute exception for ID status, but also a more-specific suppression for codeshare flights in the 7000-7999 range — the suppression wins for those flights because it has higher specificity.

Date validity filtering follows SSIM Ch. 8, Section 8.6 priorities 25-26: effective dates are in local time at the connection airport, and "an MCT with dates takes priority over one without dates."

The suppression geography fields (region/country/state) scope where the suppression applies. A suppression with `supp_country = "US"` only blocks connections where the arriving or departing flight involves the US. A suppression with all geography fields blank is a **global suppression** (SSIM Ch. 8, Section 8.6 priorities 27-28).

### Pass 2: Station Standard

```
mct_lookup.jl:596-614
```

```julia
for rec in records
    rec.station_standard || continue

    # Date validity
    if rec.eff_date != UInt32(0) && target_date != UInt32(0)
        (target_date < rec.eff_date || target_date > rec.dis_date) && continue
    end

    # No field matching needed — station standards are wildcards by definition
    return MCTResult(source = SOURCE_STATION_STANDARD, time = rec.time, ...)
end
```

Station standards have no carrier specificity, so there is no need to call `_mct_record_matches`. The first date-valid station standard for this status is returned directly.

### Pass 3: Global Default

```
mct_lookup.jl:616-630
```

```julia
default_time = arr_station == dep_station ?
    lookup.global_defaults[status_idx] :    # DD=60, DI=90, ID=90, II=120
    lookup.inter_station_default            # 240 min
MCTResult(source = SOURCE_GLOBAL_DEFAULT, time = default_time, ...)
```

If no exception, suppression, or station standard matched, the IATA global default is used. This is the absolute floor.

### Summary of the 3-Pass Cascade

```
                  ┌───────────────────────────┐
                  │  Station-pair records      │
                  │  exist for (arr, dep)?     │
                  └────────┬──────────────────┘
                           │ no → Pass 3: Global Default
                           │ yes
                  ┌────────▼──────────────────┐
                  │  Pass 1: Exceptions &     │
                  │  Suppressions (unified)   │
                  │  (non-standard,           │
                  │   descending specificity, │
                  │   serial tiebreaker)      │
                  └────────┬──────────────────┘
                           │ no match
                  ┌────────▼──────────────────┐
                  │  Pass 2: Station Std      │
                  │  (station_standard=true,  │
                  │   no field matching)      │
                  └────────┬──────────────────┘
                           │ no match
                  ┌────────▼──────────────────┐
                  │  Pass 3: Global Default   │
                  │  (DD=60, DI=90,           │
                  │   ID=90, II=120,          │
                  │   inter-station=240)      │
                  └───────────────────────────┘
```

---

## Field Matching: `_mct_record_matches`

**Source:** `src/graph/mct_lookup.jl:335-410`

This function is the inner loop of the cascade, called for every candidate record in Pass 1. It tests whether a record's specified fields all match the connection's attributes:

```julia
@inline function _mct_record_matches(rec::MCTRecord, ...)::Bool
    sp = rec.specified
    sp == UInt32(0) && return true   # Fast path: pure wildcard, matches everything

    # Simple equality checks — skip if field not specified (wildcard)
    (sp & MCT_BIT_ARR_CARRIER) != 0 && rec.arr_carrier != arr_carrier && return false
    (sp & MCT_BIT_DEP_CARRIER) != 0 && rec.dep_carrier != dep_carrier && return false
    (sp & MCT_BIT_ARR_BODY)    != 0 && rec.arr_body    != arr_body    && return false
    (sp & MCT_BIT_DEP_BODY)    != 0 && rec.dep_body    != dep_body    && return false
    (sp & MCT_BIT_PRV_STN)     != 0 && rec.prv_stn     != prv_stn     && return false
    (sp & MCT_BIT_NXT_STN)     != 0 && rec.nxt_stn     != nxt_stn     && return false
    (sp & MCT_BIT_ARR_TERM)    != 0 && rec.arr_term    != arr_term    && return false
    (sp & MCT_BIT_DEP_TERM)    != 0 && rec.dep_term    != dep_term    && return false
    (sp & MCT_BIT_PRV_REGION)  != 0 && rec.prv_region  != prv_region  && return false
    (sp & MCT_BIT_NXT_REGION)  != 0 && rec.nxt_region  != nxt_region  && return false
```

### Codeshare Matching

```julia
    # 'Y' means this MCT only applies to codeshare flights
    if (sp & MCT_BIT_DEP_CS_IND) != 0
        rec.dep_cs_ind == 'Y' && !dep_is_codeshare && return false
    end
    if (sp & MCT_BIT_DEP_CS_OP) != 0
        rec.dep_cs_op_carrier != dep_op_carrier && return false
    end
```

Per SSIM Ch. 8, Section 8.8, Arrival/Departure Codeshare Indicator: *"When the 'Y' is present, the MCT applies specifically to codeshare flights. Codeshare is determined by the presence of a DEI 50 on the flight schedule."* And: *"An MCT without the 'Y' will be treated as operating"* and *"A marketing (Y) flight MCT will override an operating MCT."*

### Flight Number Range Matching

```julia
    if (sp & MCT_BIT_ARR_FLT_RNG) != 0
        (arr_flt_no < rec.arr_flt_rng_start || arr_flt_no > rec.arr_flt_rng_end) && return false
    end
    if (sp & MCT_BIT_DEP_FLT_RNG) != 0
        (dep_flt_no < rec.dep_flt_rng_start || dep_flt_no > rec.dep_flt_rng_end) && return false
    end
```

Per SSIM Ch. 8, Section 8.8, Arrival Flight Number Range: *"Subset flight ranges take priority over larger range"* — this is handled by the specificity score (a subset range has the same bit set but is inherently more restrictive).

### Country, State, Aircraft Type Matching

```julia
    (sp & MCT_BIT_ARR_ACFT_TYPE) != 0 && rec.arr_acft_type != arr_acft_type && return false
    (sp & MCT_BIT_DEP_ACFT_TYPE) != 0 && rec.dep_acft_type != dep_acft_type && return false
    (sp & MCT_BIT_PRV_STATE)     != 0 && rec.prv_state     != prv_state     && return false
    (sp & MCT_BIT_NXT_STATE)     != 0 && rec.nxt_state     != nxt_state     && return false
    (sp & MCT_BIT_PRV_COUNTRY)   != 0 && rec.prv_country   != prv_country   && return false
    (sp & MCT_BIT_NXT_COUNTRY)   != 0 && rec.nxt_country   != nxt_country   && return false
    return true
end
```

Per SSIM Ch. 8, Section 8.6: regions, countries, and stations are mutually exclusive geography fields — a record specifies *either* a region, *or* a country+state, *or* a specific station for each side, not combinations.

---

## The MCT Cache

**Source:** `src/graph/mct_lookup.jl:12-41, 77-80`

During connection building, the same carrier/terminal/body combinations appear many times across different flights. The `MCTCacheKey` captures 23 matching fields but **excludes flight numbers and target date**:

```julia
struct MCTCacheKey
    arr_carrier::AirlineCode
    dep_carrier::AirlineCode
    arr_station::StationCode
    dep_station::StationCode
    status::MCTStatus
    arr_body::Char
    dep_body::Char
    prv_stn::StationCode
    nxt_stn::StationCode
    arr_term::InlineString3
    dep_term::InlineString3
    # ... 12 more fields (codeshare, aircraft type, geography)
end
```

Flight numbers and dates are excluded because they vary per-leg but rarely affect MCT. On a cache hit, the code checks whether the cached result's `matched_fields` bitmask includes flight number range bits (`_MCT_CACHE_REVALIDATE_MASK`). If so, the cache hit is discarded and a full lookup is performed. This gives approximately 77% cache hit rate in practice on full-schedule data, reducing the effective cost of the O(n²) connection build.

---

## Worked Example

A passenger connects from UA 100 (arriving from LHR on a 777W, terminal 5) to AA 200 (departing to DFW from terminal 3) at ORD. The connection is domestic-domestic (DD, status index 1).

**Step 0:** Look up `(StationCode("ORD"), StationCode("ORD"))` in `lookup.stations`. Found — proceed.

**Pass 1 (Exceptions & Suppressions):** Scan DD records in descending `(specificity, record_serial)`:
- Record A: `arr_carrier=UA, dep_carrier=AA, arr_term=5, dep_term=3, time=45min` — specificity bits: 28 (dep carrier) + 25 (arr carrier) + 21 (dep term) + 20 (arr term) = high score. Not suppressed. All fields match. **Return 45 min, SOURCE_EXCEPTION.**

If Record A had `dep_term=7` instead (mismatch), the cascade would continue to the next record — perhaps a suppression with carrier + codeshare specificity, or an exception with only carriers specified (lower specificity). Suppressions and exceptions compete in the same pass.

If no exceptions or suppressions matched:

**Pass 1b (Global Suppressions):** Check blank-station suppression records. None found for UA-AA DD.

**Pass 2 (Station Standard):** Return ORD's published DD station standard (e.g., 75 min, SOURCE_STATION_STANDARD).

If ORD had no station standard:

**Pass 3 (Global Default):** Return 60 min (DD global default), SOURCE_GLOBAL_DEFAULT.

---

## Codeshare Resolution

**Source:** `src/graph/rules_cnx.jl` — `_mct_codeshare_resolve`

For codeshare flights, the MCT lookup needs to consider both the marketing carrier (with codeshare indicators) and the operating carrier (without). Per SSIM Ch. 8 (p. 398): *"A marketing (Y) flight MCT will override an operating MCT"* and *"A marketing MCT is not necessary unless the marketing Carrier wants a longer MCT than the Codeshare Operating Carrier."*

The `MCTRule` callable in the connection builder performs codeshare-aware resolution:

1. **Marketing lookup**: Use marketing carriers with codeshare flags set. This finds codeshare-specific MCTs — records with `cs_ind=Y` and optionally `cs_op_carrier` specified.
2. **Operating lookup** (only if either leg is a codeshare): Use operating carriers without codeshare flags. This finds records filed for the operating carrier directly.
3. **Pick the best**: The result with higher specificity wins. At equal specificity, the marketing result takes precedence.

```julia
# Primary: marketing carriers + marketing flight numbers + codeshare context
marketing_result = _mct_direct_lookup(r, ctx,
    from_rec.carrier, to_rec.carrier,          # marketing carriers
    from_rec.flight_number, to_rec.flight_number, # marketing flight numbers
    ..., arr_op_carrier, dep_op_carrier,
    arr_is_codeshare, dep_is_codeshare, ...)

# Secondary: operating carriers + operating flight numbers, no codeshare flags
op_arr = arr_is_codeshare ? arr_op_carrier : from_rec.carrier
op_dep = dep_is_codeshare ? dep_op_carrier : to_rec.carrier
op_arr_flt = arr_is_codeshare ? from_rec.operating_flight_number : from_rec.flight_number
op_dep_flt = dep_is_codeshare ? to_rec.operating_flight_number : to_rec.flight_number
operating_result = _mct_direct_lookup(r, ctx,
    op_arr, op_dep, op_arr_flt, op_dep_flt,    # operating carriers + flight numbers
    ..., NO_AIRLINE, NO_AIRLINE,
    false, false, ...)

# Higher specificity wins; marketing preferred at equal specificity
if operating_result.specificity > marketing_result.specificity
    return operating_result
end
return marketing_result
```

Per SSIM Ch. 8 (p. 400): *"If both the Arrival Carrier and the Arrival Codeshare Operating Carrier are defined, then the flight number will be applied to the Arrival Carrier"* and *"If the Arrival Carrier is not defined, then the flight number will be applied to the Arrival Codeshare Operating Carrier."* The marketing lookup uses marketing flight numbers; the operating lookup uses operating flight numbers.

For non-codeshare connections, only the marketing lookup is performed — no overhead.

### Codeshare Example (from SSIM Ch. 8, p. 398)

Flights available at MIA:
- BA0207: LHR-MIA, 1040-1500
- AA6160: LHR-MIA, 1040-1500 (DEI 50 = BA0207, i.e., this is a codeshare of BA0207)
- AA2718: MIA-DFW, 1635-1857
- AA2720: MIA-DFW, 1700-1925

MCT records filed at MIA (ID status):

| arr_carrier | arr_cs_ind | arr_cs_op | dep_carrier | time |
|---|---|---|---|---|
| AA | Y | BA | AA | 0140 (MCT 1) |
| BA | | | AA | 0130 (MCT 2) |

**BA0207 → AA2718** (BA is the operating carrier, not a codeshare):
- Single lookup with `arr_carrier=BA`: MCT 2 matches. **Use 130 min.**

**AA6160 → AA2720** (AA is the marketing carrier, codeshare of BA):
- Marketing lookup with `arr_carrier=AA, arr_is_codeshare=true, arr_op_carrier=BA`: MCT 1 matches (codeshare Y + operating carrier BA). Specificity includes cs_ind + cs_op + carrier bits.
- Operating lookup with `arr_carrier=BA, arr_is_codeshare=false`: MCT 2 matches. Lower specificity (carrier only, no codeshare bits).
- MCT 1 wins (higher specificity). **Use 140 min.**

**AA6160 → AA2718:** MCT 1 applies (140 min). 1500 + 140 = 1720 > 1635. **Connection does not build** — insufficient time.

---

## Configuration Reference

All MCT-related configuration lives on `SearchConfig`. These settings control the lookup cascade behavior and can be set in the JSON config file or as keyword arguments.

| Parameter | Default | Description |
|---|---|---|
| `mct_cache_enabled` | `true` | Cache MCT lookup results during connection build (~77% hit rate) |
| `mct_serial_ascending` | `true` | Tiebreaker at equal specificity: `true` = lower serial (earlier record) wins; `false` = higher serial (later record) wins |
| `mct_codeshare_mode` | `:both` | Codeshare carrier resolution: `:both` = marketing + operating lookups (best specificity wins); `:marketing` = marketing carrier only; `:operating` = operating carrier only |
| `mct_schengen_mode` | `:sch_then_eur` | Schengen/Europe region priority: `:sch_then_eur` = SCH first, EUR fallback; `:eur_then_sch` = EUR first, SCH fallback; `:sch_only` = SCH or wildcard only; `:eur_only` = EUR or wildcard only |
| `mct_suppressions_enabled` | `true` | Include suppression records in MCT lookup; `false` = ignore all suppressions |

### Codeshare Mode Details

For codeshare flights (identified by `operating_carrier != carrier` on the leg record), the MCT rule performs one or two `lookup_mct` calls depending on the mode:

- **`:both`** — marketing lookup (marketing carrier + codeshare flags + marketing flight number), then operating lookup (operating carrier + operating flight number, no codeshare flags). Best specificity wins; marketing preferred at ties.
- **`:marketing`** — marketing lookup only. Misses MCT records filed for the operating carrier.
- **`:operating`** — operating lookup only. Misses codeshare-specific MCTs (records with `cs_ind=Y`).

### Schengen Mode Details

When a connection involves flights from/to Schengen or European stations, MCT records may be filed under region code `SCH` (Schengen) or `EUR` (Europe). The mode controls which region code is tried first:

- **`:sch_then_eur`** — use SCH as the primary region, fall back to EUR if no region-specific match found
- **`:eur_then_sch`** — use EUR as primary, fall back to SCH
- **`:sch_only`** — only match SCH records (or wildcard)
- **`:eur_only`** — only match EUR records (or wildcard)

The fallback is only triggered when the primary lookup did not match on region bits (`MCT_BIT_PRV_REGION | MCT_BIT_NXT_REGION`). This ensures an exact region match always takes priority over a fallback. Non-SCH/EUR regions are unaffected by this setting.

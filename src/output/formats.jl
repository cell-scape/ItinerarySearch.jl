# src/output/formats.jl — Human-readable display and tabular output formats
#
# Provides:
#   - Base.show methods for all graph types (human-friendly, no bitmask internals)
#   - itinerary_long_format  — one row per leg, for per-leg analysis
#   - itinerary_wide_format  — one row per itinerary, for ranking / cross-reference

# ── Base.show methods ─────────────────────────────────────────────────────────

"""
    `Base.show(io::IO, stn::GraphStation)`

Human-friendly one-line summary: code, departure/arrival/connection counts.
"""
function Base.show(io::IO, stn::GraphStation)
    n_dep = length(stn.departures)
    n_arr = length(stn.arrivals)
    n_cnx = length(stn.connections)
    print(io, "GraphStation($(stn.code), $(n_dep) dep, $(n_arr) arr, $(n_cnx) cnx)")
end

"""
    `Base.show(io::IO, leg::GraphLeg)`

Human-friendly one-line summary: flight identifier and O→D pair.
"""
function Base.show(io::IO, leg::GraphLeg)
    rec = leg.record
    print(io, "GraphLeg($(flight_id(rec)) $(rec.departure_station)→$(rec.arrival_station))")
end

"""
    `Base.show(io::IO, seg::GraphSegment)`

Human-friendly one-line summary: marketing flight number, segment endpoints,
and leg count.
"""
function Base.show(io::IO, seg::GraphSegment)
    rec = seg.record
    n = length(seg.legs)
    print(io, "GraphSegment($(rec.carrier)$(lpad(rec.flight_number, 4)) $(rec.segment_departure_station)→$(rec.segment_arrival_station), $(n) legs)")
end

"""
    `Base.show(io::IO, cp::GraphConnection)`

Human-friendly one-line summary. Nonstop self-connections display as
`nonstop <flight> O→D`; real connections show both flight identifiers,
connect-point code, and MCT vs available connection time.
"""
function Base.show(io::IO, cp::GraphConnection)
    from_l = cp.from_leg::GraphLeg
    to_l = cp.to_leg::GraphLeg
    from_id = flight_id(from_l.record)
    to_id = flight_id(to_l.record)
    if cp.from_leg === cp.to_leg
        print(io, "GraphConnection(nonstop $(from_id) $(from_l.record.departure_station)->$(from_l.record.arrival_station))")
    else
        print(io, "GraphConnection($(from_id)->$(to_id) at $((cp.station::GraphStation).code), cnx=$(cp.cnx_time)min, mct=$(cp.mct)min)")
    end
end

"""
    `Base.show(io::IO, itn::Itinerary)`

Human-friendly one-line summary: flight chain, stop count, elapsed time,
distance, circuity, and classification flags (INTL, INTERLINE, CODESHARE,
ONLINE).
"""
function Base.show(io::IO, itn::Itinerary)
    stops = itn.num_stops
    elapsed = itn.elapsed_time
    dist = round(itn.total_distance; digits=0)
    circ = round(itn.circuity; digits=2)

    # Build flight-number chain from the connections list.
    # connections[i].from_leg is the i-th departing leg.
    # For the last connection of a connecting itinerary, to_leg is the
    # final arriving leg (different from from_leg).
    flights = String[]
    for (i, cp) in enumerate(itn.connections)
        if i == 1
            push!(flights, flight_id((cp.from_leg::GraphLeg).record))
        end
        if !(cp.from_leg === cp.to_leg) && i > 1
            push!(flights, flight_id((cp.to_leg::GraphLeg).record))
        end
    end
    flight_str = join(flights, " -> ")

    status_parts = String[]
    is_international(itn.status) && push!(status_parts, "INTL")
    is_interline(itn.status)     && push!(status_parts, "INTERLINE")
    is_codeshare(itn.status)     && push!(status_parts, "CODESHARE")
    status_str = isempty(status_parts) ? "ONLINE" : join(status_parts, ",")

    print(io, "Itinerary($(flight_str), $(stops) stops, $(elapsed)min, $(dist)mi, circ=$(circ), $(status_str))")
end

"""
    `Base.show(io::IO, ref::ItineraryRef)`

Human-friendly display: route, flights, stops, elapsed, distance, circuity.
"""
function Base.show(io::IO, ref::ItineraryRef)
    r = route_str(ref)
    f = flights_str(ref)
    print(io, "ItineraryRef($(r), $(f), $(ref.num_stops) stops, $(ref.elapsed_minutes)min, $(round(Int, ref.distance_miles))mi, circ=$(round(ref.circuity; digits=2)))")
end

"""
    `Base.show(io::IO, key::LegKey)`

Compact display: airline/flt_no org->dst.
"""
function Base.show(io::IO, key::LegKey)
    print(io, "LegKey($(flight_id(key)) $(key.departure_station)->$(key.arrival_station))")
end

"""
    `Base.show(io::IO, trip::Trip)`

Human-friendly summary: route chain, type, itinerary count, elapsed, distance.
"""
function Base.show(io::IO, trip::Trip)
    # Build route chain: origin of each itinerary + final destination
    parts = String[]
    for itn in trip.itineraries
        isempty(itn.connections) && continue
        push!(parts, String(((itn.connections[1].from_leg::GraphLeg).org::GraphStation).code))
    end
    if !isempty(trip.itineraries)
        last_itn = trip.itineraries[end]
        if !isempty(last_itn.connections)
            last_cp = last_itn.connections[end]
            dst_leg = (last_cp.to_leg === last_cp.from_leg ?
                last_cp.from_leg : last_cp.to_leg)::GraphLeg
            push!(parts, String((dst_leg.dst::GraphStation).code))
        end
    end
    route = join(parts, "->")
    dist = round(Int, trip.total_distance)
    print(io, "Trip($(trip.trip_id): $(route), $(trip.trip_type), $(length(trip.itineraries)) itineraries, $(trip.total_elapsed)min, $(dist)mi)")
end

# ── Long format ───────────────────────────────────────────────────────────────

"""
    `function itinerary_long_format(itineraries::Vector{Itinerary})::Vector{NamedTuple}`
---

# Description
- Converts a vector of itineraries to long (leg-level) format
- Each element of the returned vector represents one leg in one itinerary's
  sequence, ordered by `(itinerary_id, leg_seq)`
- For a nonstop itinerary, emits exactly 1 row
- For a 1-stop itinerary (2 connections), emits 2 rows: the from_leg of
  connection 1 and the to_leg of the final connection (which is the second
  segment's leg)
- Terminal connection identity: for the last `GraphConnection` in a connecting
  path, if `from_leg !== to_leg`, an additional row is emitted for `to_leg`
  (the final arriving leg not otherwise represented)

# Arguments
1. `itineraries::Vector{Itinerary}`: search results from `search_itineraries`

# Returns
- `::Vector{NamedTuple}`: one NamedTuple per leg with fields:
  `itinerary_id`, `leg_seq`, `airline`, `flt_no`, `flight_id`,
  `record_serial`, `segment_hash`, `org`, `dst`, `pax_dep`, `pax_arr`,
  `eqp`, `body_type`, `distance`, `is_through`, `is_nonstop`,
  `cnx_time`, `mct`, `dep_term`, `arr_term`

# Examples
```julia
julia> rows = itinerary_long_format(itns);
julia> length(rows) >= length(itns)
true
```
"""
function itinerary_long_format(itineraries::Vector{Itinerary})::Vector{NamedTuple}
    rows = NamedTuple[]
    for (itn_idx, itn) in enumerate(itineraries)
        n_cnx = length(itn.connections)
        last_leg = nothing
        seq = 0
        for (leg_idx, cp) in enumerate(itn.connections)
            leg = cp.from_leg::GraphLeg
            is_nonstop_cp = cp.from_leg === cp.to_leg

            # Emit from_leg if not already emitted (dedup self-connection echo)
            if leg !== last_leg
                last_leg = leg
                rec = leg.record
                seq += 1
                push!(rows, (
                    itinerary_id = itn_idx,
                    leg_seq = seq,
                    carrier = String(rec.carrier),
                    flight_number = Int(rec.flight_number),
                    flight_id = flight_id(rec),
                    record_serial = Int(rec.record_serial),
                    segment_hash = rec.segment_hash,
                    departure_station = String(rec.departure_station),
                    arrival_station = String(rec.arrival_station),
                    passenger_departure_time = Int(rec.passenger_departure_time),
                    passenger_arrival_time = Int(rec.passenger_arrival_time),
                    aircraft_type = String(rec.aircraft_type),
                    body_type = rec.body_type,
                    distance = Float64(leg.distance),
                    is_through = cp.is_through,
                    is_nonstop = is_nonstop_cp,
                    cnx_time = seq > 1 ? Int(cp.cnx_time) : 0,
                    mct = seq > 1 ? Int(cp.mct) : 0,
                    departure_terminal = String(rec.departure_terminal),
                    arrival_terminal = String(rec.arrival_terminal),
                ))
            end

            # For the last connection in a connecting itinerary, to_leg is the
            # final arriving leg and must be emitted as its own row.
            if leg_idx == n_cnx && !is_nonstop_cp
                to_leg = cp.to_leg::GraphLeg
                if to_leg !== last_leg
                    seq += 1
                    last_leg = to_leg
                end
                to_rec = to_leg.record
                push!(rows, (
                    itinerary_id = itn_idx,
                    leg_seq = seq,
                    carrier = String(to_rec.carrier),
                    flight_number = Int(to_rec.flight_number),
                    flight_id = flight_id(to_rec),
                    record_serial = Int(to_rec.record_serial),
                    segment_hash = to_rec.segment_hash,
                    departure_station = String(to_rec.departure_station),
                    arrival_station = String(to_rec.arrival_station),
                    passenger_departure_time = Int(to_rec.passenger_departure_time),
                    passenger_arrival_time = Int(to_rec.passenger_arrival_time),
                    aircraft_type = String(to_rec.aircraft_type),
                    body_type = to_rec.body_type,
                    distance = Float64(to_leg.distance),
                    is_through = false,
                    is_nonstop = false,
                    cnx_time = 0,
                    mct = 0,
                    departure_terminal = String(to_rec.departure_terminal),
                    arrival_terminal = String(to_rec.arrival_terminal),
                ))
            end
        end
    end
    return rows
end

# ── Wide format ───────────────────────────────────────────────────────────────

"""
    `function itinerary_wide_format(itineraries::Vector{Itinerary})::Vector{NamedTuple}`
---

# Description
- Converts a vector of itineraries to wide (itinerary-level) format
- Exactly one row per itinerary, with summary fields for ranking and
  cross-referencing with leg-level tables
- Flight numbers are joined with `/` (e.g., `"UA 200/UA1234"`)
- Record serials are joined with `/` in the same order as flight numbers

# Arguments
1. `itineraries::Vector{Itinerary}`: search results from `search_itineraries`

# Returns
- `::Vector{NamedTuple}`: one NamedTuple per itinerary with fields:
  `itinerary_id`, `origin`, `destination`, `flights`, `record_serials`,
  `num_legs`, `num_stops`, `num_eqp_changes`, `elapsed_time`,
  `total_distance`, `market_distance`, `circuity`,
  `is_international`, `has_interline`, `has_codeshare`, `has_through`,
  `num_metros`, `num_countries`, `num_regions`

# Examples
```julia
julia> rows = itinerary_wide_format(itns);
julia> length(rows) == length(itns)
true
```
"""
function itinerary_wide_format(itineraries::Vector{Itinerary})::Vector{NamedTuple}
    rows = NamedTuple[]
    for (itn_idx, itn) in enumerate(itineraries)
        flight_nums = String[]
        record_ids = Int[]
        origin = ""
        destination = ""
        n_cnx = length(itn.connections)

        for (i, cp) in enumerate(itn.connections)
            leg = cp.from_leg::GraphLeg
            push!(flight_nums, flight_id(leg.record))
            push!(record_ids, Int(leg.record.record_serial))
            if i == 1
                origin = String(leg.record.departure_station)
            end
            if i == n_cnx
                if cp.from_leg === cp.to_leg  # nonstop self-connection
                    destination = String(leg.record.arrival_station)
                else
                    to_leg = cp.to_leg::GraphLeg
                    destination = String(to_leg.record.arrival_station)
                    push!(flight_nums, flight_id(to_leg.record))
                    push!(record_ids, Int(to_leg.record.record_serial))
                end
            end
        end

        row = (
            itinerary_id = itn_idx,
            origin = origin,
            destination = destination,
            flights = join(flight_nums, "/"),
            record_serials = join(string.(record_ids), "/"),
            num_legs = length(flight_nums),
            num_stops = Int(itn.num_stops),
            num_eqp_changes = Int(itn.num_eqp_changes),
            elapsed_time = Int(itn.elapsed_time),
            total_distance = Float64(itn.total_distance),
            market_distance = Float64(itn.market_distance),
            circuity = Float64(itn.circuity),
            is_international = is_international(itn.status),
            has_interline = is_interline(itn.status),
            has_codeshare = is_codeshare(itn.status),
            has_through = is_through(itn.status),
            num_metros = Int(itn.num_metros),
            num_countries = Int(itn.num_countries),
            num_regions = Int(itn.num_regions),
        )
        push!(rows, row)
    end
    return rows
end

# ── Delimited file export ────────────────────────────────────────────────────

# ── Passthrough-column helpers ──────────────────────────────────────────────

# Identifier-quote a column name for DuckDB: wrap in double quotes and escape
# embedded double quotes by doubling them. Protects against injection via
# user-supplied column names.
_quote_ident(name::AbstractString) = "\"" * replace(String(name), "\"" => "\"\"") * "\""

# Render a single cell value for passthrough output. Returns a String.
# missing/nothing → empty string. Strings containing `,`, `"`, `\n`, or `\r`
# are CSV-quoted with embedded `"` doubled. All other types stringify via
# `string(v)` and are CSV-quoted if the resulting string needs it.
function _render_cell(v)::String
    (v === missing || v === nothing) && return ""
    s = v isa AbstractString ? String(v) : string(v)
    needs_quote = any(c -> c == ',' || c == '"' || c == '\n' || c == '\r', s)
    needs_quote ? "\"" * replace(s, "\"" => "\"\"") * "\"" : s
end

# Resolve the DuckDB source table + key column for passthrough based on how
# the graph was built. This is the single place that knows the mapping
# between ingest path and source table.
function _passthrough_source(graph::FlightGraph)::@NamedTuple{table::String, key_col::String}
    if graph.source === :newssim
        return (table = "newssim", key_col = "row_number")
    elseif graph.source === :ssim
        return (table = "legs_with_operating", key_col = "row_id")
    else
        throw(ArgumentError("Unknown graph.source: $(graph.source) (expected :ssim or :newssim)"))
    end
end

# Validate kwargs and probe column existence. Returns the trimmed column
# names together with the source table and key column. Throws ArgumentError
# for validation failures; lets DuckDB errors propagate for column-existence
# failures (the DuckDB error already names the missing column).
function _prepare_passthrough(
    graph::FlightGraph,
    store::Union{DuckDBStore,Nothing},
    cols::Vector{String},
)::@NamedTuple{names::Vector{String}, source::String, key_col::String}
    store === nothing && throw(ArgumentError("passthrough_columns requires a store"))

    # Trim + collect, tracking positions of blanks and duplicates
    trimmed = [String(strip(c)) for c in cols]
    blank_positions = [i for (i, c) in enumerate(trimmed) if isempty(c)]
    isempty(blank_positions) || throw(ArgumentError(
        "passthrough_columns contains blank entries at positions: $(blank_positions)"))

    seen = Set{String}()
    dupes = String[]
    for c in trimmed
        if c in seen
            c in dupes || push!(dupes, c)
        else
            push!(seen, c)
        end
    end
    isempty(dupes) || throw(ArgumentError(
        "passthrough_columns has duplicate entries: $(dupes)"))

    src = _passthrough_source(graph)

    # Probe: LIMIT 0 to validate column existence without materialising rows.
    # DuckDB's error identifies which column is missing.
    col_sql = join((_quote_ident(c) for c in trimmed), ", ")
    probe_sql = "SELECT $(_quote_ident(src.key_col)), $col_sql FROM $(src.table) LIMIT 0"
    DBInterface.execute(store.db, probe_sql)  # throws on missing columns

    return (names = trimmed, source = src.table, key_col = src.key_col)
end

# Fetch passthrough cells for the given row_numbers in one batched query.
# Returns Dict{UInt64, Vector{String}} where each Vector is length(cols).
# Integer IDs are string-interpolated into the IN clause (matches the
# existing `resolve_legs` pattern; UInt64 values cannot carry injection
# payload). Column names are always identifier-quoted via `_quote_ident`.
function _fetch_passthrough(
    store::DuckDBStore,
    source_table::String,
    key_col::String,
    cols::Vector{String},
    row_ids::Vector{UInt64},
)::Dict{UInt64,Vector{String}}
    out = Dict{UInt64,Vector{String}}()
    isempty(row_ids) && return out

    id_list = join(row_ids, ",")
    col_sql = join((_quote_ident(c) for c in cols), ", ")
    sql = "SELECT $(_quote_ident(key_col)), $col_sql FROM $(source_table) " *
          "WHERE $(_quote_ident(key_col)) IN ($(id_list))"
    result = DBInterface.execute(store.db, sql)

    key_sym = Symbol(key_col)
    col_syms = [Symbol(c) for c in cols]
    for r in result
        k = UInt64(getproperty(r, key_sym))
        out[k] = [_render_cell(getproperty(r, s)) for s in col_syms]
    end
    return out
end

# Default value for row_numbers not present in the fetched dict: N empty cells.
_passthrough_default(n::Int) = fill("", n)

const _DELIM = ','

function _write_row(io::IO, vals)
    first = true
    for v in vals
        first || print(io, _DELIM)
        first = false
        print(io, v)
    end
    println(io)
end

# Format minutes-since-midnight as "HH:MM"
_format_time(m::Integer) = lpad(div(m, 60), 2, '0') * ":" * lpad(mod(m, 60), 2, '0')

# Determine if this leg is an operating flight (DEI 50 absent or same carrier).
# Also resolves aircraft_owner: defaults to airline if empty on operating legs,
# or codeshare_airline if empty on codeshare legs.
function _resolve_flags(r)
    airline = strip(String(r.carrier))
    flt_no = Int(r.flight_number)
    cs_al = strip(String(r.operating_carrier))
    cs_flt = Int(r.operating_flight_number)
    is_operating = cs_al == "" || cs_al == airline
    # Default codeshare fields to self when operating
    cs_al = is_operating ? airline : cs_al
    cs_flt = is_operating ? flt_no : cs_flt
    owner = strip(String(r.aircraft_owner))
    owner = owner == "" ? cs_al : owner
    return (; is_operating, cs_al, cs_flt, owner)
end

# Non-directional market key (alphabetical order)
_market(org, dst) = org < dst ? org * dst : dst * org

# Distance in integer miles (consistent unit across all outputs)
_miles(d::Real) = round(Int, d)

# Does this leg operate on `date`?  Checks eff/disc range + frequency DOW bit.
# UTC block time for a single leg: (arr - arr_utc_offset) - (dep - dep_utc_offset) + date_var * 1440
@inline function _utc_block_time(r)::Int32
    utc_dep = Int32(r.passenger_departure_time) - Int32(r.departure_utc_offset)
    utc_arr = Int32(r.passenger_arrival_time) - Int32(r.arrival_utc_offset) + Int32(r.arrival_date_variation) * Int32(1440)
    return max(Int32(0), utc_arr - utc_dep)
end

function _operates_on(r, date::Date)::Bool
    eff = unpack_date(r.effective_date)
    disc = unpack_date(r.discontinue_date)
    (eff <= date <= disc) || return false
    dow = Dates.dayofweek(date)  # 1=Mon .. 7=Sun
    return (Int(r.frequency) & (1 << (dow - 1))) != 0
end

"""
    `function write_legs(io::IO, graph::FlightGraph, date::Date)::Int`
---

# Description
- Write all valid legs in the graph to a comma-delimited file
- Includes both operating and codeshare (commercial duplicate) legs
- `is_operating` = true when this is the physical flight; false for codeshare
- Codeshare legs have `operating_carrier`/`operating_flight_number` pointing to the
  operating carrier (from DEI 50); on operating legs these fields are empty

# Arguments
1. `io::IO`: output stream
2. `graph::FlightGraph`: built flight graph
3. `date::Date`: target operating date (legs not operating on this date are skipped)

# Returns
- `::Int`: number of rows written
"""
function write_legs(io::IO, graph::FlightGraph, date::Date)::Int
    _write_row(io, [
        "record_serial", "row_number",
        "carrier", "flight_number", "operational_suffix", "itinerary_var_id", "leg_sequence_number", "service_type",
        "operating_carrier", "operating_flight_number", "is_operating",
        "departure_station", "arrival_station", "market",
        "dep_date", "dep_time", "arr_time", "arrival_date_variation",
        "aircraft_type", "body_type", "departure_terminal", "arrival_terminal",
        "distance_miles",
        "dep_intl_dom", "arr_intl_dom",
        "dei_10", "dei_127", "wet_lease", "aircraft_owner",
    ])

    n = 0
    for leg in graph.legs
        r = leg.record
        _operates_on(r, date) || continue
        flags = _resolve_flags(r)
        org = strip(String(r.departure_station))
        dst = strip(String(r.arrival_station))

        _write_row(io, [
            Int(r.record_serial), Int(r.row_number),
            strip(String(r.carrier)), Int(r.flight_number), r.operational_suffix,
            Int(r.itinerary_var_id), Int(r.leg_sequence_number), r.service_type,
            flags.cs_al, flags.cs_flt, flags.is_operating,
            org, dst, _market(org, dst),
            date, _format_time(r.aircraft_departure_time), _format_time(r.aircraft_arrival_time), Int(r.arrival_date_variation),
            String(r.aircraft_type), r.body_type, strip(String(r.departure_terminal)), strip(String(r.arrival_terminal)),
            _miles(leg.distance),
            r.dep_intl_dom, r.arr_intl_dom,
            strip(r.dei_10), strip(r.dei_127), r.wet_lease, flags.owner,
        ])
        n += 1
    end
    return n
end

"""
    `function write_itineraries(io::IO, itineraries::Vector{Itinerary}, graph::FlightGraph, date::Date)::Int`
---

# Description
- Write itineraries to a comma-delimited file (one row per leg per itinerary)
- `is_operating` = true for operating legs; codeshare legs reference the
  operating flight via `operating_carrier`/`operating_flight_number` (DEI 50)
- `cnx_type`: L = single-leg nonstop, S = through-segment, C = connection

# Arguments
1. `io::IO`: output stream
2. `itineraries::Vector{Itinerary}`: search results
3. `graph::FlightGraph`: built flight graph

# Returns
- `::Int`: number of rows written
"""
function write_itineraries(io::IO, itineraries::Vector{Itinerary}, graph::FlightGraph, date::Date; header::Bool=true)::Int
    if header
        _write_row(io, [
            "itinerary_id", "leg_seq",
            "record_serial", "row_number",
            "carrier", "flight_number", "operational_suffix", "itinerary_var_id", "leg_sequence_number_ssim", "service_type",
            "operating_carrier", "operating_flight_number", "is_operating",
            "departure_station", "arrival_station", "market",
            "dep_date", "dep_time", "arr_time", "arrival_date_variation",
            "aircraft_type", "body_type", "departure_terminal", "arrival_terminal",
            "distance_miles",
            "dei_10", "dei_127", "wet_lease", "aircraft_owner",
            "cnx_type", "cnx_time", "mct", "mct_id",
            "num_stops", "elapsed_time",
            "total_distance_miles", "market_distance_miles", "circuity",
            "is_international", "has_interline", "has_codeshare",
        ])
    end

    n = 0
    for (itn_idx, itn) in enumerate(itineraries)
        # Flatten connections into a leg list with inbound connection metadata
        legs_out = Tuple{GraphLeg, String, Int, Int, Int32}[]  # (leg, cnx_type, cnx_time, mct, mct_id)
        n_cnx = length(itn.connections)
        for (i, cp) in enumerate(itn.connections)
            from_l = cp.from_leg::GraphLeg
            to_l = cp.to_leg::GraphLeg
            is_nonstop = cp.from_leg === cp.to_leg
            mid = cp.mct_result.mct_id
            if i == 1
                # First leg: L if nonstop itinerary, C/S if connecting
                ct = is_nonstop && n_cnx == 1 ? "L" : (cp.is_through ? "S" : "C")
                push!(legs_out, (from_l, ct, 0, 0, Int32(0)))
            else
                ct = cp.is_through ? "S" : "C"
                push!(legs_out, (from_l, ct, Int(cp.cnx_time), Int(cp.mct), mid))
            end
            # Final arriving leg of a connecting itinerary
            if i == n_cnx && !is_nonstop
                push!(legs_out, (to_l, "C", Int(cp.cnx_time), Int(cp.mct), mid))
            end
        end

        for (seq, (leg, cnx_type, cnx_time, mct_val, mct_id)) in enumerate(legs_out)
            n += _write_itn_leg_row(io, itn_idx, seq, leg, cnx_type, cnx_time, mct_val, mct_id, itn, date)
        end
    end
    return n
end

function _write_itn_leg_row(io::IO, itn_idx, leg_seq, leg::GraphLeg,
                            cnx_type::String, cnx_time::Int, mct_val::Int, mct_id::Int32,
                            itn, date::Date)::Int
    r = leg.record
    flags = _resolve_flags(r)
    org = strip(String(r.departure_station))
    dst = strip(String(r.arrival_station))

    _write_row(io, [
        itn_idx, leg_seq,
        Int(r.record_serial), Int(r.row_number),
        strip(String(r.carrier)), Int(r.flight_number), r.operational_suffix,
        Int(r.itinerary_var_id), Int(r.leg_sequence_number), r.service_type,
        strip(String(r.operating_carrier)), Int(r.operating_flight_number), flags.is_operating,
        org, dst, _market(org, dst),
        date, _format_time(r.aircraft_departure_time), _format_time(r.aircraft_arrival_time), Int(r.arrival_date_variation),
        String(r.aircraft_type), r.body_type, strip(String(r.departure_terminal)), strip(String(r.arrival_terminal)),
        _miles(leg.distance),
        strip(r.dei_10), strip(r.dei_127), r.wet_lease, flags.owner,
        cnx_type, cnx_time, mct_val, Int(mct_id),
        Int(itn.num_stops), Int(itn.elapsed_time),
        _miles(itn.total_distance), _miles(itn.market_distance),
        round(Float64(itn.circuity); digits=2),
        is_international(itn.status),
        is_interline(itn.status),
        is_codeshare(itn.status),
    ])
    return 1
end

# ── Trip output ──────────────────────────────────────────────────────────────

"""
    `function write_trips(io::IO, trips::Vector{Trip}, graph::FlightGraph, date::Date; header::Bool=true)::Int`
---

# Description
- Write trips to a comma-delimited file (one row per leg per itinerary per trip)
- Prepends `trip_id`, `trip_type`, `itinerary_seq` to the standard itinerary leg columns

# Arguments
1. `io::IO`: output stream
2. `trips::Vector{Trip}`: trip containers
3. `graph::FlightGraph`: built flight graph
4. `date::Date`: target operating date

# Returns
- `::Int`: number of rows written
"""
function write_trips(io::IO, trips::Vector{Trip}, graph::FlightGraph, date::Date; header::Bool=true)::Int
    if header
        _write_row(io, [
            "trip_id", "trip_type", "itinerary_seq",
            "itinerary_id", "leg_seq",
            "record_serial", "row_number",
            "carrier", "flight_number", "operational_suffix", "itinerary_var_id", "leg_sequence_number_ssim", "service_type",
            "operating_carrier", "operating_flight_number", "is_operating",
            "departure_station", "arrival_station", "market",
            "dep_date", "dep_time", "arr_time", "arrival_date_variation",
            "aircraft_type", "body_type", "departure_terminal", "arrival_terminal",
            "distance_miles",
            "dei_10", "dei_127", "wet_lease", "aircraft_owner",
            "cnx_type", "cnx_time", "mct", "mct_id",
            "num_stops", "elapsed_time",
            "total_distance_miles", "market_distance_miles", "circuity",
            "is_international", "has_interline", "has_codeshare",
        ])
    end

    n = 0
    for trip in trips
        for (itn_seq, itn) in enumerate(trip.itineraries)
            # Flatten connections into legs (same logic as write_itineraries)
            legs_out = Tuple{GraphLeg, String, Int, Int, Int32}[]
            n_cnx = length(itn.connections)
            for (i, cp) in enumerate(itn.connections)
                from_l = cp.from_leg::GraphLeg
                to_l = cp.to_leg::GraphLeg
                is_nonstop = cp.from_leg === cp.to_leg
                mid = cp.mct_result.mct_id
                if i == 1
                    ct = is_nonstop && n_cnx == 1 ? "L" : (cp.is_through ? "S" : "C")
                    push!(legs_out, (from_l, ct, 0, 0, Int32(0)))
                else
                    ct = cp.is_through ? "S" : "C"
                    push!(legs_out, (from_l, ct, Int(cp.cnx_time), Int(cp.mct), mid))
                end
                if i == n_cnx && !is_nonstop
                    push!(legs_out, (to_l, "C", Int(cp.cnx_time), Int(cp.mct), mid))
                end
            end

            for (seq, (leg, cnx_type, cnx_time, mct_val, mct_id)) in enumerate(legs_out)
                r = leg.record
                flags = _resolve_flags(r)
                org = strip(String(r.departure_station))
                dst = strip(String(r.arrival_station))

                _write_row(io, [
                    Int(trip.trip_id), trip.trip_type, itn_seq,
                    itn_seq, seq,
                    Int(r.record_serial), Int(r.row_number),
                    strip(String(r.carrier)), Int(r.flight_number), r.operational_suffix,
                    Int(r.itinerary_var_id), Int(r.leg_sequence_number), r.service_type,
                    flags.cs_al, flags.cs_flt, flags.is_operating,
                    org, dst, _market(org, dst),
                    date, _format_time(r.aircraft_departure_time), _format_time(r.aircraft_arrival_time), Int(r.arrival_date_variation),
                    String(r.aircraft_type), r.body_type, strip(String(r.departure_terminal)), strip(String(r.arrival_terminal)),
                    _miles(leg.distance),
                    strip(r.dei_10), strip(r.dei_127), r.wet_lease, flags.owner,
                    cnx_type, cnx_time, mct_val, Int(mct_id),
                    Int(itn.num_stops), Int(itn.elapsed_time),
                    _miles(itn.total_distance), _miles(itn.market_distance),
                    round(Float64(itn.circuity); digits=2),
                    is_international(itn.status),
                    is_interline(itn.status),
                    is_codeshare(itn.status),
                ])
                n += 1
            end
        end
    end
    return n
end

# ── Compact itinerary leg index ──────────────────────────────────────────────

"""
    `function itinerary_legs(stations, origin, dest, date, ctx)::Vector{ItineraryRef}`
---

# Description
- Search itineraries for an O-D pair on a date and return a compact leg index
- Each row contains only the identity fields needed to cross-reference legs:
  itinerary number, leg position, row_number, record_serial, and flight identifier fields

# Arguments
1. `stations::Dict{StationCode,GraphStation}`: the station graph
2. `origin::StationCode`: departure airport
3. `dest::StationCode`: arrival airport
4. `date::Date`: travel date
5. `ctx::RuntimeContext`: search context

# Returns
- `::Vector{ItineraryRef}`: one inner vector per itinerary, each containing the
  `LegKey` references for its legs in route order. Itinerary index = position in
  the outer vector. Leg position = position in the inner vector.
"""
function itinerary_legs(
    stations::Dict{StationCode,GraphStation},
    origin::StationCode,
    dest::StationCode,
    date::Date,
    ctx::RuntimeContext,
)::Vector{ItineraryRef}
    itineraries = copy(search_itineraries(stations, origin, dest, date, ctx))

    # Filter to itineraries whose first leg operates on the requested date.
    # The graph may contain legs from leading_days before through trailing_days after;
    # all itineraries with a valid first-leg departure within the window are included.
    filter!(itineraries) do itn
        isempty(itn.connections) && return false
        first_leg = (itn.connections[1].from_leg::GraphLeg).record
        _operates_on(first_leg, date)
    end

    # Sort by stops first (nonstop before 1-stop before 2-stop), then elapsed time,
    # then departure time within each tier. This is the standard industry convention.
    # Stops take precedence over operating date so nonstops always rank first
    # even when the schedule window spans multiple days.
    sort!(itineraries; by=itn -> begin
        first_rec = (itn.connections[1].from_leg::GraphLeg).record
        (itn.num_stops, itn.elapsed_time, first_rec.operating_date, first_rec.passenger_departure_time, itn.total_distance)
    end)

    # Deduplicate: two itineraries are identical if they use the same legs in the same order.
    seen = Set{UInt64}()
    unique_itns = Itinerary[]
    for itn in itineraries
        fp = _itinerary_fingerprint(itn)
        fp in seen && continue
        push!(seen, fp)
        push!(unique_itns, itn)
    end

    # Construct a ConnectionRef from a GraphConnection, copying MCT result fields.
    _cnxref(cp::GraphConnection) = ConnectionRef(
        station            = (cp.station::GraphStation).code,
        cnx_time           = cp.cnx_time,
        mct_time           = cp.mct_result.time,
        mct_source         = cp.mct_result.source,
        mct_status         = cp.mct_result.queried_status,
        mct_id             = cp.mct_result.mct_id,
        mct_specificity    = cp.mct_result.specificity,
        mct_matched_fields = cp.mct_result.matched_fields,
        suppressed         = cp.mct_result.suppressed,
        is_through         = cp.is_through,
    )

    # Extract LegKey sequences and ConnectionRef data, wrap in ItineraryRef
    result = ItineraryRef[]
    for itn in unique_itns
        keys = LegKey[]
        cnx_refs = ConnectionRef[]
        flight_mins = Int32(0)
        last_leg = nothing
        for cp in itn.connections
            from_l = cp.from_leg::GraphLeg
            to_l = cp.to_leg::GraphLeg
            if from_l !== last_leg
                push!(keys, LegKey(from_l.record))
                flight_mins += _utc_block_time(from_l.record)
                last_leg = from_l
            end
            if !(from_l === to_l) && to_l !== last_leg
                # This is a real connection (not a nonstop self-connection)
                push!(cnx_refs, _cnxref(cp))
                push!(keys, LegKey(to_l.record))
                flight_mins += _utc_block_time(to_l.record)
                last_leg = to_l
            end
        end

        elapsed = itn.elapsed_time
        layover = max(Int32(0), elapsed - flight_mins)
        ns = max(0, length(keys) - 1)  # num_stops = legs - 1

        push!(result, ItineraryRef(
            legs            = keys,
            connections     = cnx_refs,
            num_stops       = ns,
            elapsed_minutes = elapsed,
            flight_minutes  = flight_mins,
            layover_minutes = layover,
            distance_miles  = Float32(itn.total_distance),
            circuity        = ns == 0 ? Float32(1.0) : Float32(itn.circuity),
        ))
    end
    return result
end

# Fingerprint an itinerary by its unique leg sequence (row_numbers).
function _itinerary_fingerprint(itn::Itinerary)::UInt64
    h = UInt64(0)
    last_rn = UInt64(0)
    for cp in itn.connections
        from_l = cp.from_leg::GraphLeg
        to_l = cp.to_leg::GraphLeg
        rn = from_l.record.row_number
        if rn != last_rn
            h = hash(rn, h)
            last_rn = rn
        end
        if !(from_l === to_l)
            rn2 = to_l.record.row_number
            if rn2 != last_rn
                h = hash(rn2, h)
                last_rn = rn2
            end
        end
    end
    return h
end

# ── Input normalization helpers ───────────────────────────────────────────────

_to_station_codes(s::AbstractString) = [StationCode(s)]
_to_station_codes(s::StationCode) = [s]
_to_station_codes(v::AbstractVector) = StationCode[isa(x, StationCode) ? x : StationCode(x) for x in v]

_to_dates(d::Date) = [d]
_to_dates(v::AbstractVector{Date}) = collect(v)
_to_dates(v::AbstractVector) = Date[d for d in v]

# ── Flexible multi-search ────────────────────────────────────────────────────

"""
    `function itinerary_legs_multi(stations, ctx; origins, destinations=nothing, dates, cross=false)`
---

# Description
- Flexible search accepting single values or collections for origins, destinations, and dates
- When `destinations` is omitted or `nothing`, searches all stations reachable from each origin
- By default, parallel lists of origins and destinations are treated as **paired** O-D pairs
  (origins[1]→destinations[1], origins[2]→destinations[2], etc.)
- Set `cross=true` to search the full **cross-product** of all origins × all destinations
- Cross-product is automatic when only one origin or one destination is provided
- Returns a nested dictionary: `origin → destination → date → Vector{NamedTuple}`

# Arguments
1. `stations::Dict{StationCode,GraphStation}`: the station graph
2. `ctx::RuntimeContext`: search context

# Keyword Arguments
- `origins`: a station code or vector of station codes (String or StationCode)
- `destinations`: a station code, vector, or `nothing` (all destinations)
- `dates`: a Date or vector of Dates
- `cross::Bool=false`: when true, search all origins × all destinations; when false, pair them

# Returns
- Nested `Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}}` keyed by date → origin → destination

# Examples
```julia
# Paired O-D pairs (default): ORD→LHR and DEN→LAX
result = itinerary_legs_multi(stations, ctx;
    origins=["ORD","DEN"], destinations=["LHR","LAX"], dates=Date(2026,3,20))

# Cross-product: ORD→LHR, ORD→LAX, DEN→LHR, DEN→LAX
result = itinerary_legs_multi(stations, ctx;
    origins=["ORD","DEN"], destinations=["LHR","LAX"], dates=Date(2026,3,20), cross=true)

# Single origin → auto cross-product with all destinations
result = itinerary_legs_multi(stations, ctx;
    origins="ORD", destinations=["LHR","SFO","LAX"], dates=Date(2026,3,20))

# All destinations from a station
result = itinerary_legs_multi(stations, ctx; origins="ORD", dates=Date(2026,3,20))
```
"""
function itinerary_legs_multi(
    stations::Dict{StationCode,GraphStation},
    ctx::RuntimeContext;
    origins,
    destinations = nothing,
    dates,
    cross::Bool = false,
)
    orgs = _to_station_codes(origins)
    ds = _to_dates(dates)

    # Determine OD pairs to search
    od_pairs = Tuple{StationCode,StationCode}[]

    if destinations === nothing
        # All destinations from each origin
        all_stns = collect(keys(stations))
        for org in orgs
            for dst in all_stns
                org == dst && continue
                push!(od_pairs, (org, dst))
            end
        end
    elseif cross || length(orgs) == 1 || length(_to_station_codes(destinations)) == 1
        # Cross-product: every origin × every destination
        dsts = _to_station_codes(destinations)
        for org in orgs
            for dst in dsts
                org == dst && continue
                push!(od_pairs, (org, dst))
            end
        end
    else
        # Paired: origins[i] → destinations[i]
        dsts = _to_station_codes(destinations)
        for i in 1:min(length(orgs), length(dsts))
            orgs[i] == dsts[i] && continue
            push!(od_pairs, (orgs[i], dsts[i]))
        end
    end

    result = Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}}()
    for date in ds
        for (org, dst) in od_pairs
            haskey(stations, org) || continue
            haskey(stations, dst) || continue
            legs = itinerary_legs(stations, org, dst, date, ctx)
            isempty(legs) && continue
            org_s = strip(String(org))
            dst_s = strip(String(dst))
            date_dict = get!(result, date) do
                Dict{String, Dict{String, Vector{ItineraryRef}}}()
            end
            org_dict = get!(date_dict, org_s) do
                Dict{String, Vector{ItineraryRef}}()
            end
            org_dict[dst_s] = legs
        end
    end
    return result
end

# Legacy positional method — convert to keyword form
function itinerary_legs_multi(
    stations::Dict{StationCode,GraphStation},
    od_pairs::Vector{Tuple{StationCode,StationCode,Date}},
    ctx::RuntimeContext,
)
    result = Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}}()
    for (org, dst, date) in od_pairs
        legs = itinerary_legs(stations, org, dst, date, ctx)
        isempty(legs) && continue
        org_s = strip(String(org))
        dst_s = strip(String(dst))
        date_dict = get!(result, date) do
            Dict{String, Dict{String, Vector{ItineraryRef}}}()
        end
        org_dict = get!(date_dict, org_s) do
            Dict{String, Vector{ItineraryRef}}()
        end
        org_dict[dst_s] = legs
    end
    return result
end

# ── JSON export ──────────────────────────────────────────────────────────────

# ── LegKey / ItineraryRef resolution ──────────────────────────────────────────

"""
    `resolve_leg(key::LegKey, graph::FlightGraph)::Union{GraphLeg, Nothing}`

Resolve a `LegKey` to a `GraphLeg` in the graph by matching `row_number`.
Returns `nothing` if the leg is not in the current graph.
"""
function resolve_leg(key::LegKey, graph)::Union{GraphLeg, Nothing}
    for leg in graph.legs
        leg.record.row_number == key.row_number && return leg
    end
    return nothing
end

"""
    `resolve_leg(key::LegKey, store::DuckDBStore)::Union{LegRecord, Nothing}`

Resolve a `LegKey` to a full `LegRecord` from the DuckDB store by `row_number`.
Works without a graph — queries the `legs_with_operating` view directly.
"""
function resolve_leg(key::LegKey, store::DuckDBStore)::Union{LegRecord, Nothing}
    # Try the expanded view first (has DEI joins)
    result = DBInterface.execute(store.db,
        "SELECT * FROM legs_with_operating WHERE row_id = ? LIMIT 1",
        [Int64(key.row_number)])
    rows = collect(result)
    if !isempty(rows)
        return _row_to_leg(rows[1])
    end
    # Fall back to base legs table (schedule-level, no DEI)
    result = DBInterface.execute(store.db,
        "SELECT * FROM legs WHERE row_id = ? LIMIT 1",
        [Int64(key.row_number)])
    rows = collect(result)
    isempty(rows) && return nothing
    _row_to_schedule_leg(rows[1])
end

"""
    `resolve_segment(key::LegKey, graph::FlightGraph)::Union{GraphSegment, Nothing}`

Resolve a `LegKey` to its parent `GraphSegment` in the graph.
"""
function resolve_segment(key::LegKey, graph)::Union{GraphSegment, Nothing}
    leg = resolve_leg(key, graph)
    leg === nothing && return nothing
    return leg.segment
end

"""
    `resolve_segment(key::LegKey, store::DuckDBStore)::Union{SegmentRecord, Nothing}`

Resolve a `LegKey` to its `SegmentRecord` from the DuckDB store.
"""
function resolve_segment(key::LegKey, store::DuckDBStore)::Union{SegmentRecord, Nothing}
    leg = resolve_leg(key, store)
    leg === nothing && return nothing
    leg.segment_hash == UInt64(0) && return nothing
    query_segment(store, leg.segment_hash)
end

"""
    `resolve_legs(itn::ItineraryRef, graph::FlightGraph)::Vector{Union{GraphLeg, Nothing}}`

Resolve all legs in an `ItineraryRef` to `GraphLeg` objects from the graph.
"""
function resolve_legs(itn::ItineraryRef, graph)::Vector{Union{GraphLeg, Nothing}}
    # Build row_number lookup once
    idx = Dict{UInt64, GraphLeg}()
    for leg in graph.legs
        idx[leg.record.row_number] = leg
    end
    return [get(idx, k.row_number, nothing) for k in itn.legs]
end

"""
    `resolve_legs(itn::ItineraryRef, store::DuckDBStore)::Vector{Union{LegRecord, Nothing}}`

Resolve all legs in an `ItineraryRef` to full `LegRecord`s from the DuckDB store.
Uses a single batched SQL query instead of one query per leg.
"""
function resolve_legs(itn::ItineraryRef, store::DuckDBStore)::Vector{Union{LegRecord, Nothing}}
    isempty(itn.legs) && return Union{LegRecord, Nothing}[]

    # Collect unique row_numbers
    row_ids = unique([Int64(k.row_number) for k in itn.legs])

    # Batch query — try expanded view first, then base table for any missing
    id_list = join(row_ids, ",")
    records = Dict{UInt64, LegRecord}()

    result = DBInterface.execute(store.db,
        "SELECT * FROM legs_with_operating WHERE row_id IN ($(id_list))")
    for r in result
        rec = _row_to_leg(r)
        records[rec.row_number] = rec
    end

    # Fill gaps from base legs table
    missing_ids = filter(id -> !haskey(records, UInt64(id)), row_ids)
    if !isempty(missing_ids)
        miss_list = join(missing_ids, ",")
        result2 = DBInterface.execute(store.db,
            "SELECT * FROM legs WHERE row_id IN ($(miss_list))")
        for r in result2
            rec = _row_to_schedule_leg(r)
            records[rec.row_number] = rec
        end
    end

    return [get(records, k.row_number, nothing) for k in itn.legs]
end

# ── JSON helpers ─────────────────────────────────────────────────────────────

function _legkey_to_dict(k::LegKey)::Dict{String,Any}
    Dict{String,Any}(
        "row_number"                          => Int(k.row_number),
        "record_serial"                       => Int(k.record_serial),
        "carrier"                             => strip(String(k.carrier)),
        "flight_number"                       => Int(k.flight_number),
        "operational_suffix"                  => string(k.operational_suffix),
        "itinerary_var_id"                    => Int(k.itinerary_var_id),
        "itinerary_var_overflow"              => string(k.itinerary_var_overflow),
        "leg_sequence_number"                 => Int(k.leg_sequence_number),
        "service_type"                        => string(k.service_type),
        "operating_carrier"                   => strip(String(k.operating_carrier)),
        "operating_flight_number"             => Int(k.operating_flight_number),
        "departure_station"                   => strip(String(k.departure_station)),
        "arrival_station"                     => strip(String(k.arrival_station)),
    )
end

function _cnxref_to_dict(cr::ConnectionRef)::Dict{String,Any}
    status_labels = ("DD", "DI", "ID", "II")
    Dict{String,Any}(
        "station"          => strip(String(cr.station)),
        "cnx_time"         => Int(cr.cnx_time),
        "mct_time"         => Int(cr.mct_time),
        "mct_source"       => mct_source_label(cr),
        "mct_status"       => status_labels[Int(cr.mct_status)],
        "mct_id"           => Int(cr.mct_id),
        "mct_specificity"  => Int(cr.mct_specificity),
        "suppressed"       => cr.suppressed,
        "is_through"       => cr.is_through,
    )
end

function _itnref_summary_dict(itn::ItineraryRef)::Dict{String,Any}
    first_key = isempty(itn.legs) ? LegKey() : itn.legs[1]
    d = unpack_date(first_key.operating_date)
    result = Dict{String,Any}(
        "flights"         => flights_str(itn),
        "route"           => route_str(itn),
        "stops"           => String.(stops(itn)),
        "num_stops"       => itn.num_stops,
        "origin"          => String(origin(itn)),
        "destination"     => String(destination(itn)),
        "operating_date"  => Dates.format(d, "yyyy-mm-dd"),
        "departure_time"  => _format_time(first_key.departure_time),
        "elapsed_minutes" => Int(itn.elapsed_minutes),
        "flight_minutes"  => Int(itn.flight_minutes),
        "layover_minutes" => Int(itn.layover_minutes),
        "distance_miles"  => round(Float64(itn.distance_miles); digits=0),
        "circuity"        => round(Float64(itn.circuity); digits=2),
    )
    if !isempty(itn.connections)
        result["connections"] = [_cnxref_to_dict(cr) for cr in itn.connections]
    end
    return result
end

# ── Streaming JSON writers (avoid intermediate Dict allocations) ──────────────

function _write_legkey_json(io::IOBuffer, k::LegKey)
    print(io, "{\"row_number\":", Int(k.row_number),
              ",\"record_serial\":", Int(k.record_serial),
              ",\"carrier\":\"", strip(String(k.carrier)), "\"",
              ",\"flight_number\":", Int(k.flight_number),
              ",\"operational_suffix\":\"", k.operational_suffix, "\"",
              ",\"itinerary_var_id\":", Int(k.itinerary_var_id),
              ",\"itinerary_var_overflow\":\"", k.itinerary_var_overflow, "\"",
              ",\"leg_sequence_number\":", Int(k.leg_sequence_number),
              ",\"service_type\":\"", k.service_type, "\"",
              ",\"operating_carrier\":\"", strip(String(k.operating_carrier)), "\"",
              ",\"operating_flight_number\":", Int(k.operating_flight_number),
              ",\"departure_station\":\"", strip(String(k.departure_station)), "\"",
              ",\"arrival_station\":\"", strip(String(k.arrival_station)), "\"",
              ",\"operating_date\":", Int(k.operating_date),
              ",\"departure_time\":", Int(k.departure_time), "}")
end

function _write_itnref_summary_json(io::IOBuffer, itn::ItineraryRef)
    print(io, "\"flights\":\"", flights_str(itn), "\"",
              ",\"route\":\"", route_str(itn), "\"",
              ",\"stops\":[")
    stn_stops = stops(itn)
    for (i, s) in enumerate(stn_stops)
        i > 1 && print(io, ",")
        print(io, "\"", s, "\"")
    end
    first_key = isempty(itn.legs) ? LegKey() : itn.legs[1]
    d = unpack_date(first_key.operating_date)
    status_labels = ("DD", "DI", "ID", "II")
    print(io, "],\"num_stops\":", itn.num_stops,
              ",\"origin\":\"", origin(itn), "\"",
              ",\"destination\":\"", destination(itn), "\"",
              ",\"operating_date\":\"", Dates.format(d, "yyyy-mm-dd"), "\"",
              ",\"departure_time\":\"", _format_time(first_key.departure_time), "\"",
              ",\"elapsed_minutes\":", Int(itn.elapsed_minutes),
              ",\"flight_minutes\":", Int(itn.flight_minutes),
              ",\"layover_minutes\":", Int(itn.layover_minutes),
              ",\"distance_miles\":", round(Float64(itn.distance_miles); digits=0),
              ",\"circuity\":", round(Float64(itn.circuity); digits=2))
    if !isempty(itn.connections)
        print(io, ",\"connections\":[")
        for (i, cr) in enumerate(itn.connections)
            i > 1 && print(io, ",")
            print(io, "{\"station\":\"", strip(String(cr.station)), "\"",
                      ",\"cnx_time\":", Int(cr.cnx_time),
                      ",\"mct_time\":", Int(cr.mct_time),
                      ",\"mct_source\":\"", mct_source_label(cr), "\"",
                      ",\"mct_status\":\"", status_labels[Int(cr.mct_status)], "\"",
                      ",\"mct_id\":", Int(cr.mct_id),
                      ",\"mct_specificity\":", Int(cr.mct_specificity),
                      ",\"suppressed\":", cr.suppressed,
                      ",\"is_through\":", cr.is_through, "}")
        end
        print(io, "]")
    end
end

function _nested_to_json(nested::Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}})::String
    io = IOBuffer()
    print(io, "{")
    d_first = true
    for (date, org_dict) in nested
        d_first || print(io, ",")
        d_first = false
        print(io, "\"", date, "\":{")
        o_first = true
        for (org, dst_dict) in org_dict
            o_first || print(io, ",")
            o_first = false
            print(io, "\"", org, "\":{")
            dst_first = true
            for (dst, itineraries) in dst_dict
                dst_first || print(io, ",")
                dst_first = false
                print(io, "\"", dst, "\":[")
                for (i, itn) in enumerate(itineraries)
                    i > 1 && print(io, ",")
                    print(io, "{")
                    _write_itnref_summary_json(io, itn)
                    print(io, ",\"legs\":[")
                    for (j, k) in enumerate(itn.legs)
                        j > 1 && print(io, ",")
                        _write_legkey_json(io, k)
                    end
                    print(io, "]}")
                end
                print(io, "]")
            end
            print(io, "}")
        end
        print(io, "}")
    end
    print(io, "}")
    return String(take!(io))
end

function _nested_to_json_compact(nested::Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}})::String
    io = IOBuffer()
    print(io, "{")
    d_first = true
    for (date, org_dict) in nested
        d_first || print(io, ",")
        d_first = false
        print(io, "\"", date, "\":{")
        o_first = true
        for (org, dst_dict) in org_dict
            o_first || print(io, ",")
            o_first = false
            print(io, "\"", org, "\":{")
            dst_first = true
            for (dst, itineraries) in dst_dict
                dst_first || print(io, ",")
                dst_first = false
                print(io, "\"", dst, "\":[")
                for (i, itn) in enumerate(itineraries)
                    i > 1 && print(io, ",")
                    print(io, "{")
                    _write_itnref_summary_json(io, itn)
                    print(io, "}")
                end
                print(io, "]")
            end
            print(io, "}")
        end
        print(io, "}")
    end
    print(io, "}")
    return String(take!(io))
end

"""
    `function itinerary_legs_json(stations, ctx; origins, destinations=nothing, dates, cross=false, compact=false)::String`

Same as `itinerary_legs_multi` but returns a JSON string.
Accepts the same flexible keyword arguments.

When `compact=true`, returns only the ItineraryRef summary fields (flights, stops,
num_stops, origin, destination) without the full `legs` array — useful for display
and debugging.
"""
function itinerary_legs_json(
    stations::Dict{StationCode,GraphStation},
    ctx::RuntimeContext;
    origins,
    destinations = nothing,
    dates,
    cross::Bool = false,
    compact::Bool = false,
)::String
    nested = itinerary_legs_multi(stations, ctx; origins, destinations, dates, cross)
    compact ? _nested_to_json_compact(nested) : _nested_to_json(nested)
end

# Legacy positional method
function itinerary_legs_json(
    stations::Dict{StationCode,GraphStation},
    od_pairs::Vector{Tuple{StationCode,StationCode,Date}},
    ctx::RuntimeContext;
    compact::Bool = false,
)::String
    nested = itinerary_legs_multi(stations, od_pairs, ctx)
    compact ? _nested_to_json_compact(nested) : _nested_to_json(nested)
end

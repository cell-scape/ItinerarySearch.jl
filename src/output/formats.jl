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
    print(io, "GraphLeg($(flight_id(rec)) $(rec.org)→$(rec.dst))")
end

"""
    `Base.show(io::IO, seg::GraphSegment)`

Human-friendly one-line summary: marketing flight number, segment endpoints,
and leg count.
"""
function Base.show(io::IO, seg::GraphSegment)
    rec = seg.record
    n = length(seg.legs)
    print(io, "GraphSegment($(rec.airline)$(lpad(rec.flt_no, 4)) $(rec.segment_org)→$(rec.segment_dst), $(n) legs)")
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
        print(io, "GraphConnection(nonstop $(from_id) $(from_l.record.org)->$(from_l.record.dst))")
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
        for (leg_idx, cp) in enumerate(itn.connections)
            leg = cp.from_leg::GraphLeg
            rec = leg.record

            is_nonstop_cp = cp.from_leg === cp.to_leg

            row = (
                itinerary_id = itn_idx,
                leg_seq = leg_idx,
                airline = String(rec.airline),
                flt_no = Int(rec.flt_no),
                flight_id = flight_id(rec),
                record_serial = Int(rec.record_serial),
                segment_hash = rec.segment_hash,
                org = String(rec.org),
                dst = String(rec.dst),
                pax_dep = Int(rec.pax_dep),
                pax_arr = Int(rec.pax_arr),
                eqp = String(rec.eqp),
                body_type = rec.body_type,
                distance = Float64(leg.distance),
                is_through = cp.is_through,
                is_nonstop = is_nonstop_cp,
                cnx_time = leg_idx > 1 ? Int(cp.cnx_time) : 0,
                mct = leg_idx > 1 ? Int(cp.mct) : 0,
                dep_term = String(rec.dep_term),
                arr_term = String(rec.arr_term),
            )
            push!(rows, row)

            # For the last connection in a connecting itinerary, to_leg is the
            # final arriving leg and must be emitted as its own row — it is not
            # the from_leg of any subsequent connection.
            if leg_idx == n_cnx && !is_nonstop_cp
                to_leg = cp.to_leg::GraphLeg
                to_rec = to_leg.record
                push!(rows, (
                    itinerary_id = itn_idx,
                    leg_seq = leg_idx + 1,
                    airline = String(to_rec.airline),
                    flt_no = Int(to_rec.flt_no),
                    flight_id = flight_id(to_rec),
                    record_serial = Int(to_rec.record_serial),
                    segment_hash = to_rec.segment_hash,
                    org = String(to_rec.org),
                    dst = String(to_rec.dst),
                    pax_dep = Int(to_rec.pax_dep),
                    pax_arr = Int(to_rec.pax_arr),
                    eqp = String(to_rec.eqp),
                    body_type = to_rec.body_type,
                    distance = Float64(to_leg.distance),
                    is_through = false,
                    is_nonstop = false,
                    cnx_time = 0,
                    mct = 0,
                    dep_term = String(to_rec.dep_term),
                    arr_term = String(to_rec.arr_term),
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
                origin = String(leg.record.org)
            end
            if i == n_cnx
                if cp.from_leg === cp.to_leg  # nonstop self-connection
                    destination = String(leg.record.dst)
                else
                    to_leg = cp.to_leg::GraphLeg
                    destination = String(to_leg.record.dst)
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

const _DELIM = '|'

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
    airline = strip(String(r.airline))
    flt_no = Int(r.flt_no)
    cs_al = strip(String(r.codeshare_airline))
    cs_flt = Int(r.codeshare_flt_no)
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
function _operates_on(r, date::Date)::Bool
    eff = unpack_date(r.eff_date)
    disc = unpack_date(r.disc_date)
    (eff <= date <= disc) || return false
    dow = Dates.dayofweek(date)  # 1=Mon .. 7=Sun
    return (Int(r.frequency) & (1 << (dow - 1))) != 0
end

"""
    `function write_legs(io::IO, graph::FlightGraph, date::Date)::Int`
---

# Description
- Write all valid legs in the graph to a pipe-delimited file
- Includes both operating and codeshare (commercial duplicate) legs
- `is_operating` = true when this is the physical flight; false for codeshare
- Codeshare legs have `codeshare_airline`/`codeshare_flt_no` pointing to the
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
        "airline", "flt_no", "operational_suffix", "itin_var", "leg_seq", "svc_type",
        "codeshare_airline", "codeshare_flt_no", "is_operating",
        "org", "dst", "market",
        "dep_date", "dep_time", "arr_time", "arr_date_var",
        "eqp", "body_type", "dep_term", "arr_term",
        "distance_miles",
        "mct_status_dep", "mct_status_arr",
        "dei_10", "dei_127", "wet_lease", "aircraft_owner",
    ])

    n = 0
    for leg in values(graph.legs)
        r = leg.record
        _operates_on(r, date) || continue
        flags = _resolve_flags(r)
        org = strip(String(r.org))
        dst = strip(String(r.dst))

        _write_row(io, [
            Int(r.record_serial), Int(r.row_number),
            strip(String(r.airline)), Int(r.flt_no), r.operational_suffix,
            Int(r.itin_var), Int(r.leg_seq), r.svc_type,
            flags.cs_al, flags.cs_flt, flags.is_operating,
            org, dst, _market(org, dst),
            date, _format_time(r.ac_dep), _format_time(r.ac_arr), Int(r.arr_date_var),
            String(r.eqp), r.body_type, strip(String(r.dep_term)), strip(String(r.arr_term)),
            _miles(leg.distance),
            r.mct_status_dep, r.mct_status_arr,
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
- Write itineraries to a pipe-delimited file (one row per leg per itinerary)
- `is_operating` = true for operating legs; codeshare legs reference the
  operating flight via `codeshare_airline`/`codeshare_flt_no` (DEI 50)
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
            "airline", "flt_no", "operational_suffix", "itin_var", "leg_seq_ssim", "svc_type",
            "codeshare_airline", "codeshare_flt_no", "is_operating",
            "org", "dst", "market",
            "dep_date", "dep_time", "arr_time", "arr_date_var",
            "eqp", "body_type", "dep_term", "arr_term",
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
    org = strip(String(r.org))
    dst = strip(String(r.dst))

    _write_row(io, [
        itn_idx, leg_seq,
        Int(r.record_serial), Int(r.row_number),
        strip(String(r.airline)), Int(r.flt_no), r.operational_suffix,
        Int(r.itin_var), Int(r.leg_seq), r.svc_type,
        strip(String(r.codeshare_airline)), Int(r.codeshare_flt_no), flags.is_operating,
        org, dst, _market(org, dst),
        date, _format_time(r.ac_dep), _format_time(r.ac_arr), Int(r.arr_date_var),
        String(r.eqp), r.body_type, strip(String(r.dep_term)), strip(String(r.arr_term)),
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
- Write trips to a pipe-delimited file (one row per leg per itinerary per trip)
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
            "airline", "flt_no", "operational_suffix", "itin_var", "leg_seq_ssim", "svc_type",
            "codeshare_airline", "codeshare_flt_no", "is_operating",
            "org", "dst", "market",
            "dep_date", "dep_time", "arr_time", "arr_date_var",
            "eqp", "body_type", "dep_term", "arr_term",
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
                org = strip(String(r.org))
                dst = strip(String(r.dst))

                _write_row(io, [
                    Int(trip.trip_id), trip.trip_type, itn_seq,
                    itn_seq, seq,
                    Int(r.record_serial), Int(r.row_number),
                    strip(String(r.airline)), Int(r.flt_no), r.operational_suffix,
                    Int(r.itin_var), Int(r.leg_seq), r.svc_type,
                    flags.cs_al, flags.cs_flt, flags.is_operating,
                    org, dst, _market(org, dst),
                    date, _format_time(r.ac_dep), _format_time(r.ac_arr), Int(r.arr_date_var),
                    String(r.eqp), r.body_type, strip(String(r.dep_term)), strip(String(r.arr_term)),
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

    # Filter to itineraries whose first leg operates on the requested date
    filter!(itineraries) do itn
        isempty(itn.connections) && return false
        first_leg = (itn.connections[1].from_leg::GraphLeg).record
        _operates_on(first_leg, date)
    end

    # Sort by stops (ascending), then elapsed time, then total distance
    sort!(itineraries; by=itn -> (itn.num_stops, itn.elapsed_time, itn.total_distance))

    # Deduplicate: two itineraries are identical if they use the same legs in the same order.
    seen = Set{UInt64}()
    unique_itns = Itinerary[]
    for itn in itineraries
        fp = _itinerary_fingerprint(itn)
        fp in seen && continue
        push!(seen, fp)
        push!(unique_itns, itn)
    end

    # Extract LegKey sequences and wrap in ItineraryRef
    result = ItineraryRef[]
    for itn in unique_itns
        keys = LegKey[]
        last_leg = nothing
        for cp in itn.connections
            from_l = cp.from_leg::GraphLeg
            to_l = cp.to_leg::GraphLeg
            if from_l !== last_leg
                push!(keys, LegKey(from_l.record))
                last_leg = from_l
            end
            if !(from_l === to_l) && to_l !== last_leg
                push!(keys, LegKey(to_l.record))
                last_leg = to_l
            end
        end
        push!(result, ItineraryRef(keys))
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

function _legkey_to_dict(k::LegKey)::Dict{String,Any}
    Dict{String,Any}(
        "row_number"         => Int(k.row_number),
        "record_serial"      => Int(k.record_serial),
        "airline"            => strip(String(k.airline)),
        "flt_no"             => Int(k.flt_no),
        "operational_suffix" => string(k.operational_suffix),
        "itin_var"           => Int(k.itin_var),
        "itin_var_overflow"  => string(k.itin_var_overflow),
        "leg_seq"            => Int(k.leg_seq),
        "svc_type"           => string(k.svc_type),
        "codeshare_airline"  => strip(String(k.codeshare_airline)),
        "codeshare_flt_no"   => Int(k.codeshare_flt_no),
        "org"                => strip(String(k.org)),
        "dst"                => strip(String(k.dst)),
    )
end

function _nested_to_json(nested::Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}})::String
    json_root = Dict{String,Any}()
    for (date, org_dict) in nested
        json_date = Dict{String,Any}()
        for (org, dst_dict) in org_dict
            json_org = Dict{String,Any}()
            for (dst, itineraries) in dst_dict
                json_org[dst] = [
                    Dict{String,Any}(
                        "flights"     => itn.flights,
                        "stops"       => itn.stops,
                        "num_stops"   => itn.num_stops,
                        "origin"      => itn.origin,
                        "destination" => itn.destination,
                        "legs"        => [_legkey_to_dict(k) for k in itn.legs],
                    )
                    for itn in itineraries
                ]
            end
            json_date[org] = json_org
        end
        json_root[string(date)] = json_date
    end
    return JSON3.write(json_root)
end

function _nested_to_json_compact(nested::Dict{Date, Dict{String, Dict{String, Vector{ItineraryRef}}}})::String
    json_root = Dict{String,Any}()
    for (date, org_dict) in nested
        json_date = Dict{String,Any}()
        for (org, dst_dict) in org_dict
            json_org = Dict{String,Any}()
            for (dst, itineraries) in dst_dict
                json_org[dst] = [
                    Dict{String,Any}(
                        "flights"     => itn.flights,
                        "stops"       => itn.stops,
                        "num_stops"   => itn.num_stops,
                        "origin"      => itn.origin,
                        "destination" => itn.destination,
                    )
                    for itn in itineraries
                ]
            end
            json_date[org] = json_org
        end
        json_root[string(date)] = json_date
    end
    return JSON3.write(json_root)
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

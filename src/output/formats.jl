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
    from_id = flight_id(cp.from_leg.record)
    to_id = flight_id(cp.to_leg.record)
    if cp.from_leg === cp.to_leg
        print(io, "GraphConnection(nonstop $(from_id) $(cp.from_leg.record.org)→$(cp.from_leg.record.dst))")
    else
        print(io, "GraphConnection($(from_id)→$(to_id) at $(cp.station.code), cnx=$(cp.cnx_time)min, mct=$(cp.mct)min)")
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
            push!(flights, flight_id(cp.from_leg.record))
        end
        if !(cp.from_leg === cp.to_leg) && i > 1
            push!(flights, flight_id(cp.to_leg.record))
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
                distance = Float64(rec.distance),
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
                    distance = Float64(to_rec.distance),
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

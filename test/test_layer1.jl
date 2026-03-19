using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Layer 1 one-via pre-computation" begin

    # ── Test helpers ────────────────────────────────────────────────────────────

    function _make_stn_rec(code, lat, lng)
        StationRecord(
            code = StationCode(code),
            country = InlineString3("US"),
            state = InlineString3(""),
            metro_area = InlineString3(""),
            region = InlineString3("NAM"),
            lat = lat,
            lng = lng,
            utc_offset = Int16(0),
        )
    end

    function _make_leg_rec(;
        airline = "UA",
        flt_no = 100,
        org = "JFK",
        dst = "ORD",
        pax_dep = Int16(480),
        pax_arr = Int16(600),
        distance = 500.0f0,
        frequency = UInt8(0x7f),
        eff_date = UInt32(20260101),
        disc_date = UInt32(20261231),
    )
        LegRecord(
            airline = AirlineCode(airline),
            flt_no = Int16(flt_no),
            operational_suffix = ' ',
            itin_var = UInt8(1),
            itin_var_overflow = ' ',
            leg_seq = UInt8(1),
            svc_type = 'J',
            org = StationCode(org),
            dst = StationCode(dst),
            pax_dep = pax_dep,
            pax_arr = pax_arr,
            ac_dep = pax_dep,
            ac_arr = pax_arr,
            dep_utc_offset = Int16(0),
            arr_utc_offset = Int16(0),
            dep_date_var = Int8(0),
            arr_date_var = Int8(0),
            eqp = InlineString7("738"),
            body_type = 'N',
            dep_term = InlineString3("1"),
            arr_term = InlineString3("1"),
            aircraft_owner = AirlineCode(airline),
            operating_date = UInt32(20260601),
            day_of_week = UInt8(1),
            eff_date = eff_date,
            disc_date = disc_date,
            frequency = frequency,
            mct_status_dep = 'D',
            mct_status_arr = 'D',
            trc = InlineString15(""),
            trc_overflow = ' ',
            record_serial = UInt32(1),
            row_number = UInt64(1),
            segment_hash = UInt64(0),
            distance = Distance(distance),
            codeshare_airline = AirlineCode(""),
            codeshare_flt_no = Int16(0),
            dei_10 = "",
            wet_lease = false,
            dei_127 = "",
            prbd = InlineString31(""),
        )
    end

    # ── Build 4-station graph: JFK → ORD → DEN → LAX ──────────────────────────
    #
    # East-to-west US route: coordinates chosen so circuity passes easily.
    #
    # Legs:
    #   leg_ab: JFK → ORD  (distance ~ 740 NM)
    #   leg_bc: ORD → DEN  (distance ~ 920 NM)   ← transit leg at ORD
    #   leg_cd: DEN → LAX  (distance ~ 860 NM)
    #
    # Connections:
    #   cnx_b: at ORD — from_leg=leg_ab, to_leg=leg_bc
    #   cnx_c: at DEN — from_leg=leg_bc, to_leg=leg_cd
    #
    # Expected Layer 1 entry: (JFK, LAX) via transit leg leg_bc
    #   path: JFK →[leg_ab]→ ORD →[leg_bc]→ DEN →[leg_cd]→ LAX

    @testset "Basic 4-station path A→B→C→D" begin
        # Approx coordinates (lat, lng)
        stn_a = GraphStation(_make_stn_rec("JFK",  40.64, -73.78))   # New York
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97, -87.91))   # Chicago
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))  # Denver
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))  # Los Angeles

        # JFK→ORD
        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 101,
                pax_dep = Int16(480), pax_arr = Int16(570),
                distance = 740.0f0),
            stn_a, stn_b,
        )
        # ORD→DEN  (transit leg)
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 102,
                pax_dep = Int16(660), pax_arr = Int16(780),
                distance = 920.0f0),
            stn_b, stn_c,
        )
        # DEN→LAX
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 103,
                pax_dep = Int16(840), pax_arr = Int16(960),
                distance = 860.0f0),
            stn_c, stn_d,
        )

        # Populate station departure/arrival lists
        push!(stn_a.departures, leg_ab)
        push!(stn_b.arrivals, leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals, leg_bc)
        push!(stn_c.departures, leg_cd)
        push!(stn_d.arrivals, leg_cd)

        # Connection at ORD (stn_b): leg_ab arrives, leg_bc departs
        # Storage convention (from connect.jl lines 219-221):
        #   push!(station.connections, cp)
        #   push!(arr_leg.connect_to, cp)
        #   push!(dep_leg.connect_from, cp)
        cnx_b = GraphConnection(
            from_leg = leg_ab,
            to_leg = leg_bc,
            station = stn_b,
            valid_from = UInt32(20260101),
            valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_b.connections, cnx_b)
        push!(leg_ab.connect_to, cnx_b)    # leg_ab is from_leg → connect_to
        push!(leg_bc.connect_from, cnx_b)  # leg_bc is to_leg   → connect_from

        # Connection at DEN (stn_c): leg_bc arrives, leg_cd departs
        cnx_c = GraphConnection(
            from_leg = leg_bc,
            to_leg = leg_cd,
            station = stn_c,
            valid_from = UInt32(20260101),
            valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_c.connections, cnx_c)
        push!(leg_bc.connect_to, cnx_c)    # leg_bc is from_leg → connect_to
        push!(leg_cd.connect_from, cnx_c)  # leg_cd is to_leg   → connect_from

        # Nonstop self-connections for each leg (required by search)
        ns_ab = nonstop_connection(leg_ab, stn_a)
        ns_bc = nonstop_connection(leg_bc, stn_b)
        ns_cd = nonstop_connection(leg_cd, stn_c)
        push!(stn_a.connections, ns_ab)
        push!(stn_b.connections, ns_bc)
        push!(stn_c.connections, ns_cd)

        # Build FlightGraph (circumventing DuckDB — inject directly)
        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),  # generous — east→west is not circuitous
        )

        # ── Preconditions: verify the connection topology we set up ────────────

        # leg_bc.connect_from should contain cnx_b (the connection at ORD)
        @test length(leg_bc.connect_from) == 1
        @test (leg_bc.connect_from[1]::GraphConnection) === cnx_b

        # leg_bc.connect_to should contain cnx_c (the connection at DEN)
        @test length(leg_bc.connect_to) == 1
        @test (leg_bc.connect_to[1]::GraphConnection) === cnx_c

        # ── Call build_layer1! ────────────────────────────────────────────────

        @test graph.layer1_built == false
        build_layer1!(graph)
        @test graph.layer1_built == true

        # ── Verify (ORD, LAX) entry exists ────────────────────────────────────
        # Index is keyed by (via_station, destination) so the DFS can look up
        # two-hop completions using (current_leg.dst.code, dest.code).

        key = (StationCode("ORD"), StationCode("LAX"))
        @test haskey(graph.layer1, key)

        oscs = graph.layer1[key]
        @test length(oscs) >= 1

        osc = oscs[1]::OneStopConnection

        # Structural checks
        @test osc.via_leg === leg_bc
        @test osc.first === cnx_b
        @test osc.second === cnx_c

        # Distance = leg_ab + leg_bc + leg_cd
        expected_dist = leg_ab.distance + leg_bc.distance + leg_cd.distance
        @test osc.total_distance ≈ expected_dist

        # Validity = intersection of cnx_b and cnx_c (both cover full year)
        @test osc.valid_from == UInt32(20260101)
        @test osc.valid_to == UInt32(20261231)
        @test osc.valid_days == UInt8(0x7f)

        # No reverse path (DEN → JFK) should exist from the transit station
        @test !haskey(graph.layer1, (StationCode("LAX"), StationCode("JFK")))
    end

    # ── Validity intersection ───────────────────────────────────────────────────

    @testset "Validity intersection respected" begin
        stn_a = GraphStation(_make_stn_rec("JFK", 40.64, -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD", 41.97, -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN", 39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX", 33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 201,
                eff_date = UInt32(20260101), disc_date = UInt32(20260630),
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 202,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 203,
                eff_date = UInt32(20260401), disc_date = UInt32(20261231),
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
            stn_c, stn_d,
        )

        push!(stn_b.arrivals, leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals, leg_bc)
        push!(stn_c.departures, leg_cd)

        # cnx_b: leg_ab covers Jan–Jun
        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20260630),
            valid_days = UInt8(0x7f),
        )
        push!(leg_ab.connect_to, cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        # cnx_c: leg_cd covers Apr–Dec
        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260401), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(leg_bc.connect_to, cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
        )

        build_layer1!(graph)

        # Index keyed by (via_station=ORD, destination=LAX)
        key = (StationCode("ORD"), StationCode("LAX"))
        @test haskey(graph.layer1, key)
        osc = graph.layer1[key][1]::OneStopConnection

        # Intersection: max(Jan, Apr) = Apr, min(Jun, Dec) = Jun → Apr–Jun
        @test osc.valid_from == UInt32(20260401)
        @test osc.valid_to == UInt32(20260630)
    end

    # ── DOW intersection — non-overlapping days → no Layer 1 entry ──────────────

    @testset "Non-overlapping DOW bitmasks produce no entry" begin
        stn_a = GraphStation(_make_stn_rec("JFK", 40.64, -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD", 41.97, -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN", 39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX", 33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 301,
                frequency = UInt8(0x01),  # Mon only
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 302,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 303,
                frequency = UInt8(0x02),  # Tue only
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
            stn_c, stn_d,
        )

        push!(stn_b.arrivals, leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals, leg_bc)
        push!(stn_c.departures, leg_cd)

        # cnx_b: Mon only
        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x01),
        )
        push!(leg_ab.connect_to, cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        # cnx_c: Tue only → no overlap with cnx_b
        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x02),
        )
        push!(leg_bc.connect_to, cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
        )

        build_layer1!(graph)

        # DOW intersection is 0x01 & 0x02 = 0x00 → path must not appear
        # (keyed by via_station=ORD, destination=LAX)
        @test !haskey(graph.layer1, (StationCode("ORD"), StationCode("LAX")))
    end

    # ── Round-trip filter ───────────────────────────────────────────────────────

    @testset "Round-trip path (org == dst) is not indexed" begin
        # JFK → ORD → DEN → JFK: origin equals destination
        stn_a = GraphStation(_make_stn_rec("JFK", 40.64, -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD", 41.97, -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN", 39.86, -104.67))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 401,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 402,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        # leg_cd returns to JFK — creates a round-trip
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "JFK", flt_no = 403,
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 1600.0f0),
            stn_c, stn_a,
        )

        push!(stn_b.arrivals, leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals, leg_bc)
        push!(stn_c.departures, leg_cd)

        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(leg_ab.connect_to, cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(leg_bc.connect_to, cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 5.0),
        )

        build_layer1!(graph)

        # JFK → JFK is a round-trip and must be filtered out
        @test !haskey(graph.layer1, (StationCode("JFK"), StationCode("JFK")))
    end

    # ── Layer 1 integration: DFS uses Layer 1 index ─────────────────────────────

    @testset "DFS uses Layer 1 index (layer1_hits)" begin
        using ItinerarySearch: search_itineraries, _is_valid_on_date

        # Reuse the same 4-station JFK→ORD→DEN→LAX topology as the basic test.
        stn_a = GraphStation(_make_stn_rec("JFK",  40.64, -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97, -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 501,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 502,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 503,
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
            stn_c, stn_d,
        )

        push!(stn_a.departures, leg_ab)
        push!(stn_b.arrivals,   leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals,   leg_bc)
        push!(stn_c.departures, leg_cd)
        push!(stn_d.arrivals,   leg_cd)

        # Connecting connections
        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_b.connections, cnx_b)
        push!(leg_ab.connect_to,   cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_c.connections, cnx_c)
        push!(leg_bc.connect_to,   cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        # Nonstop self-connections (required by search_itineraries)
        ns_ab = nonstop_connection(leg_ab, stn_a)
        ns_bc = nonstop_connection(leg_bc, stn_b)
        ns_cd = nonstop_connection(leg_cd, stn_c)
        push!(stn_a.connections, ns_ab)
        push!(stn_b.connections, ns_bc)
        push!(stn_c.connections, ns_cd)

        stations = Dict(
            StationCode("JFK") => stn_a,
            StationCode("ORD") => stn_b,
            StationCode("DEN") => stn_c,
            StationCode("LAX") => stn_d,
        )

        graph = FlightGraph(
            stations = stations,
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
        )

        build_layer1!(graph)
        @test graph.layer1_built

        # Wire Layer 1 into a RuntimeContext
        ctx = RuntimeContext(
            layer1_built = true,
            layer1 = graph.layer1,
            itn_rules = build_itn_rules(SearchConfig()),
        )

        target = Date(2026, 6, 1)   # Monday — all days valid
        itns = search_itineraries(stations, StationCode("JFK"), StationCode("LAX"), target, ctx)

        @test !isempty(itns)
        @test ctx.search_stats.layer1_hits > 0
    end

    @testset "DFS Layer 1 miss counter increments for unknown pair" begin
        using ItinerarySearch: search_itineraries

        # Single leg JFK→ORD; no Layer 1 entry for ORD→anywhere
        stn_a = GraphStation(_make_stn_rec("JFK", 40.64, -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD", 41.97, -87.91))
        stn_c = GraphStation(_make_stn_rec("LAX", 33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 601,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )

        push!(stn_a.departures, leg_ab)
        push!(stn_b.arrivals,   leg_ab)

        ns_ab = nonstop_connection(leg_ab, stn_a)
        push!(stn_a.connections, ns_ab)

        # Build an empty Layer 1 (no entries) and attach to ctx
        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("LAX") => stn_c,
            ),
            legs = [leg_ab],
            config = SearchConfig(circuity_factor = 3.0),
        )
        build_layer1!(graph)   # will produce no entries for this tiny graph

        stations = Dict(
            StationCode("JFK") => stn_a,
            StationCode("ORD") => stn_b,
            StationCode("LAX") => stn_c,
        )

        ctx = RuntimeContext(
            layer1_built = true,
            layer1 = graph.layer1,
            itn_rules = build_itn_rules(SearchConfig()),
        )

        target = Date(2026, 6, 1)
        # Search JFK→LAX — ORD arrives but has no onward connections to LAX, so
        # the Layer 1 lookup for (ORD, LAX) will miss.
        search_itineraries(stations, StationCode("JFK"), StationCode("LAX"), target, ctx)

        @test ctx.search_stats.layer1_misses > 0
    end

    # ── Fingerprinting ──────────────────────────────────────────────────────────

    @testset "Fingerprinting" begin
        using ItinerarySearch: _compute_fingerprint

        # Build the same 4-station JFK→ORD→DEN→LAX graph used in the basic test.
        stn_a = GraphStation(_make_stn_rec("JFK",  40.64,  -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97,  -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 701,
                pax_dep = Int16(480), pax_arr = Int16(570),
                distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 702,
                pax_dep = Int16(660), pax_arr = Int16(780),
                distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 703,
                pax_dep = Int16(840), pax_arr = Int16(960),
                distance = 860.0f0),
            stn_c, stn_d,
        )

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
        )

        fp = _compute_fingerprint(graph)

        # Returns a 3-tuple of UInt64
        @test fp isa Tuple{UInt64, UInt64, UInt64}

        # All components must be non-zero (3 legs → non-empty row_id list;
        # empty MCT → hash([]) is still a deterministic non-zero UInt64;
        # config tuple is non-trivial)
        schedule_hash, mct_hash, config_hash = fp
        @test schedule_hash != UInt64(0)
        @test config_hash   != UInt64(0)
        # mct_hash: hash of empty Int32[] — just check it's a UInt64 (may be 0)
        @test mct_hash isa UInt64

        # Same graph → same fingerprint (idempotent within session)
        fp2 = _compute_fingerprint(graph)
        @test fp2 === fp

        # Different config → different config_hash, same schedule/mct hashes
        graph_alt = FlightGraph(
            stations = graph.stations,
            legs     = graph.legs,
            config   = SearchConfig(circuity_factor = 99.0, max_stops = 5),
        )
        fp_alt = _compute_fingerprint(graph_alt)
        sched_alt, mct_alt, cfg_alt = fp_alt

        @test sched_alt == schedule_hash   # schedule unchanged
        @test mct_alt   == mct_hash        # MCT unchanged
        @test cfg_alt   != config_hash     # config changed
    end

    # ── export_layer1! ──────────────────────────────────────────────────────────

    @testset "export_layer1! flushes Layer 1 to DuckDB" begin
        using ItinerarySearch: export_layer1!
        using DuckDB
        using DBInterface

        # Build the same 4-station JFK→ORD→DEN→LAX topology as the basic test.
        stn_a = GraphStation(_make_stn_rec("JFK",  40.64,  -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97,  -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 801,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 802,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 803,
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
            stn_c, stn_d,
        )

        push!(stn_a.departures, leg_ab)
        push!(stn_b.arrivals,   leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals,   leg_bc)
        push!(stn_c.departures, leg_cd)
        push!(stn_d.arrivals,   leg_cd)

        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_b.connections, cnx_b)
        push!(leg_ab.connect_to,   cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_c.connections, cnx_c)
        push!(leg_bc.connect_to,   cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        ns_ab = nonstop_connection(leg_ab, stn_a)
        ns_bc = nonstop_connection(leg_bc, stn_b)
        ns_cd = nonstop_connection(leg_cd, stn_c)
        push!(stn_a.connections, ns_ab)
        push!(stn_b.connections, ns_bc)
        push!(stn_c.connections, ns_cd)

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
            window_start = Date(2026, 1, 1),
            window_end   = Date(2026, 12, 31),
        )

        build_layer1!(graph)
        @test graph.layer1_built

        store = DuckDBStore()
        try
            export_layer1!(store, graph)

            # ── layer1_metadata: exactly 1 row with num_connections > 0 ─────────
            meta_rows = collect(
                DBInterface.execute(store.db, "SELECT * FROM layer1_metadata")
            )
            @test length(meta_rows) == 1

            meta = meta_rows[1]
            expected_total = sum(length(v) for (_, v) in graph.layer1; init = 0)
            @test Int(meta.num_connections) == expected_total
            @test Int(meta.num_connections) > 0

            # ── layer1_connections: row count matches in-memory total ──────────
            cnx_rows = collect(
                DBInterface.execute(store.db, "SELECT * FROM layer1_connections")
            )
            @test length(cnx_rows) == expected_total

            # ── spot-check via_station / destination values ───────────────────
            via_stns = Set(String(r.via_station) for r in cnx_rows)
            dests    = Set(String(r.destination)  for r in cnx_rows)

            # The only Layer 1 entry is (ORD, LAX): via_station=ORD, destination=LAX
            @test "ORD" in via_stns
            @test "LAX" in dests

            # ── idempotent: second export clears and re-inserts ───────────────
            export_layer1!(store, graph)
            meta2 = collect(
                DBInterface.execute(store.db, "SELECT * FROM layer1_metadata")
            )
            @test length(meta2) == 1
            @test Int(meta2[1].num_connections) == expected_total
        finally
            close(store)
        end
    end

    # ── export_layer1_parquet! ───────────────────────────────────────────────────

    @testset "export_layer1_parquet! writes Parquet files" begin
        using ItinerarySearch: export_layer1!, export_layer1_parquet!
        using DuckDB
        using DBInterface

        # Build the same 4-station JFK→ORD→DEN→LAX topology.
        stn_a = GraphStation(_make_stn_rec("JFK",  40.64,  -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97,  -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 901,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 902,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 903,
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
            stn_c, stn_d,
        )

        push!(stn_a.departures, leg_ab)
        push!(stn_b.arrivals,   leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals,   leg_bc)
        push!(stn_c.departures, leg_cd)
        push!(stn_d.arrivals,   leg_cd)

        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_b.connections, cnx_b)
        push!(leg_ab.connect_to,   cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_c.connections, cnx_c)
        push!(leg_bc.connect_to,   cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        ns_ab = nonstop_connection(leg_ab, stn_a)
        ns_bc = nonstop_connection(leg_bc, stn_b)
        ns_cd = nonstop_connection(leg_cd, stn_c)
        push!(stn_a.connections, ns_ab)
        push!(stn_b.connections, ns_bc)
        push!(stn_c.connections, ns_cd)

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
            window_start = Date(2026, 1, 1),
            window_end   = Date(2026, 12, 31),
        )

        build_layer1!(graph)
        @test graph.layer1_built

        store = DuckDBStore()
        try
            export_layer1!(store, graph)

            parquet_dir  = mktempdir()
            parquet_path = joinpath(parquet_dir, "layer1")
            export_layer1_parquet!(parquet_path, store)

            @test isfile(parquet_path * "_connections.parquet")
            @test isfile(parquet_path * "_metadata.parquet")
            @test filesize(parquet_path * "_connections.parquet") > 0
            @test filesize(parquet_path * "_metadata.parquet") > 0
        finally
            close(store)
        end
    end

    # ── import_layer1!: round-trip export → import ──────────────────────────────

    @testset "import_layer1! round-trip export→import" begin
        using ItinerarySearch: export_layer1!, import_layer1!
        using DuckDB
        using DBInterface

        stn_a = GraphStation(_make_stn_rec("JFK",  40.64,  -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97,  -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))

        # Helper: rebuild LegRecord with a unique row_number
        function _with_row_number(rec::LegRecord, rn::UInt64)::LegRecord
            LegRecord(
                airline            = rec.airline,
                flt_no             = rec.flt_no,
                operational_suffix = rec.operational_suffix,
                itin_var           = rec.itin_var,
                itin_var_overflow  = rec.itin_var_overflow,
                leg_seq            = rec.leg_seq,
                svc_type           = rec.svc_type,
                org                = rec.org,
                dst                = rec.dst,
                pax_dep            = rec.pax_dep,
                pax_arr            = rec.pax_arr,
                ac_dep             = rec.ac_dep,
                ac_arr             = rec.ac_arr,
                dep_utc_offset     = rec.dep_utc_offset,
                arr_utc_offset     = rec.arr_utc_offset,
                dep_date_var       = rec.dep_date_var,
                arr_date_var       = rec.arr_date_var,
                eqp                = rec.eqp,
                body_type          = rec.body_type,
                dep_term           = rec.dep_term,
                arr_term           = rec.arr_term,
                aircraft_owner     = rec.aircraft_owner,
                operating_date     = rec.operating_date,
                day_of_week        = rec.day_of_week,
                eff_date           = rec.eff_date,
                disc_date          = rec.disc_date,
                frequency          = rec.frequency,
                mct_status_dep     = rec.mct_status_dep,
                mct_status_arr     = rec.mct_status_arr,
                trc                = rec.trc,
                trc_overflow       = rec.trc_overflow,
                record_serial      = rec.record_serial,
                row_number         = rn,
                segment_hash       = rec.segment_hash,
                distance           = rec.distance,
                codeshare_airline  = rec.codeshare_airline,
                codeshare_flt_no   = rec.codeshare_flt_no,
                dei_10             = rec.dei_10,
                wet_lease          = rec.wet_lease,
                dei_127            = rec.dei_127,
                prbd               = rec.prbd,
            )
        end

        leg_ab = GraphLeg(
            _with_row_number(
                _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 1201,
                    pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
                UInt64(1201),
            ),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _with_row_number(
                _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 1202,
                    pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
                UInt64(1202),
            ),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _with_row_number(
                _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 1203,
                    pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
                UInt64(1203),
            ),
            stn_c, stn_d,
        )

        push!(stn_a.departures, leg_ab)
        push!(stn_b.arrivals,   leg_ab)
        push!(stn_b.departures, leg_bc)
        push!(stn_c.arrivals,   leg_bc)
        push!(stn_c.departures, leg_cd)
        push!(stn_d.arrivals,   leg_cd)

        cnx_b = GraphConnection(
            from_leg = leg_ab, to_leg = leg_bc, station = stn_b,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_b.connections, cnx_b)
        push!(leg_ab.connect_to,   cnx_b)
        push!(leg_bc.connect_from, cnx_b)

        cnx_c = GraphConnection(
            from_leg = leg_bc, to_leg = leg_cd, station = stn_c,
            valid_from = UInt32(20260101), valid_to = UInt32(20261231),
            valid_days = UInt8(0x7f),
        )
        push!(stn_c.connections, cnx_c)
        push!(leg_bc.connect_to,   cnx_c)
        push!(leg_cd.connect_from, cnx_c)

        ns_ab = nonstop_connection(leg_ab, stn_a)
        ns_bc = nonstop_connection(leg_bc, stn_b)
        ns_cd = nonstop_connection(leg_cd, stn_c)
        push!(stn_a.connections, ns_ab)
        push!(stn_b.connections, ns_bc)
        push!(stn_c.connections, ns_cd)

        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
            window_start = Date(2026, 1, 1),
            window_end   = Date(2026, 12, 31),
        )

        build_layer1!(graph)
        @test graph.layer1_built

        expected_total = sum(length(v) for (_, v) in graph.layer1; init = 0)
        @test expected_total > 0

        store = DuckDBStore()
        try
            export_layer1!(store, graph)

            # Clear in-memory layer1 and reset flag, keep legs/connections intact
            empty!(graph.layer1)
            graph.layer1_built = false

            # Import should succeed and restore the same count
            result = import_layer1!(store, graph)
            @test result == true
            @test graph.layer1_built == true

            imported_total = sum(length(v) for (_, v) in graph.layer1; init = 0)
            @test imported_total == expected_total

            # Spot-check: (ORD, LAX) entry exists with valid live leg pointers
            key = (StationCode("ORD"), StationCode("LAX"))
            @test haskey(graph.layer1, key)
            osc = graph.layer1[key][1]::OneStopConnection
            @test osc.via_leg.record.row_number > UInt64(0)
            @test osc.first.from_leg.record.row_number > UInt64(0)
            @test osc.second.to_leg.record.row_number > UInt64(0)
        finally
            close(store)
        end
    end

    # ── import_layer1!: staleness detection ────────────────────────────────────

    @testset "import_layer1! staleness detection" begin
        using ItinerarySearch: export_layer1!, import_layer1!
        using DuckDB

        # Build and export a graph with one leg set
        stn_a = GraphStation(_make_stn_rec("JFK",  40.64,  -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD",  41.97,  -87.91))
        stn_c = GraphStation(_make_stn_rec("DEN",  39.86, -104.67))
        stn_d = GraphStation(_make_stn_rec("LAX",  33.94, -118.41))

        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 1301,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        leg_bc = GraphLeg(
            _make_leg_rec(org = "ORD", dst = "DEN", flt_no = 1302,
                pax_dep = Int16(660), pax_arr = Int16(780), distance = 920.0f0),
            stn_b, stn_c,
        )
        leg_cd = GraphLeg(
            _make_leg_rec(org = "DEN", dst = "LAX", flt_no = 1303,
                pax_dep = Int16(840), pax_arr = Int16(960), distance = 860.0f0),
            stn_c, stn_d,
        )

        graph_orig = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
                StationCode("DEN") => stn_c,
                StationCode("LAX") => stn_d,
            ),
            legs = [leg_ab, leg_bc, leg_cd],
            config = SearchConfig(circuity_factor = 3.0),
            window_start = Date(2026, 1, 1),
            window_end   = Date(2026, 12, 31),
        )
        build_layer1!(graph_orig)

        store = DuckDBStore()
        try
            export_layer1!(store, graph_orig)

            # Build a different graph with a different leg set — fingerprint differs
            stn_x = GraphStation(_make_stn_rec("SFO",  37.61, -122.38))
            stn_y = GraphStation(_make_stn_rec("SEA",  47.44, -122.30))
            leg_xy = GraphLeg(
                _make_leg_rec(org = "SFO", dst = "SEA", flt_no = 2001,
                    pax_dep = Int16(480), pax_arr = Int16(600), distance = 680.0f0),
                stn_x, stn_y,
            )
            graph_new = FlightGraph(
                stations = Dict(
                    StationCode("SFO") => stn_x,
                    StationCode("SEA") => stn_y,
                ),
                legs = [leg_xy],
                config = SearchConfig(circuity_factor = 3.0),
            )

            result = import_layer1!(store, graph_new)
            @test result == false
            @test graph_new.layer1_built == false
        finally
            close(store)
        end
    end

    # ── import_layer1!: empty store returns false ───────────────────────────────

    @testset "import_layer1! empty store returns false" begin
        using ItinerarySearch: import_layer1!
        using DuckDB

        stn_a = GraphStation(_make_stn_rec("JFK", 40.64, -73.78))
        stn_b = GraphStation(_make_stn_rec("ORD", 41.97, -87.91))
        leg_ab = GraphLeg(
            _make_leg_rec(org = "JFK", dst = "ORD", flt_no = 1401,
                pax_dep = Int16(480), pax_arr = Int16(570), distance = 740.0f0),
            stn_a, stn_b,
        )
        graph = FlightGraph(
            stations = Dict(
                StationCode("JFK") => stn_a,
                StationCode("ORD") => stn_b,
            ),
            legs = [leg_ab],
            config = SearchConfig(circuity_factor = 3.0),
        )

        store = DuckDBStore()
        try
            # Fresh store — layer1_metadata is empty → must return false
            result = import_layer1!(store, graph)
            @test result == false
            @test graph.layer1_built == false
        finally
            close(store)
        end
    end

    # ── _is_valid_on_date helper ────────────────────────────────────────────────

    @testset "_is_valid_on_date" begin
        using ItinerarySearch: _is_valid_on_date

        osc = OneStopConnection(
            valid_from = UInt32(20260401),
            valid_to = UInt32(20260630),
            valid_days = UInt8(0x15),  # Mon=1, Wed=4, Fri=16 → 0b0010101
        )

        # Within window on a Monday (bit 0 set)
        @test _is_valid_on_date(osc, UInt32(20260601), DOW_MON)

        # Within window on a Wednesday (bit 2 set)
        @test _is_valid_on_date(osc, UInt32(20260603), DOW_WED)

        # Within window but Tuesday (bit 1 not in 0x15)
        @test !_is_valid_on_date(osc, UInt32(20260602), DOW_TUE)

        # Before window
        @test !_is_valid_on_date(osc, UInt32(20260331), DOW_MON)

        # After window
        @test !_is_valid_on_date(osc, UInt32(20260701), DOW_MON)
    end

end

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

        # ── Verify (JFK, LAX) entry exists ────────────────────────────────────

        key = (StationCode("JFK"), StationCode("LAX"))
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

        # No reverse path (LAX → JFK) should exist
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

        key = (StationCode("JFK"), StationCode("LAX"))
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
        @test !haskey(graph.layer1, (StationCode("JFK"), StationCode("LAX")))
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

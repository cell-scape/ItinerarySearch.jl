using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Itinerary Rules" begin

    # ── Test helpers ──────────────────────────────────────────────────────────

    function _itn_station_record(code, country, region; latitude=0.0, longitude=0.0)
        StationRecord(
            code=StationCode(code),
            country=InlineString3(country),
            state=InlineString3(""),
            city=InlineString3(""),
            region=InlineString3(region),
            latitude=latitude,
            longitude=longitude,
            utc_offset=Int16(0),
        )
    end

    function _itn_leg_record(;
        carrier="UA",
        flight_number=100,
        departure_station="ORD",
        arrival_station="LHR",
        passenger_departure_time=Int16(540),
        passenger_arrival_time=Int16(1320),
        leg_sequence_number=UInt8(1),
        traffic_restriction_for_leg="",
        distance=1000.0f0,
        frequency=0x7f,
    )
        LegRecord(
            carrier=AirlineCode(carrier),
            flight_number=Int16(flight_number),
            operational_suffix=' ',
            itinerary_var_id=UInt8(1),
            itinerary_var_overflow=' ',
            leg_sequence_number=leg_sequence_number,
            service_type='J',
            departure_station=StationCode(departure_station),
            arrival_station=StationCode(arrival_station),
            passenger_departure_time=Int16(passenger_departure_time),
            passenger_arrival_time=Int16(passenger_arrival_time),
            aircraft_departure_time=Int16(passenger_departure_time),
            aircraft_arrival_time=Int16(passenger_arrival_time),
            departure_utc_offset=Int16(0),
            arrival_utc_offset=Int16(0),
            departure_date_variation=Int8(0),
            arrival_date_variation=Int8(0),
            aircraft_type=InlineString7("738"),
            body_type='N',
            departure_terminal=InlineString3("1"),
            arrival_terminal=InlineString3("1"),
            aircraft_owner=AirlineCode(carrier),
            operating_date=UInt32(20260101),
            day_of_week=UInt8(1),
            effective_date=UInt32(20260101),
            discontinue_date=UInt32(20261231),
            frequency=UInt8(frequency),
            dep_intl_dom='D',
            arr_intl_dom='D',
            traffic_restriction_for_leg=InlineString15(traffic_restriction_for_leg),
            traffic_restriction_overflow=' ',
            record_serial=UInt32(1),
            row_number=UInt64(1),
            segment_hash=UInt64(0),
            distance=Distance(distance),
            operating_carrier=AirlineCode(""),
            operating_flight_number=Int16(0),
            dei_10="",
            wet_lease=false,
            dei_127="",
            prbd=InlineString31(""),
        )
    end

    # Build a nonstop Itinerary (1 connection, from_leg === to_leg).
    function _nonstop_itn(;
        status=StatusBits(DOW_MON | DOW_TUE | DOW_WED | DOW_THU | DOW_FRI),
        total_distance=Distance(1000.0f0),
        market_distance=Distance(1000.0f0),
        num_stops=Int16(0),
        leg_rec=_itn_leg_record(),
    )
        org_stn = GraphStation(_itn_station_record("ORD", "US", "NAM"))
        dst_stn = GraphStation(_itn_station_record("LHR", "GB", "EUR"))
        leg = GraphLeg(leg_rec, org_stn, dst_stn)
        cp = nonstop_connection(leg, org_stn)
        Itinerary(
            connections=GraphConnection[cp],
            status=status,
            total_distance=total_distance,
            market_distance=market_distance,
            num_stops=num_stops,
        )
    end

    # Build a 1-stop Itinerary (2 connections).
    function _oneStop_itn(;
        status=StatusBits(DOW_MON | DOW_WED | DOW_FRI),
        total_distance=Distance(2000.0f0),
        market_distance=Distance(1500.0f0),
        num_stops=Int16(1),
        from_rec=_itn_leg_record(departure_station="JFK", arrival_station="ORD", distance=1000.0f0),
        to_rec=_itn_leg_record(departure_station="ORD", arrival_station="LHR", distance=3000.0f0),
    )
        org_stn = GraphStation(_itn_station_record("JFK", "US", "NAM"))
        cnx_stn = GraphStation(_itn_station_record("ORD", "US", "NAM"))
        dst_stn = GraphStation(_itn_station_record("LHR", "GB", "EUR"))
        leg1 = GraphLeg(from_rec, org_stn, cnx_stn)
        leg2 = GraphLeg(to_rec, cnx_stn, dst_stn)
        cp1 = nonstop_connection(leg1, org_stn)
        cp2 = nonstop_connection(leg2, cnx_stn)
        Itinerary(
            connections=GraphConnection[cp1, cp2],
            status=status,
            total_distance=total_distance,
            market_distance=market_distance,
            num_stops=num_stops,
        )
    end

    # Minimal mock context; rules access only the fields they need.
    function _mock_ctx(;
        scope=SCOPE_ALL,
        constraints=SearchConstraints(),
    )
        (
            config=SearchConfig(scope=scope),
            constraints=constraints,
        )
    end

    # ── Return-code constants exported ───────────────────────────────────────

    @testset "Itinerary return-code constants" begin
        @test FAIL_ITN_SCOPE    < 0
        @test FAIL_ITN_OPDAYS   < 0
        @test FAIL_ITN_CIRCUITY < 0
        @test FAIL_ITN_SUPPCODE < 0
        @test FAIL_ITN_MAFT     < 0

        # All itinerary fail codes must be unique and not overlap cnx codes
        itn_codes = [FAIL_ITN_SCOPE, FAIL_ITN_OPDAYS, FAIL_ITN_CIRCUITY,
                     FAIL_ITN_SUPPCODE, FAIL_ITN_MAFT]
        @test allunique(itn_codes)

        cnx_codes = [FAIL_SCOPE, FAIL_ONLINE, FAIL_CODESHARE, FAIL_INTERLINE,
                     FAIL_TIME_MIN, FAIL_TIME_MAX, FAIL_OPDAYS, FAIL_SUPPCODE,
                     FAIL_MAFT, FAIL_CIRCUITY, FAIL_TRFREST]
        @test isempty(intersect(Set(itn_codes), Set(cnx_codes)))
    end

    # ── Rule 1: check_itn_scope ───────────────────────────────────────────────

    @testset "check_itn_scope" begin

        @testset "SCOPE_ALL passes both domestic and international" begin
            ctx = _mock_ctx(scope=SCOPE_ALL)
            dom_itn  = _nonstop_itn(status=StatusBits(DOW_MON))
            intl_itn = _nonstop_itn(status=StatusBits(DOW_MON | STATUS_INTERNATIONAL))
            @test check_itn_scope(dom_itn,  ctx) == PASS
            @test check_itn_scope(intl_itn, ctx) == PASS
        end

        @testset "SCOPE_DOM passes domestic, rejects international" begin
            ctx = _mock_ctx(scope=SCOPE_DOM)
            dom_itn  = _nonstop_itn(status=StatusBits(DOW_MON))
            intl_itn = _nonstop_itn(status=StatusBits(DOW_MON | STATUS_INTERNATIONAL))
            @test check_itn_scope(dom_itn,  ctx) == PASS
            @test check_itn_scope(intl_itn, ctx) == FAIL_ITN_SCOPE
        end

        @testset "SCOPE_INTL passes international, rejects domestic" begin
            ctx = _mock_ctx(scope=SCOPE_INTL)
            dom_itn  = _nonstop_itn(status=StatusBits(DOW_MON))
            intl_itn = _nonstop_itn(status=StatusBits(DOW_MON | STATUS_INTERNATIONAL))
            @test check_itn_scope(dom_itn,  ctx) == FAIL_ITN_SCOPE
            @test check_itn_scope(intl_itn, ctx) == PASS
        end
    end

    # ── Rule 2: check_itn_opdays ──────────────────────────────────────────────

    @testset "check_itn_opdays" begin
        ctx = _mock_ctx()

        @testset "passes when at least one DOW bit is set" begin
            itn = _nonstop_itn(status=StatusBits(DOW_MON))
            @test check_itn_opdays(itn, ctx) == PASS
        end

        @testset "fails when no DOW bits are set" begin
            itn = _nonstop_itn(status=StatusBits(0))
            @test check_itn_opdays(itn, ctx) == FAIL_ITN_OPDAYS
        end

        @testset "passes with all DOW bits set" begin
            itn = _nonstop_itn(status=DOW_MASK)
            @test check_itn_opdays(itn, ctx) == PASS
        end

        @testset "STATUS_ bits alone do not count as DOW" begin
            # STATUS_INTERNATIONAL is bit 7; DOW_MASK covers bits 0-6
            itn = _nonstop_itn(status=STATUS_INTERNATIONAL)
            @test check_itn_opdays(itn, ctx) == FAIL_ITN_OPDAYS
        end
    end

    # ── Rule 3: check_itn_circuity ────────────────────────────────────────────

    @testset "check_itn_circuity" begin
        ctx = _mock_ctx()

        @testset "passes when itinerary is empty" begin
            itn = Itinerary()
            @test check_itn_circuity(itn, ctx) == PASS
        end

        @testset "passes when market_distance is zero" begin
            itn = _nonstop_itn(total_distance=Distance(999.0f0), market_distance=Distance(0.0f0))
            @test check_itn_circuity(itn, ctx) == PASS
        end

        @testset "passes when total_distance <= factor * market_distance + extra" begin
            # ParameterSet defaults: max_circuity=2.5, domestic_circuity_extra_miles=500
            # 1000 <= 2.5 * 1000 + 500 = 3000 => PASS
            itn = _nonstop_itn(total_distance=Distance(1000.0f0), market_distance=Distance(1000.0f0))
            @test check_itn_circuity(itn, ctx) == PASS
        end

        @testset "fails when total_distance >> market_distance" begin
            # 10000 > 2.5 * 500 + 500 = 1750 => FAIL
            itn = _nonstop_itn(
                total_distance=Distance(10000.0f0),
                market_distance=Distance(500.0f0),
            )
            @test check_itn_circuity(itn, ctx) == FAIL_ITN_CIRCUITY
        end

        @testset "uses constraints.defaults.max_circuity" begin
            # Use a tight factor of 1.0 with no extra miles
            tight = SearchConstraints(defaults=ParameterSet(max_circuity=1.0, domestic_circuity_extra_miles=0.0))
            ctx_tight = _mock_ctx(constraints=tight)
            # total=2000, market=1000 => 2000 > 1.0 * 1000 + 0 => FAIL
            itn = _nonstop_itn(total_distance=Distance(2000.0f0), market_distance=Distance(1000.0f0))
            @test check_itn_circuity(itn, ctx_tight) == FAIL_ITN_CIRCUITY
        end

        @testset "international route uses international_circuity_extra_miles" begin
            # ParameterSet defaults: max_circuity=2.5, international_circuity_extra_miles=1000
            # total=3400, market=1000 => 3400 <= 2.5*1000+1000=3500 => PASS
            # total=3600, market=1000 => 3600 > 3500 => FAIL
            ctx_intl = _mock_ctx()
            intl_status = StatusBits(DOW_MON | STATUS_INTERNATIONAL)
            itn_pass = _nonstop_itn(
                status=intl_status,
                total_distance=Distance(3400.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity(itn_pass, ctx_intl) == PASS
            itn_fail = _nonstop_itn(
                status=intl_status,
                total_distance=Distance(3600.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity(itn_fail, ctx_intl) == FAIL_ITN_CIRCUITY
        end
    end

    # ── Rule 4: check_itn_suppcodes ───────────────────────────────────────────

    @testset "check_itn_suppcodes" begin
        ctx = _mock_ctx()

        @testset "passes when TRC is empty" begin
            itn = _nonstop_itn()
            @test check_itn_suppcodes(itn, ctx) == PASS
        end

        @testset "passes when TRC has an informational-only code ('Z')" begin
            # 'Z' is informational/ignored — must not suppress any itinerary type
            leg_rec = _itn_leg_record(traffic_restriction_for_leg="Z", leg_sequence_number=UInt8(1))
            itn = _nonstop_itn(leg_rec=leg_rec)
            @test check_itn_suppcodes(itn, ctx) == PASS
        end

        @testset "fails when from_leg TRC has 'I' at leg_seq" begin
            leg_rec = _itn_leg_record(traffic_restriction_for_leg="I", leg_sequence_number=UInt8(1))
            itn = _nonstop_itn(leg_rec=leg_rec)
            @test check_itn_suppcodes(itn, ctx) == FAIL_ITN_SUPPCODE
        end

        @testset "fails when a non-first leg has 'I' at its leg_seq" begin
            clean_rec = _itn_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg="",  leg_sequence_number=UInt8(1))
            supp_rec  = _itn_leg_record(departure_station="ORD", arrival_station="LHR", traffic_restriction_for_leg="XI", leg_sequence_number=UInt8(2))
            itn = _oneStop_itn(from_rec=clean_rec, to_rec=supp_rec)
            @test check_itn_suppcodes(itn, ctx) == FAIL_ITN_SUPPCODE
        end

        @testset "passes when 'I' is at a different leg_seq position (informational code at seq)" begin
            # traffic_restriction_for_leg="ZI", leg_sequence_number=1 => _get_trc returns 'Z' (informational)
            # 'I' is at position 2 but this leg has seq=1, so it is not seen
            leg_rec = _itn_leg_record(traffic_restriction_for_leg="ZI", leg_sequence_number=UInt8(1))
            itn = _nonstop_itn(leg_rec=leg_rec)
            @test check_itn_suppcodes(itn, ctx) == PASS
        end
    end

    # ── Rule 5: check_itn_maft ────────────────────────────────────────────────

    @testset "check_itn_maft" begin
        ctx = _mock_ctx()

        @testset "passes when itinerary is empty" begin
            itn = Itinerary()
            @test check_itn_maft(itn, ctx) == PASS
        end

        @testset "passes when market_distance is zero" begin
            itn = _nonstop_itn(market_distance=Distance(0.0f0))
            @test check_itn_maft(itn, ctx) == PASS
        end

        @testset "nonstop always passes (num_stops < 1 short-circuit)" begin
            # New formula: nonstop itineraries skip MAFT check entirely
            leg_rec = _itn_leg_record(distance=1000.0f0)
            itn = _nonstop_itn(
                market_distance=Distance(1000.0f0),
                num_stops=Int16(0),
                leg_rec=leg_rec,
            )
            @test check_itn_maft(itn, ctx) == PASS
        end

        @testset "1-stop passes when block time within MAFT" begin
            # gc_dist=1500, num_stops=1
            # base = max((1500/400)*60, 30) = max(225, 30) = 225 min
            # stop_allowance = 240 (1 stop), taxi = 30
            # maft = 225 + 240 + 30 = 495 min
            # from_rec: dep=0, arr=150 => block = 150 min
            # to_rec:   dep=300, arr=450 => block = 150 min
            # total_bt = 300 min; 300 <= 495 => PASS
            from_rec = _itn_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(0),
                                        passenger_arrival_time=Int16(150),
                                        distance=1000.0f0)
            to_rec   = _itn_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(300),
                                        passenger_arrival_time=Int16(450),
                                        distance=1000.0f0)
            itn = _oneStop_itn(
                market_distance=Distance(1500.0f0),
                total_distance=Distance(2000.0f0),
                num_stops=Int16(1),
                from_rec=from_rec,
                to_rec=to_rec,
            )
            @test check_itn_maft(itn, ctx) == PASS
        end

        @testset "1-stop fails when block time exceeds MAFT" begin
            # gc_dist=100, num_stops=1
            # base = max((100/400)*60, 30) = max(15, 30) = 30 min
            # stop_allowance = 240 (1 stop), taxi = 30
            # maft = 30 + 240 + 30 = 300 min
            # from_rec: dep=0, arr=300 => block = 300 min
            # to_rec:   dep=400, arr=700 => block = 300 min
            # total_bt = 600 min; 600 > 300 => FAIL
            from_rec = _itn_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(0),
                                        passenger_arrival_time=Int16(300),
                                        distance=500.0f0)
            to_rec   = _itn_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(400),
                                        passenger_arrival_time=Int16(700),
                                        distance=500.0f0)
            itn = _oneStop_itn(
                market_distance=Distance(100.0f0),
                total_distance=Distance(1000.0f0),
                num_stops=Int16(1),
                from_rec=from_rec,
                to_rec=to_rec,
            )
            @test check_itn_maft(itn, ctx) == FAIL_ITN_MAFT
        end
    end

    # ── build_itn_rules ───────────────────────────────────────────────────────

    @testset "build_itn_rules" begin
        config = SearchConfig()
        rules = build_itn_rules(config)

        @test length(rules) == 5
        @test rules[1] === check_itn_scope
        @test rules[2] === check_itn_opdays
        @test rules[3] === check_itn_circuity
        @test rules[4] === check_itn_suppcodes
        @test rules[5] === check_itn_maft

        # All elements are callable with (Itinerary, ctx) signature
        ctx = _mock_ctx()
        itn = _nonstop_itn()
        @test all(r -> applicable(r, itn, ctx), rules)
    end

end

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

    # Build a 2-stop Itinerary (3 connections).
    function _twoStop_itn(;
        status=StatusBits(DOW_MON | DOW_WED | DOW_FRI),
        total_distance=Distance(2000.0f0),
        market_distance=Distance(1000.0f0),
        num_stops=Int16(2),
        origin_code="DEN",
        destination_code="SFO",
    )
        org_stn = GraphStation(_itn_station_record(origin_code, "US", "NAM"))
        cnx1_stn = GraphStation(_itn_station_record("ORD", "US", "NAM"))
        cnx2_stn = GraphStation(_itn_station_record("LAX", "US", "NAM"))
        dst_stn = GraphStation(_itn_station_record(destination_code, "US", "NAM"))
        leg1 = GraphLeg(_itn_leg_record(departure_station=origin_code, arrival_station="ORD", distance=700.0f0), org_stn, cnx1_stn)
        leg2 = GraphLeg(_itn_leg_record(departure_station="ORD", arrival_station="LAX", distance=800.0f0), cnx1_stn, cnx2_stn)
        leg3 = GraphLeg(_itn_leg_record(departure_station="LAX", arrival_station=destination_code, distance=500.0f0), cnx2_stn, dst_stn)
        cp1 = nonstop_connection(leg1, org_stn)
        cp2 = nonstop_connection(leg2, cnx1_stn)
        cp3 = nonstop_connection(leg3, cnx2_stn)
        Itinerary(
            connections=GraphConnection[cp1, cp2, cp3],
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
        @test FAIL_ITN_SCOPE          < 0
        @test FAIL_ITN_OPDAYS         < 0
        @test FAIL_ITN_CIRCUITY       < 0
        @test FAIL_ITN_SUPPCODE       < 0
        @test FAIL_ITN_MAFT           < 0
        @test FAIL_ITN_ELAPSED        < 0
        @test FAIL_ITN_DISTANCE       < 0
        @test FAIL_ITN_STOPS          < 0
        @test FAIL_ITN_FLIGHT_TIME    < 0
        @test FAIL_ITN_LAYOVER        < 0
        @test FAIL_ITN_CARRIER        < 0
        @test FAIL_ITN_INTERLINE_DCNX < 0
        @test FAIL_ITN_CRS_CNX        < 0

        # All itinerary fail codes must be unique and not overlap cnx codes
        itn_codes = [FAIL_ITN_SCOPE, FAIL_ITN_OPDAYS, FAIL_ITN_CIRCUITY,
                     FAIL_ITN_SUPPCODE, FAIL_ITN_MAFT,
                     FAIL_ITN_ELAPSED, FAIL_ITN_DISTANCE, FAIL_ITN_STOPS,
                     FAIL_ITN_FLIGHT_TIME, FAIL_ITN_LAYOVER, FAIL_ITN_CARRIER,
                     FAIL_ITN_INTERLINE_DCNX, FAIL_ITN_CRS_CNX]
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

    # ── Rule 3: check_itn_circuity_range ────────────────────────────────────────────

    @testset "check_itn_circuity_range" begin
        ctx = _mock_ctx()

        @testset "passes when itinerary is empty" begin
            itn = Itinerary()
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "nonstop (num_stops=0) skipped — connection-level rule handles it" begin
            # After T5 rewire, nonstop itineraries bypass the itinerary-level check.
            # A nonstop that would otherwise exceed the ratio still returns PASS here.
            itn = _nonstop_itn(total_distance=Distance(10000.0f0), market_distance=Distance(500.0f0))
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "1-stop (num_stops=1) also skipped" begin
            itn = _oneStop_itn(total_distance=Distance(10000.0f0), market_distance=Distance(500.0f0))
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "2-stop passes when market_distance is zero" begin
            itn = _twoStop_itn(total_distance=Distance(999.0f0), market_distance=Distance(0.0f0))
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "2-stop passes when total_distance <= tier_factor * market_distance + extra" begin
            # DEFAULT_CIRCUITY_TIERS: market=1000mi → tier [800,2000] → factor=1.5
            # max_dist = 1.5*1000 + 500 (domestic extra) = 2000; 1800 <= 2000 => PASS
            itn = _twoStop_itn(total_distance=Distance(1800.0f0), market_distance=Distance(1000.0f0))
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "2-stop fails when total_distance >> market_distance" begin
            # DEFAULT_CIRCUITY_TIERS: market=500mi → tier [250,800] → factor=1.9
            # max_dist = 1.9*500 + 500 = 1450; 10000 > 1450 => FAIL
            itn = _twoStop_itn(
                total_distance=Distance(10000.0f0),
                market_distance=Distance(500.0f0),
            )
            @test check_itn_circuity_range(itn, ctx) == FAIL_ITN_CIRCUITY
        end

        @testset "max_circuity ceiling caps tier factor" begin
            # Tight max_circuity=1.0 overrides tier factor; no extra miles.
            # 2-stop: total=2000, market=1000 => 2000 > min(1.5, 1.0)*1000+0 = 1000 => FAIL
            tight = SearchConstraints(defaults=ParameterSet(max_circuity=1.0, domestic_circuity_extra_miles=0.0))
            ctx_tight = _mock_ctx(constraints=tight)
            itn = _twoStop_itn(total_distance=Distance(2000.0f0), market_distance=Distance(1000.0f0))
            @test check_itn_circuity_range(itn, ctx_tight) == FAIL_ITN_CIRCUITY
        end

        @testset "international 2-stop uses international_circuity_extra_miles" begin
            # DEFAULT_CIRCUITY_TIERS: market=1000mi → factor=1.5; international extra=1000
            # max_dist = 1.5*1000+1000 = 2500; 2400 <= 2500 => PASS; 2600 > 2500 => FAIL
            intl_status = StatusBits(DOW_MON | STATUS_INTERNATIONAL)
            itn_pass = _twoStop_itn(
                status=intl_status,
                total_distance=Distance(2400.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity_range(itn_pass, ctx) == PASS
            itn_fail = _twoStop_itn(
                status=intl_status,
                total_distance=Distance(2600.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity_range(itn_fail, ctx) == FAIL_ITN_CIRCUITY
        end
    end

    # ── Rule 3b: check_itn_circuity_range — tier-based ───────────────────────────

    @testset "check_itn_circuity_range — tier-based" begin
        ctx = _mock_ctx()

        @testset "nonstop always PASS (covered by connection-level)" begin
            itn = _nonstop_itn(total_distance=Distance(1000.0f0), market_distance=Distance(1000.0f0))
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "1-stop still skipped" begin
            itn = _oneStop_itn(total_distance=Distance(1000.0f0), market_distance=Distance(500.0f0))
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "2-stop accepted when flown <= tier_factor * market_dist + extra" begin
            # DEFAULT_CIRCUITY_TIERS: 1000mi → factor 1.5; domestic extra 500
            # max_dist = 1.5*1000+500 = 2000; 1800 <= 2000 => PASS
            itn = _twoStop_itn(
                total_distance=Distance(1800.0f0),
                market_distance=Distance(1000.0f0),
                origin_code="DEN",
                destination_code="SFO",
            )
            @test check_itn_circuity_range(itn, ctx) == PASS
        end

        @testset "2-stop rejected when flown exceeds tier factor" begin
            # DEFAULT_CIRCUITY_TIERS: 1000mi → factor 1.5; domestic extra 500
            # max_dist = 1.5*1000+500 = 2000; 3500 > 2000 => FAIL
            itn = _twoStop_itn(
                total_distance=Distance(3500.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity_range(itn, ctx) == FAIL_ITN_CIRCUITY
        end

        @testset "max_circuity ceiling kicks in when less than tier factor" begin
            # max_circuity=1.1 caps factor (tier gives 1.5); no extra miles
            # max_dist = 1.1*1000+0 = 1100; 1200 > 1100 => FAIL
            ctx_cap = _mock_ctx(constraints=SearchConstraints(
                defaults=ParameterSet(max_circuity=1.1, domestic_circuity_extra_miles=0.0),
            ))
            itn = _twoStop_itn(
                total_distance=Distance(1200.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity_range(itn, ctx_cap) == FAIL_ITN_CIRCUITY
        end

        @testset "min_circuity floor rejects overly-direct itineraries" begin
            # min_circuity=1.3; ratio=1100/1000=1.1 < 1.3 => FAIL
            ctx_floor = _mock_ctx(constraints=SearchConstraints(
                defaults=ParameterSet(min_circuity=1.3),
            ))
            itn = _twoStop_itn(
                total_distance=Distance(1100.0f0),
                market_distance=Distance(1000.0f0),
            )
            @test check_itn_circuity_range(itn, ctx_floor) == FAIL_ITN_CIRCUITY
        end

        @testset "market override changes the effective factor" begin
            # Override DEN->SFO: single tier at factor=3.5 (Inf threshold)
            # max_dist = 3.5*1000+500 = 4000; 3000 <= 4000 => PASS (default factor 1.5 would reject it)
            override_ctx = _mock_ctx(constraints=SearchConstraints(
                overrides=[MarketOverride(
                    origin=StationCode("DEN"),
                    destination=StationCode("SFO"),
                    carrier=WILDCARD_AIRLINE,
                    params=ParameterSet(circuity_tiers=[CircuityTier(Inf, 3.5)]),
                    specificity=UInt32(1000),
                )],
            ))
            itn = _twoStop_itn(
                total_distance=Distance(3000.0f0),
                market_distance=Distance(1000.0f0),
                origin_code="DEN",
                destination_code="SFO",
            )
            @test check_itn_circuity_range(itn, override_ctx) == PASS
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

        # Default config: maft_enabled=true, interline_dcnx_enabled=true,
        # crs_cnx_enabled=true; default constraints add no range rules.
        # Expected chain: scope, opdays, circuity_range, suppcodes, maft,
        #                 interline_dcnx, crs_cnx  (7 rules)
        @test length(rules) == 7
        @test rules[1] === check_itn_scope
        @test rules[2] === check_itn_opdays
        @test rules[3] === check_itn_circuity_range
        @test rules[4] === check_itn_suppcodes
        @test rules[5] === check_itn_maft
        @test rules[6] === check_itn_interline_dcnx
        @test rules[7] === check_itn_crs_cnx

        # All elements are callable with (Itinerary, ctx) signature
        ctx = _mock_ctx()
        itn = _nonstop_itn()
        @test all(r -> applicable(r, itn, ctx), rules)

        @testset "maft_enabled=false omits check_itn_maft" begin
            cfg_no_maft = SearchConfig(maft_enabled=false)
            r2 = build_itn_rules(cfg_no_maft)
            @test check_itn_maft ∉ r2
            @test check_itn_scope ∈ r2
        end

        @testset "interline_dcnx_enabled=false omits interline rule" begin
            cfg = SearchConfig(interline_dcnx_enabled=false)
            r3 = build_itn_rules(cfg)
            @test check_itn_interline_dcnx ∉ r3
        end

        @testset "crs_cnx_enabled=false omits CRS rule" begin
            cfg = SearchConfig(crs_cnx_enabled=false)
            r4 = build_itn_rules(cfg)
            @test check_itn_crs_cnx ∉ r4
        end

        @testset "non-default constraints add range rules" begin
            c = SearchConstraints(defaults=ParameterSet(min_elapsed=Int32(60)))
            r5 = build_itn_rules(config; constraints=c)
            @test check_itn_elapsed_range ∈ r5
        end

        @testset "carrier filter adds carrier rule" begin
            c = SearchConstraints(defaults=ParameterSet(allow_carriers=Set([AirlineCode("UA")])))
            r6 = build_itn_rules(config; constraints=c)
            @test check_itn_carriers ∈ r6
        end
    end

end

using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Connection Rules" begin

    # ── Test helpers ──────────────────────────────────────────────────────────

    function _test_station_record(code, country, region; latitude=0.0, longitude=0.0)
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

    function _test_leg_record(;
        carrier="UA",
        flight_number=100,
        departure_station="ORD",
        arrival_station="LHR",
        passenger_departure_time=Int16(840),   # 14:00
        passenger_arrival_time=Int16(540),   # 09:00
        arrival_date_variation=Int8(0),
        dep_intl_dom='D',
        arr_intl_dom='D',
        traffic_restriction_for_leg="",
        leg_sequence_number=UInt8(1),
        distance=1000.0f0,
        frequency=0x7f,       # all days
        operating_carrier="",
        body_type='N',
        departure_terminal="1",
        arrival_terminal="1",
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
            arrival_date_variation=arrival_date_variation,
            aircraft_type=InlineString7("738"),
            body_type=body_type,
            departure_terminal=InlineString3(departure_terminal),
            arrival_terminal=InlineString3(arrival_terminal),
            aircraft_owner=AirlineCode(carrier),
            operating_date=UInt32(20260101),
            day_of_week=UInt8(1),
            effective_date=UInt32(20260101),
            discontinue_date=UInt32(20261231),
            frequency=UInt8(frequency),
            dep_intl_dom=dep_intl_dom,
            arr_intl_dom=arr_intl_dom,
            traffic_restriction_for_leg=InlineString15(traffic_restriction_for_leg),
            traffic_restriction_overflow=' ',
            record_serial=UInt32(1),
            row_number=UInt64(1),
            segment_hash=UInt64(0),
            distance=Distance(distance),
            operating_carrier=AirlineCode(operating_carrier),
            operating_flight_number=Int16(0),
            dei_10="",
            wet_lease=false,
            dei_127="",
            prbd=InlineString31(""),
        )
    end

    # Build a GraphConnection from two leg records and a connect station.
    # status is a caller-supplied StatusBits value (e.g. for intl/interline flags).
    function _test_connection(;
        from_rec=_test_leg_record(departure_station="JFK", arrival_station="ORD", passenger_departure_time=Int16(420), passenger_arrival_time=Int16(720)),
        to_rec=_test_leg_record(departure_station="ORD", arrival_station="LHR", passenger_departure_time=Int16(900), passenger_arrival_time=Int16(1380)),
        cnx_station_code="ORD",
        cnx_station_country="US",
        cnx_station_region="NAM",
        status=StatusBits(DOW_MON | DOW_TUE | DOW_WED | DOW_THU | DOW_FRI),
        is_through=false,
    )
        org_stn  = GraphStation(_test_station_record(from_rec.departure_station, "US", "NAM"))
        cnx_stn  = GraphStation(_test_station_record(cnx_station_code, cnx_station_country, cnx_station_region))
        dst_stn  = GraphStation(_test_station_record(to_rec.arrival_station, "GB", "EUR"))
        from_leg = GraphLeg(from_rec, org_stn, cnx_stn)
        to_leg   = GraphLeg(to_rec, cnx_stn, dst_stn)
        GraphConnection(
            from_leg=from_leg,
            to_leg=to_leg,
            station=cnx_stn,
            status=status,
            is_through=is_through,
        )
    end

    # Minimal mock context; rules access only the fields they need.
    function _mock_ctx(;
        scope=SCOPE_ALL,
        interline=INTERLINE_CODESHARE,
        constraints=SearchConstraints(),
        gc_cache=Dict{Tuple{StationCode,StationCode}, Float64}(),
        target_date=UInt32(0),
    )
        (
            config = SearchConfig(scope=scope, interline=interline),
            constraints = constraints,
            build_stats = BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache = Dict{MCTCacheKey, MCTResult}(),
            gc_cache = gc_cache,
            target_date = target_date,
            mct_selections = MCTSelectionRow[],
        )
    end

    # ── Return-code constants exported ───────────────────────────────────────

    @testset "Return-code constants" begin
        @test PASS > 0
        @test FAIL_ROUNDTRIP == 0
        @test FAIL_SCOPE     < 0
        @test FAIL_ONLINE    < 0
        @test FAIL_CODESHARE < 0
        @test FAIL_INTERLINE < 0
        @test FAIL_TIME_MIN  < 0
        @test FAIL_TIME_MAX  < 0
        @test FAIL_OPDAYS    < 0
        @test FAIL_SUPPCODE  < 0
        @test FAIL_MAFT      < 0
        @test FAIL_CIRCUITY  < 0
        @test FAIL_TRFREST   < 0
        # All fail codes must be unique
        codes = [FAIL_ROUNDTRIP, FAIL_SCOPE, FAIL_ONLINE, FAIL_CODESHARE,
                 FAIL_INTERLINE, FAIL_TIME_MIN, FAIL_TIME_MAX, FAIL_OPDAYS,
                 FAIL_SUPPCODE, FAIL_MAFT, FAIL_CIRCUITY, FAIL_TRFREST]
        @test allunique(codes)
    end

    # ── Rule 1: check_cnx_roundtrip ──────────────────────────────────────────

    @testset "check_cnx_roundtrip" begin
        ctx = _mock_ctx()

        @testset "no roundtrip — different O and D" begin
            cp = _test_connection()
            result = check_cnx_roundtrip(cp, ctx)
            @test result == PASS
            @test !is_roundtrip(cp.status)
        end

        @testset "roundtrip — from_leg.org == to_leg.dst" begin
            # from_leg: JFK→ORD  to_leg: ORD→JFK  => org JFK == dst JFK
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD")
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="JFK")
            org_stn  = GraphStation(_test_station_record("JFK", "US", "NAM"))
            cnx_stn  = GraphStation(_test_station_record("ORD", "US", "NAM"))
            dst_stn  = GraphStation(_test_station_record("JFK", "US", "NAM"))
            from_leg = GraphLeg(from_rec, org_stn, cnx_stn)
            to_leg   = GraphLeg(to_rec,   cnx_stn, dst_stn)
            cp = GraphConnection(
                from_leg=from_leg,
                to_leg=to_leg,
                station=cnx_stn,
                status=StatusBits(DOW_MON),
            )
            result = check_cnx_roundtrip(cp, ctx)
            @test result == PASS
            @test is_roundtrip(cp.status)
        end

        @testset "always passes even when roundtrip" begin
            # ensure it never returns a fail code
            from_rec = _test_leg_record(departure_station="LAX", arrival_station="SFO")
            to_rec   = _test_leg_record(departure_station="SFO", arrival_station="LAX")
            org_stn  = GraphStation(_test_station_record("LAX", "US", "NAM"))
            cnx_stn  = GraphStation(_test_station_record("SFO", "US", "NAM"))
            dst_stn  = GraphStation(_test_station_record("LAX", "US", "NAM"))
            from_leg = GraphLeg(from_rec, org_stn, cnx_stn)
            to_leg   = GraphLeg(to_rec,   cnx_stn, dst_stn)
            cp = GraphConnection(from_leg=from_leg, to_leg=to_leg, station=cnx_stn)
            @test check_cnx_roundtrip(cp, ctx) > 0
        end
    end

    # ── Rule 2: check_cnx_scope ───────────────────────────────────────────────

    @testset "check_cnx_scope" begin
        intl_status  = StatusBits(DOW_MON | STATUS_INTERNATIONAL)
        dom_status   = StatusBits(DOW_MON)

        @testset "SCOPE_ALL always passes" begin
            ctx = _mock_ctx(scope=SCOPE_ALL)
            cp_intl = _test_connection(status=intl_status)
            cp_dom  = _test_connection(status=dom_status)
            @test check_cnx_scope(cp_intl, ctx) == PASS
            @test check_cnx_scope(cp_dom,  ctx) == PASS
        end

        @testset "SCOPE_DOM passes domestic, rejects international" begin
            ctx = _mock_ctx(scope=SCOPE_DOM)
            cp_intl = _test_connection(status=intl_status)
            cp_dom  = _test_connection(status=dom_status)
            @test check_cnx_scope(cp_intl, ctx) == FAIL_SCOPE
            @test check_cnx_scope(cp_dom,  ctx) == PASS
        end

        @testset "SCOPE_INTL passes international, rejects domestic" begin
            ctx = _mock_ctx(scope=SCOPE_INTL)
            cp_intl = _test_connection(status=intl_status)
            cp_dom  = _test_connection(status=dom_status)
            @test check_cnx_scope(cp_intl, ctx) == PASS
            @test check_cnx_scope(cp_dom,  ctx) == FAIL_SCOPE
        end
    end

    # ── Rule 3: check_cnx_interline ───────────────────────────────────────────

    @testset "check_cnx_interline" begin
        online_status    = StatusBits(DOW_MON)
        codeshare_status = StatusBits(DOW_MON | STATUS_CODESHARE)
        interline_status = StatusBits(DOW_MON | STATUS_INTERLINE)
        intl_interline   = StatusBits(DOW_MON | STATUS_INTERLINE | STATUS_INTERNATIONAL)

        @testset "INTERLINE_ONLINE rejects codeshare" begin
            ctx = _mock_ctx(interline=INTERLINE_ONLINE)
            @test check_cnx_interline(_test_connection(status=online_status),    ctx) == PASS
            @test check_cnx_interline(_test_connection(status=codeshare_status), ctx) == FAIL_ONLINE
            @test check_cnx_interline(_test_connection(status=interline_status), ctx) == FAIL_ONLINE
        end

        @testset "INTERLINE_CODESHARE allows codeshare, rejects interline" begin
            ctx = _mock_ctx(interline=INTERLINE_CODESHARE)
            @test check_cnx_interline(_test_connection(status=online_status),    ctx) == PASS
            @test check_cnx_interline(_test_connection(status=codeshare_status), ctx) == PASS
            @test check_cnx_interline(_test_connection(status=interline_status), ctx) == FAIL_CODESHARE
        end

        @testset "INTERLINE_ALL allows international interline, rejects domestic interline" begin
            ctx = _mock_ctx(interline=INTERLINE_ALL)
            @test check_cnx_interline(_test_connection(status=online_status),    ctx) == PASS
            @test check_cnx_interline(_test_connection(status=codeshare_status), ctx) == PASS
            @test check_cnx_interline(_test_connection(status=interline_status), ctx) == FAIL_INTERLINE
            @test check_cnx_interline(_test_connection(status=intl_interline),   ctx) == PASS
        end
    end

    # ── Rule 4: MCTRule ───────────────────────────────────────────────────────

    @testset "MCTRule" begin
        ctx = _mock_ctx()
        lookup = MCTLookup()  # empty — falls back to global defaults (DD=60)

        @testset "through-flight always passes" begin
            cp = _test_connection(is_through=true)
            rule = MCTRule(lookup)
            @test rule(cp, ctx) == PASS
        end

        @testset "sufficient cnx_time passes (DD default=30 min)" begin
            # from_leg arrives at passenger_arrival_time=540 (09:00), to_leg departs passenger_departure_time=660 (11:00)
            # cnx_time = 660 - 540 = 120 min > 30 min MCT_DD default
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(300), passenger_arrival_time=Int16(540),
                                        arr_intl_dom='D')
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(660), passenger_arrival_time=Int16(960),
                                        dep_intl_dom='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            @test rule(cp, ctx) == PASS
            @test cp.cnx_time == Minutes(120)
            @test cp.mct == Minutes(30)  # global default DD
        end

        @testset "insufficient cnx_time fails with FAIL_TIME_MIN" begin
            # from_leg arrives at 540, to_leg departs at 560 => cnx_time=20 < MCT_DD=30
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(300), passenger_arrival_time=Int16(540),
                                        arr_intl_dom='D')
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(560), passenger_arrival_time=Int16(900),
                                        dep_intl_dom='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            @test rule(cp, ctx) == FAIL_TIME_MIN
        end

        @testset "cnx_time > max_mct_override fails with FAIL_TIME_MAX" begin
            # ParameterSet default max_mct_override=480; use a tight override
            constraints = SearchConstraints(
                defaults=ParameterSet(max_mct_override=Minutes(90))
            )
            ctx2 = _mock_ctx(constraints=constraints)
            # cnx_time = 900 - 540 = 360 min > 90 min max
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(300), passenger_arrival_time=Int16(540),
                                        arr_intl_dom='D')
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(900), passenger_arrival_time=Int16(1200),
                                        dep_intl_dom='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            @test rule(cp, ctx2) == FAIL_TIME_MAX
        end

        @testset "overnight wrap-around" begin
            # from_leg arrives at 1380 (23:00), to_leg departs at 60 (01:00 next day)
            # cnx_time = 60 - 1380 = -1320 => +1440 = 120 min
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(1200), passenger_arrival_time=Int16(1380),
                                        arrival_date_variation=Int8(0),
                                        arr_intl_dom='D')
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(60), passenger_arrival_time=Int16(480),
                                        dep_intl_dom='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            result = rule(cp, ctx)
            @test cp.cnx_time == Minutes(120)
            @test result == PASS
        end

        @testset "min_mct_override applied" begin
            # MCT_DD default = 30; override = 120; cnx_time = 90 => fails
            constraints = SearchConstraints(
                defaults=ParameterSet(min_mct_override=Minutes(120))
            )
            ctx2 = _mock_ctx(constraints=constraints)
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD",
                                        passenger_departure_time=Int16(300), passenger_arrival_time=Int16(540),
                                        arr_intl_dom='D')
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR",
                                        passenger_departure_time=Int16(630), passenger_arrival_time=Int16(900),
                                        dep_intl_dom='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            @test rule(cp, ctx2) == FAIL_TIME_MIN
            @test cp.mct == Minutes(120)
        end
    end

    # ── Rule 5: check_cnx_opdays ──────────────────────────────────────────────

    @testset "check_cnx_opdays" begin
        ctx = _mock_ctx()

        @testset "passes when at least one DOW bit set" begin
            cp = _test_connection(status=StatusBits(DOW_MON))
            @test check_cnx_opdays(cp, ctx) == PASS
        end

        @testset "fails when no DOW bits set" begin
            cp = _test_connection(status=StatusBits(0))
            @test check_cnx_opdays(cp, ctx) == FAIL_OPDAYS
        end

        @testset "passes with all DOW bits set" begin
            cp = _test_connection(status=DOW_MASK)
            @test check_cnx_opdays(cp, ctx) == PASS
        end

        @testset "DOW bits not affected by STATUS_ bits" begin
            # STATUS_INTERNATIONAL is bit 7; DOW_MASK covers bits 0-6
            # Setting only STATUS_INTERNATIONAL means no DOW bits => fail
            cp = _test_connection(status=STATUS_INTERNATIONAL)
            @test check_cnx_opdays(cp, ctx) == FAIL_OPDAYS
        end
    end

    # ── Rule 6: check_cnx_suppcodes ───────────────────────────────────────────

    @testset "check_cnx_suppcodes" begin
        ctx = _mock_ctx()

        @testset "passes when TRC is empty" begin
            cp = _test_connection()
            @test check_cnx_suppcodes(cp, ctx) == PASS
        end

        @testset "passes for informational codes (Z, J, P, R, S, U)" begin
            for code in ["Z", "J", "P", "R", "S", "U"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                cp = _test_connection(from_rec=from_rec)
                @test check_cnx_suppcodes(cp, ctx) == PASS
            end
        end

        @testset "passes for K and V (any connection allowed)" begin
            for code in ["K", "V"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                cp = _test_connection(from_rec=from_rec)
                @test check_cnx_suppcodes(cp, ctx) == PASS
            end
        end

        @testset "fails for unconditional block codes (A, H, I, B, M, T)" begin
            for code in ["A", "H", "I", "B", "M", "T"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                cp = _test_connection(from_rec=from_rec)
                @test check_cnx_suppcodes(cp, ctx) == FAIL_SUPPCODE
            end
        end

        @testset "fails when to_leg TRC has unconditional block code" begin
            to_rec = _test_leg_record(departure_station="ORD", arrival_station="LHR", traffic_restriction_for_leg="A", leg_sequence_number=UInt8(1))
            cp = _test_connection(to_rec=to_rec)
            @test check_cnx_suppcodes(cp, ctx) == FAIL_SUPPCODE
        end

        @testset "C (domestic only) — passes for domestic, fails for international" begin
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg="C", leg_sequence_number=UInt8(1))
            dom_status  = StatusBits(DOW_MON)
            intl_status = StatusBits(DOW_MON | STATUS_INTERNATIONAL)
            cp_dom  = _test_connection(from_rec=from_rec, status=dom_status)
            cp_intl = _test_connection(from_rec=from_rec, status=intl_status)
            @test check_cnx_suppcodes(cp_dom,  ctx) == PASS
            @test check_cnx_suppcodes(cp_intl, ctx) == FAIL_SUPPCODE
        end

        @testset "N and W (international only) — passes for international, fails for domestic" begin
            for code in ["N", "W"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                dom_status  = StatusBits(DOW_MON)
                intl_status = StatusBits(DOW_MON | STATUS_INTERNATIONAL)
                cp_dom  = _test_connection(from_rec=from_rec, status=dom_status)
                cp_intl = _test_connection(from_rec=from_rec, status=intl_status)
                @test check_cnx_suppcodes(cp_dom,  ctx) == FAIL_SUPPCODE
                @test check_cnx_suppcodes(cp_intl, ctx) == PASS
            end
        end

        @testset "F, Y, E, G, X (online only) — passes for online, fails for interline" begin
            for code in ["F", "Y", "E", "G", "X"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                online_status   = StatusBits(DOW_MON)
                interline_status = StatusBits(DOW_MON | STATUS_INTERLINE)
                cp_online    = _test_connection(from_rec=from_rec, status=online_status)
                cp_interline = _test_connection(from_rec=from_rec, status=interline_status)
                @test check_cnx_suppcodes(cp_online,    ctx) == PASS
                @test check_cnx_suppcodes(cp_interline, ctx) == FAIL_SUPPCODE
            end
        end

        @testset "D, O, Q (international online only) — fails if domestic or interline" begin
            for code in ["D", "O", "Q"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                # domestic online — fails (not international)
                cp_dom_online = _test_connection(from_rec=from_rec, status=StatusBits(DOW_MON))
                @test check_cnx_suppcodes(cp_dom_online, ctx) == FAIL_SUPPCODE
                # international interline — fails (interline)
                cp_intl_interline = _test_connection(from_rec=from_rec, status=StatusBits(DOW_MON | STATUS_INTERNATIONAL | STATUS_INTERLINE))
                @test check_cnx_suppcodes(cp_intl_interline, ctx) == FAIL_SUPPCODE
                # international online — passes
                cp_intl_online = _test_connection(from_rec=from_rec, status=StatusBits(DOW_MON | STATUS_INTERNATIONAL))
                @test check_cnx_suppcodes(cp_intl_online, ctx) == PASS
            end
        end

        @testset "passes when blocking code is at a different leg_seq position" begin
            # traffic_restriction_for_leg = "XA" but leg_sequence_number=1 => trc[1]='X' => no suppression
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg="XA", leg_sequence_number=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_suppcodes(cp, ctx) == PASS
        end
    end

    # ── Rule 7: MAFTRule ──────────────────────────────────────────────────────

    @testset "MAFTRule" begin
        ctx = _mock_ctx()

        @testset "default constructor" begin
            rule = MAFTRule()
            @test rule.speed == 400.0
            @test rule.rest_time == 240.0
        end

        @testset "round-trip always passes" begin
            cp = _test_connection(status=StatusBits(DOW_MON | STATUS_ROUNDTRIP))
            rule = MAFTRule()
            @test rule(cp, ctx) == PASS
        end

        @testset "normal flight passes (block_time << MAFT)" begin
            # dep=0, arr=150 per leg => block_time = 150 min each, total = 300 min
            # total_dist = 1000 + 1000 = 2000 NM
            # maft = max((2000/400)*60, 30) + 240 = max(300,30) + 240 = 540 min
            # 300 <= 540 => PASS
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD",
                                         passenger_departure_time=Int16(0),
                                         passenger_arrival_time=Int16(150),
                                         distance=1000.0f0)
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR",
                                         passenger_departure_time=Int16(300),
                                         passenger_arrival_time=Int16(450),
                                         distance=1000.0f0)
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MAFTRule()
            @test rule(cp, ctx) == PASS
        end
    end

    # ── Rule 8: CircuityRule ──────────────────────────────────────────────────

    @testset "CircuityRule" begin

        @testset "default constructor" begin
            rule = CircuityRule()
            @test rule.factor == 2.0
            @test rule.domestic_extra_miles == 500.0
            @test rule.international_extra_miles == 1000.0
        end

        @testset "round-trip always passes" begin
            ctx = _mock_ctx()
            cp = _test_connection(status=StatusBits(DOW_MON | STATUS_ROUNDTRIP))
            rule = CircuityRule()
            @test rule(cp, ctx) == PASS
        end

        @testset "passes when route is within circuity threshold" begin
            # JFK (40.63N, 73.78W) → ORD (41.97N, 87.91W) → LHR (51.47N, 0.45W)
            # GC dist JFK→LHR ≈ 3450 NM; legs ~1100 + 3550 = 4650 NM
            # 4650 <= 2 * 3450 + 500 = 7400 => PASS
            gc_cache = Dict{Tuple{StationCode,StationCode}, Float64}()
            ctx = _mock_ctx(gc_cache=gc_cache)
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", distance=1100.0f0)
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR", distance=3550.0f0)

            org_stn_rec = _test_station_record("JFK", "US", "NAM"; latitude=40.63, longitude=-73.78)
            cnx_stn_rec = _test_station_record("ORD", "US", "NAM"; latitude=41.97, longitude=-87.91)
            dst_stn_rec = _test_station_record("LHR", "GB", "EUR"; latitude=51.47, longitude=-0.45)

            org_stn  = GraphStation(org_stn_rec)
            cnx_stn  = GraphStation(cnx_stn_rec)
            dst_stn  = GraphStation(dst_stn_rec)
            from_leg = GraphLeg(from_rec, org_stn, cnx_stn)
            to_leg   = GraphLeg(to_rec,   cnx_stn, dst_stn)
            cp = GraphConnection(
                from_leg=from_leg,
                to_leg=to_leg,
                station=cnx_stn,
                status=StatusBits(DOW_MON),
            )
            rule = CircuityRule()
            @test rule(cp, ctx) == PASS
        end

        @testset "fails when route is too circuitous" begin
            # Use a very tight factor so even moderate detour fails
            # Set up: org→cnx→dst where route_dist >> factor * gc_dist
            # Use very short gc distance (nearby airports) but long leg distances
            gc_cache = Dict{Tuple{StationCode,StationCode}, Float64}()
            ctx = _mock_ctx(gc_cache=gc_cache)

            # LAX→SFO→NYC: SFO is very close to LAX but NYC is the actual destination
            # LAX(33.9N,118.4W) → SFO(37.6N,122.4W) → NYC(40.6N,73.8W)
            # GC LAX→NYC ≈ 2440 NM; legs 340 + 2570 = 2910 NM
            # With factor=1.0, extra=0: 2910 > 1*2440+0 => FAIL
            from_rec = _test_leg_record(departure_station="LAX", arrival_station="SFO", distance=340.0f0)
            to_rec   = _test_leg_record(departure_station="SFO", arrival_station="NYC", distance=2570.0f0)

            org_stn_rec = _test_station_record("LAX", "US", "NAM"; latitude=33.94, longitude=-118.40)
            cnx_stn_rec = _test_station_record("SFO", "US", "NAM"; latitude=37.62, longitude=-122.38)
            dst_stn_rec = _test_station_record("NYC", "US", "NAM"; latitude=40.63, longitude=-73.78)

            org_stn  = GraphStation(org_stn_rec)
            cnx_stn  = GraphStation(cnx_stn_rec)
            dst_stn  = GraphStation(dst_stn_rec)
            from_leg = GraphLeg(from_rec, org_stn, cnx_stn)
            to_leg   = GraphLeg(to_rec,   cnx_stn, dst_stn)
            cp = GraphConnection(
                from_leg=from_leg,
                to_leg=to_leg,
                station=cnx_stn,
                status=StatusBits(DOW_MON),
            )
            rule = CircuityRule(1.0, 0.0, 0.0)   # very tight rule
            @test rule(cp, ctx) == FAIL_CIRCUITY
        end

        @testset "gc_cache is populated on first call" begin
            gc_cache = Dict{Tuple{StationCode,StationCode}, Float64}()
            ctx = _mock_ctx(gc_cache=gc_cache)

            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", distance=1100.0f0)
            to_rec   = _test_leg_record(departure_station="ORD", arrival_station="LHR", distance=3550.0f0)

            org_stn  = GraphStation(_test_station_record("JFK", "US", "NAM"; latitude=40.63, longitude=-73.78))
            cnx_stn  = GraphStation(_test_station_record("ORD", "US", "NAM"; latitude=41.97, longitude=-87.91))
            dst_stn  = GraphStation(_test_station_record("LHR", "GB", "EUR"; latitude=51.47, longitude=-0.45))
            from_leg = GraphLeg(from_rec, org_stn, cnx_stn)
            to_leg   = GraphLeg(to_rec,   cnx_stn, dst_stn)
            cp = GraphConnection(
                from_leg=from_leg,
                to_leg=to_leg,
                station=cnx_stn,
                status=StatusBits(DOW_MON),
            )
            rule = CircuityRule()
            rule(cp, ctx)
            @test !isempty(gc_cache)
            gc_key = (StationCode("JFK"), StationCode("LHR"))
            @test haskey(gc_cache, gc_key)
        end
    end

    # ── Rule 9: check_cnx_trfrest ─────────────────────────────────────────────

    @testset "check_cnx_trfrest" begin
        ctx = _mock_ctx()

        @testset "passes when TRC is empty" begin
            cp = _test_connection()
            @test check_cnx_trfrest(cp, ctx) == PASS
        end

        @testset "passes for non-'A' TRC codes (B, C, D, E, F handled by suppcodes)" begin
            for code in ["B", "C", "D", "E", "F"]
                from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg=code, leg_sequence_number=UInt8(1))
                cp = _test_connection(from_rec=from_rec)
                @test check_cnx_trfrest(cp, ctx) == PASS
            end
        end

        @testset "fails for TRC code 'A' on from_leg" begin
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg="A", leg_sequence_number=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_trfrest(cp, ctx) == FAIL_TRFREST
        end

        @testset "fails for TRC code 'A' on to_leg" begin
            to_rec = _test_leg_record(departure_station="ORD", arrival_station="LHR", traffic_restriction_for_leg="A", leg_sequence_number=UInt8(1))
            cp = _test_connection(to_rec=to_rec)
            @test check_cnx_trfrest(cp, ctx) == FAIL_TRFREST
        end

        @testset "passes when 'A' is at a different leg_seq position" begin
            # traffic_restriction_for_leg="XA" but leg_sequence_number=1 => trc[1]='X' => no block
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", traffic_restriction_for_leg="XA", leg_sequence_number=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_trfrest(cp, ctx) == PASS
        end
    end

    # ── _chars_to_mct_status ──────────────────────────────────────────────────

    @testset "_chars_to_mct_status" begin
        using ItinerarySearch: _chars_to_mct_status
        @test _chars_to_mct_status('D', 'D') == MCT_DD
        @test _chars_to_mct_status('D', 'I') == MCT_DI
        @test _chars_to_mct_status('I', 'D') == MCT_ID
        @test _chars_to_mct_status('I', 'I') == MCT_II
        # Unknown chars map to MCT_II (both non-domestic)
        @test _chars_to_mct_status('X', 'X') == MCT_II
    end

    # ── _haversine_distance ───────────────────────────────────────────────────

    @testset "_haversine_distance" begin
        using ItinerarySearch: _haversine_distance
        # JFK (40.63N, 73.78W) to LHR (51.47N, 0.45W)
        # Expected ≈ 3451 statute miles; allow 5% tolerance
        dist = _haversine_distance(40.63, -73.78, 51.47, -0.45)
        @test 3200.0 < dist < 3700.0
        # Same point => zero distance
        @test _haversine_distance(0.0, 0.0, 0.0, 0.0) ≈ 0.0 atol=1e-6
    end

    # ── Geodesic Distance ─────────────────────────────────────────────────────

    @testset "Geodesic Distance" begin
        using ItinerarySearch: _haversine_distance, _vincenty_distance, _geodesic_distance

        # ORD (41.97N, 87.90W) → LHR (51.47N, 0.46W): expect 3900–4100 statute miles
        ord_lat, ord_lng = 41.97, -87.90
        lhr_lat, lhr_lng = 51.47, -0.46

        @testset "haversine ORD→LHR in expected range" begin
            d = _haversine_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test 3900.0 < d < 4100.0
        end

        @testset "vincenty ORD→LHR in expected range" begin
            d = _vincenty_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test 3900.0 < d < 4100.0
        end

        @testset "haversine and vincenty agree within 0.5% for ORD→LHR" begin
            dh = _haversine_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            dv = _vincenty_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test abs(dh - dv) / dv < 0.005
        end

        @testset "near-antipodal SYD→SCL > 5000 statute miles (both formulas)" begin
            # SYD (-33.87, 151.21) → SCL (-33.45, -70.67)
            syd_lat, syd_lng = -33.87, 151.21
            scl_lat, scl_lng = -33.45, -70.67
            @test _haversine_distance(syd_lat, syd_lng, scl_lat, scl_lng) > 5000.0
            @test _vincenty_distance(syd_lat, syd_lng, scl_lat, scl_lng) > 5000.0
        end

        @testset "same point returns 0.0" begin
            @test _vincenty_distance(0.0, 0.0, 0.0, 0.0) ≈ 0.0 atol=1e-6
            @test _vincenty_distance(40.0, -74.0, 40.0, -74.0) ≈ 0.0 atol=1e-6
        end

        @testset "_geodesic_distance dispatches on :haversine" begin
            cfg = SearchConfig(distance_formula=:haversine)
            d = _geodesic_distance(cfg, ord_lat, ord_lng, lhr_lat, lhr_lng)
            expected = _haversine_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test d ≈ expected
        end

        @testset "_geodesic_distance dispatches on :vincenty" begin
            cfg = SearchConfig(distance_formula=:vincenty)
            d = _geodesic_distance(cfg, ord_lat, ord_lng, lhr_lat, lhr_lng)
            expected = _vincenty_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test d ≈ expected
        end
    end

    # ── build_cnx_rules ───────────────────────────────────────────────────────

    @testset "build_cnx_rules" begin
        config      = SearchConfig()
        constraints = SearchConstraints()
        lookup      = MCTLookup()
        rules = build_cnx_rules(config, constraints, lookup)

        @test length(rules) == 10
        @test rules[1] === check_cnx_roundtrip
        @test rules[2] === check_cnx_backtrack
        @test rules[3] === check_cnx_scope
        @test rules[4] === check_cnx_interline
        @test rules[5] isa MCTRule
        @test rules[6] === check_cnx_opdays
        @test rules[7] === check_cnx_suppcodes
        @test rules[8] isa MAFTRule
        @test rules[9] isa CircuityRule
        @test rules[10] === check_cnx_trfrest
        # All elements are callable
        @test all(r -> applicable(r, GraphConnection(), (config=config, constraints=constraints, gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(), mct_cache=Dict{UInt64,MCTResult}(), build_stats=BuildStats(), mct_selections=MCTSelectionRow[])), rules)

        # Verify CircuityRule picks up defaults from constraints
        rule9 = rules[9]::CircuityRule
        @test rule9.factor == constraints.defaults.circuity_factor
        @test rule9.domestic_extra_miles == constraints.defaults.domestic_circuity_extra_miles
        @test rule9.international_extra_miles == constraints.defaults.international_circuity_extra_miles
    end

    @testset "build_cnx_rules with maft_enabled=false omits MAFTRule" begin
        config      = SearchConfig(maft_enabled=false)
        constraints = SearchConstraints()
        lookup      = MCTLookup()
        rules = build_cnx_rules(config, constraints, lookup)
        @test length(rules) == 9
        @test !any(r -> r isa MAFTRule, rules)
        @test rules[8] isa CircuityRule
        @test rules[9] === check_cnx_trfrest
    end

    # ── Allocation regression test ────────────────────────────────────────────

    @testset "MCTRule zero-allocation hot path" begin
        lookup = MCTLookup()
        rule = MCTRule(lookup)
        cp = _test_connection(
            from_rec = _test_leg_record(departure_station="JFK", arrival_station="ORD", passenger_departure_time=Int16(420), passenger_arrival_time=Int16(720)),
            to_rec = _test_leg_record(departure_station="ORD", arrival_station="LHR", passenger_departure_time=Int16(900), passenger_arrival_time=Int16(1380)),
        )
        ctx = _mock_ctx()

        # Warmup
        rule(cp, ctx)

        allocs = @allocated rule(cp, ctx)
        @test allocs == 0
    end

end

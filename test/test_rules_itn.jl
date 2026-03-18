using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Itinerary Rules" begin

    # ── Test helpers ──────────────────────────────────────────────────────────

    function _itn_station_record(code, country, region; lat=0.0, lng=0.0)
        StationRecord(
            code=StationCode(code),
            country=InlineString3(country),
            state=InlineString3(""),
            city=InlineString31(""),
            region=InlineString3(region),
            lat=lat,
            lng=lng,
            utc_offset=Int16(0),
        )
    end

    function _itn_leg_record(;
        airline="UA",
        flt_no=100,
        org="ORD",
        dst="LHR",
        pax_dep=Int16(540),
        pax_arr=Int16(1320),
        leg_seq=UInt8(1),
        trc="",
        distance=1000.0f0,
        frequency=0x7f,
    )
        LegRecord(
            airline=AirlineCode(airline),
            flt_no=Int16(flt_no),
            operational_suffix=' ',
            itin_var=UInt8(1),
            itin_var_overflow=' ',
            leg_seq=leg_seq,
            svc_type='J',
            org=StationCode(org),
            dst=StationCode(dst),
            pax_dep=Int16(pax_dep),
            pax_arr=Int16(pax_arr),
            ac_dep=Int16(pax_dep),
            ac_arr=Int16(pax_arr),
            dep_utc_offset=Int16(0),
            arr_utc_offset=Int16(0),
            dep_date_var=Int8(0),
            arr_date_var=Int8(0),
            eqp=InlineString7("738"),
            body_type='N',
            dep_term=InlineString3("1"),
            arr_term=InlineString3("1"),
            aircraft_owner=AirlineCode(airline),
            operating_date=UInt32(20260101),
            day_of_week=UInt8(1),
            eff_date=UInt32(20260101),
            disc_date=UInt32(20261231),
            frequency=UInt8(frequency),
            mct_status_dep='D',
            mct_status_arr='D',
            trc=InlineString15(trc),
            trc_overflow=' ',
            record_serial=UInt32(1),
            row_number=UInt64(1),
            segment_hash=UInt64(0),
            distance=Distance(distance),
            codeshare_airline=AirlineCode(""),
            codeshare_flt_no=Int16(0),
            dei_10=InlineString31(""),
            wet_lease=false,
            dei_127=InlineString31(""),
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
        from_rec=_itn_leg_record(org="JFK", dst="ORD", distance=1000.0f0),
        to_rec=_itn_leg_record(org="ORD", dst="LHR", distance=3000.0f0),
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
            # ParameterSet defaults: itinerary_circuity=2.5, circuity_extra_miles=500
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

        @testset "uses constraints.defaults.itinerary_circuity" begin
            # Use a tight factor of 1.0 with no extra miles
            tight = SearchConstraints(defaults=ParameterSet(itinerary_circuity=1.0, circuity_extra_miles=0.0))
            ctx_tight = _mock_ctx(constraints=tight)
            # total=2000, market=1000 => 2000 > 1.0 * 1000 + 0 => FAIL
            itn = _nonstop_itn(total_distance=Distance(2000.0f0), market_distance=Distance(1000.0f0))
            @test check_itn_circuity(itn, ctx_tight) == FAIL_ITN_CIRCUITY
        end
    end

    # ── Rule 4: check_itn_suppcodes ───────────────────────────────────────────

    @testset "check_itn_suppcodes" begin
        ctx = _mock_ctx()

        @testset "passes when TRC is empty" begin
            itn = _nonstop_itn()
            @test check_itn_suppcodes(itn, ctx) == PASS
        end

        @testset "passes when TRC has no 'I' at leg_seq" begin
            # leg_seq=1, trc[1]='A' — 'A' is suppressed for connections but not 'I'
            leg_rec = _itn_leg_record(trc="A", leg_seq=UInt8(1))
            itn = _nonstop_itn(leg_rec=leg_rec)
            @test check_itn_suppcodes(itn, ctx) == PASS
        end

        @testset "fails when from_leg TRC has 'I' at leg_seq" begin
            leg_rec = _itn_leg_record(trc="I", leg_seq=UInt8(1))
            itn = _nonstop_itn(leg_rec=leg_rec)
            @test check_itn_suppcodes(itn, ctx) == FAIL_ITN_SUPPCODE
        end

        @testset "fails when a non-first leg has 'I' at its leg_seq" begin
            clean_rec = _itn_leg_record(org="JFK", dst="ORD", trc="",  leg_seq=UInt8(1))
            supp_rec  = _itn_leg_record(org="ORD", dst="LHR", trc="XI", leg_seq=UInt8(2))
            itn = _oneStop_itn(from_rec=clean_rec, to_rec=supp_rec)
            @test check_itn_suppcodes(itn, ctx) == FAIL_ITN_SUPPCODE
        end

        @testset "passes when 'I' is at a different leg_seq position" begin
            # trc="XI" but leg_seq=1 => trc[1]='X' => no suppression
            leg_rec = _itn_leg_record(trc="XI", leg_seq=UInt8(1))
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

        @testset "nonstop passes (block time << MAFT)" begin
            # gc_dist=1000, maft = max((1000/400)*60, 30) + 240 + 0*120
            #               = max(150, 30) + 240 = 390 min
            # block_time = (1000/400)*60 = 150 min => 150 <= 390 => PASS
            leg_rec = _itn_leg_record(distance=1000.0f0)
            itn = _nonstop_itn(
                market_distance=Distance(1000.0f0),
                leg_rec=leg_rec,
            )
            @test check_itn_maft(itn, ctx) == PASS
        end

        @testset "fails when block time exceeds MAFT" begin
            # gc_dist=100, maft = max((100/400)*60, 30) + 240 + 0*120
            #              = max(15, 30) + 240 = 270 min
            # leg distance=10000 => block_time = (10000/400)*60 = 1500 min
            # 1500 > 270 => FAIL
            leg_rec = _itn_leg_record(distance=10000.0f0)
            itn = _nonstop_itn(
                market_distance=Distance(100.0f0),
                leg_rec=leg_rec,
            )
            @test check_itn_maft(itn, ctx) == FAIL_ITN_MAFT
        end

        @testset "1-stop itinerary: stop allowance relaxes MAFT" begin
            # gc_dist=1500, num_stops=1
            # maft = max((1500/400)*60, 30) + 240 + 1*120
            #       = max(225, 30) + 240 + 120 = 585 min
            # legs: distance=1000 + 1000 => total_bt = 2*(1000/400)*60 = 300 min
            # 300 <= 585 => PASS
            from_rec = _itn_leg_record(org="JFK", dst="ORD", distance=1000.0f0)
            to_rec   = _itn_leg_record(org="ORD", dst="LHR", distance=1000.0f0)
            itn = _oneStop_itn(
                market_distance=Distance(1500.0f0),
                total_distance=Distance(2000.0f0),
                num_stops=Int16(1),
                from_rec=from_rec,
                to_rec=to_rec,
            )
            @test check_itn_maft(itn, ctx) == PASS
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

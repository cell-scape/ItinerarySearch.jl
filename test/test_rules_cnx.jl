using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Connection Rules" begin

    # ── Test helpers ──────────────────────────────────────────────────────────

    function _test_station_record(code, country, region; lat=0.0, lng=0.0)
        StationRecord(
            code=StationCode(code),
            country=InlineString3(country),
            state=InlineString3(""),
            metro_area=InlineString3(""),
            region=InlineString3(region),
            lat=lat,
            lng=lng,
            utc_offset=Int16(0),
        )
    end

    function _test_leg_record(;
        airline="UA",
        flt_no=100,
        org="ORD",
        dst="LHR",
        pax_dep=Int16(840),   # 14:00
        pax_arr=Int16(540),   # 09:00
        arr_date_var=Int8(0),
        mct_status_dep='D',
        mct_status_arr='D',
        trc="",
        leg_seq=UInt8(1),
        distance=1000.0f0,
        frequency=0x7f,       # all days
        codeshare_airline="",
        body_type='N',
        dep_term="1",
        arr_term="1",
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
            arr_date_var=arr_date_var,
            eqp=InlineString7("738"),
            body_type=body_type,
            dep_term=InlineString3(dep_term),
            arr_term=InlineString3(arr_term),
            aircraft_owner=AirlineCode(airline),
            operating_date=UInt32(20260101),
            day_of_week=UInt8(1),
            eff_date=UInt32(20260101),
            disc_date=UInt32(20261231),
            frequency=UInt8(frequency),
            mct_status_dep=mct_status_dep,
            mct_status_arr=mct_status_arr,
            trc=InlineString15(trc),
            trc_overflow=' ',
            record_serial=UInt32(1),
            row_number=UInt64(1),
            segment_hash=UInt64(0),
            distance=Distance(distance),
            codeshare_airline=AirlineCode(codeshare_airline),
            codeshare_flt_no=Int16(0),
            dei_10="",
            wet_lease=false,
            dei_127="",
            prbd=InlineString31(""),
        )
    end

    # Build a GraphConnection from two leg records and a connect station.
    # status is a caller-supplied StatusBits value (e.g. for intl/interline flags).
    function _test_connection(;
        from_rec=_test_leg_record(org="JFK", dst="ORD", pax_dep=Int16(420), pax_arr=Int16(720)),
        to_rec=_test_leg_record(org="ORD", dst="LHR", pax_dep=Int16(900), pax_arr=Int16(1380)),
        cnx_station_code="ORD",
        cnx_station_country="US",
        cnx_station_region="NAM",
        status=StatusBits(DOW_MON | DOW_TUE | DOW_WED | DOW_THU | DOW_FRI),
        is_through=false,
    )
        org_stn  = GraphStation(_test_station_record(from_rec.org, "US", "NAM"))
        cnx_stn  = GraphStation(_test_station_record(cnx_station_code, cnx_station_country, cnx_station_region))
        dst_stn  = GraphStation(_test_station_record(to_rec.dst, "GB", "EUR"))
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
        gc_cache=Dict{UInt64, Float64}(),
    )
        (
            config = SearchConfig(scope=scope, interline=interline),
            constraints = constraints,
            build_stats = BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache = Dict{UInt64, MCTResult}(),
            gc_cache = gc_cache,
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
            from_rec = _test_leg_record(org="JFK", dst="ORD")
            to_rec   = _test_leg_record(org="ORD", dst="JFK")
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
            from_rec = _test_leg_record(org="LAX", dst="SFO")
            to_rec   = _test_leg_record(org="SFO", dst="LAX")
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

        @testset "sufficient cnx_time passes (DD default=60 min)" begin
            # from_leg arrives at pax_arr=540 (09:00), to_leg departs pax_dep=660 (11:00)
            # cnx_time = 660 - 540 = 120 min > 60 min MCT_DD default
            from_rec = _test_leg_record(org="JFK", dst="ORD",
                                        pax_dep=Int16(300), pax_arr=Int16(540),
                                        mct_status_arr='D')
            to_rec   = _test_leg_record(org="ORD", dst="LHR",
                                        pax_dep=Int16(660), pax_arr=Int16(960),
                                        mct_status_dep='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            @test rule(cp, ctx) == PASS
            @test cp.cnx_time == Minutes(120)
            @test cp.mct == Minutes(60)  # global default DD
        end

        @testset "insufficient cnx_time fails with FAIL_TIME_MIN" begin
            # from_leg arrives at 540, to_leg departs at 570 => cnx_time=30 < MCT_DD=60
            from_rec = _test_leg_record(org="JFK", dst="ORD",
                                        pax_dep=Int16(300), pax_arr=Int16(540),
                                        mct_status_arr='D')
            to_rec   = _test_leg_record(org="ORD", dst="LHR",
                                        pax_dep=Int16(570), pax_arr=Int16(900),
                                        mct_status_dep='D')
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
            from_rec = _test_leg_record(org="JFK", dst="ORD",
                                        pax_dep=Int16(300), pax_arr=Int16(540),
                                        mct_status_arr='D')
            to_rec   = _test_leg_record(org="ORD", dst="LHR",
                                        pax_dep=Int16(900), pax_arr=Int16(1200),
                                        mct_status_dep='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            @test rule(cp, ctx2) == FAIL_TIME_MAX
        end

        @testset "overnight wrap-around" begin
            # from_leg arrives at 1380 (23:00), to_leg departs at 60 (01:00 next day)
            # cnx_time = 60 - 1380 = -1320 => +1440 = 120 min
            from_rec = _test_leg_record(org="JFK", dst="ORD",
                                        pax_dep=Int16(1200), pax_arr=Int16(1380),
                                        arr_date_var=Int8(0),
                                        mct_status_arr='D')
            to_rec   = _test_leg_record(org="ORD", dst="LHR",
                                        pax_dep=Int16(60), pax_arr=Int16(480),
                                        mct_status_dep='D')
            cp = _test_connection(from_rec=from_rec, to_rec=to_rec,
                                  status=StatusBits(DOW_MON))
            rule = MCTRule(lookup)
            result = rule(cp, ctx)
            @test cp.cnx_time == Minutes(120)
            @test result == PASS
        end

        @testset "min_mct_override applied" begin
            # MCT_DD default = 60; override = 120; cnx_time = 90 => fails
            constraints = SearchConstraints(
                defaults=ParameterSet(min_mct_override=Minutes(120))
            )
            ctx2 = _mock_ctx(constraints=constraints)
            from_rec = _test_leg_record(org="JFK", dst="ORD",
                                        pax_dep=Int16(300), pax_arr=Int16(540),
                                        mct_status_arr='D')
            to_rec   = _test_leg_record(org="ORD", dst="LHR",
                                        pax_dep=Int16(630), pax_arr=Int16(900),
                                        mct_status_dep='D')
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

        @testset "passes when TRC has no 'A' at leg_seq" begin
            # leg_seq=1, trc[1]='B' — 'B' is a traffic restriction but not 'A'
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="B", leg_seq=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_suppcodes(cp, ctx) == PASS
        end

        @testset "fails when from_leg TRC has 'A' at leg_seq" begin
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="A", leg_seq=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_suppcodes(cp, ctx) == FAIL_SUPPCODE
        end

        @testset "fails when to_leg TRC has 'A' at leg_seq" begin
            to_rec = _test_leg_record(org="ORD", dst="LHR", trc="A", leg_seq=UInt8(1))
            cp = _test_connection(to_rec=to_rec)
            @test check_cnx_suppcodes(cp, ctx) == FAIL_SUPPCODE
        end

        @testset "passes when 'A' is at different leg_seq position" begin
            # trc = "XA" but leg_seq=1 => trc[1]='X' => no suppression
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="XA", leg_seq=UInt8(1))
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
            # distance=1000 NM each leg => total=2000 NM
            # block_time = (2000/400)*60 = 300 min
            # maft = max(300, 30) + 240 = 540 min
            # 300 <= 540 => PASS
            from_rec = _test_leg_record(org="JFK", dst="ORD", distance=1000.0f0)
            to_rec   = _test_leg_record(org="ORD", dst="LHR", distance=1000.0f0)
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
            @test rule.extra_miles == 500.0
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
            gc_cache = Dict{UInt64, Float64}()
            ctx = _mock_ctx(gc_cache=gc_cache)
            from_rec = _test_leg_record(org="JFK", dst="ORD", distance=1100.0f0)
            to_rec   = _test_leg_record(org="ORD", dst="LHR", distance=3550.0f0)

            org_stn_rec = _test_station_record("JFK", "US", "NAM"; lat=40.63, lng=-73.78)
            cnx_stn_rec = _test_station_record("ORD", "US", "NAM"; lat=41.97, lng=-87.91)
            dst_stn_rec = _test_station_record("LHR", "GB", "EUR"; lat=51.47, lng=-0.45)

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
            gc_cache = Dict{UInt64, Float64}()
            ctx = _mock_ctx(gc_cache=gc_cache)

            # LAX→SFO→NYC: SFO is very close to LAX but NYC is the actual destination
            # LAX(33.9N,118.4W) → SFO(37.6N,122.4W) → NYC(40.6N,73.8W)
            # GC LAX→NYC ≈ 2440 NM; legs 340 + 2570 = 2910 NM
            # With factor=1.0, extra=0: 2910 > 1*2440+0 => FAIL
            from_rec = _test_leg_record(org="LAX", dst="SFO", distance=340.0f0)
            to_rec   = _test_leg_record(org="SFO", dst="NYC", distance=2570.0f0)

            org_stn_rec = _test_station_record("LAX", "US", "NAM"; lat=33.94, lng=-118.40)
            cnx_stn_rec = _test_station_record("SFO", "US", "NAM"; lat=37.62, lng=-122.38)
            dst_stn_rec = _test_station_record("NYC", "US", "NAM"; lat=40.63, lng=-73.78)

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
            rule = CircuityRule(1.0, 0.0)   # very tight rule
            @test rule(cp, ctx) == FAIL_CIRCUITY
        end

        @testset "gc_cache is populated on first call" begin
            gc_cache = Dict{UInt64, Float64}()
            ctx = _mock_ctx(gc_cache=gc_cache)

            from_rec = _test_leg_record(org="JFK", dst="ORD", distance=1100.0f0)
            to_rec   = _test_leg_record(org="ORD", dst="LHR", distance=3550.0f0)

            org_stn  = GraphStation(_test_station_record("JFK", "US", "NAM"; lat=40.63, lng=-73.78))
            cnx_stn  = GraphStation(_test_station_record("ORD", "US", "NAM"; lat=41.97, lng=-87.91))
            dst_stn  = GraphStation(_test_station_record("LHR", "GB", "EUR"; lat=51.47, lng=-0.45))
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
            gc_key = hash(StationCode("JFK"), hash(StationCode("LHR")))
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

        @testset "passes for non-blocked TRC code" begin
            # 'E' is not in the blocked set
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="E", leg_seq=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_trfrest(cp, ctx) == PASS
        end

        @testset "fails for blocked TRC code 'A' on from_leg" begin
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="A", leg_seq=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_trfrest(cp, ctx) == FAIL_TRFREST
        end

        @testset "fails for blocked TRC code 'B' on from_leg" begin
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="B", leg_seq=UInt8(1))
            cp = _test_connection(from_rec=from_rec)
            @test check_cnx_trfrest(cp, ctx) == FAIL_TRFREST
        end

        @testset "fails for blocked TRC code 'C' on to_leg" begin
            to_rec = _test_leg_record(org="ORD", dst="LHR", trc="C", leg_seq=UInt8(1))
            cp = _test_connection(to_rec=to_rec)
            @test check_cnx_trfrest(cp, ctx) == FAIL_TRFREST
        end

        @testset "fails for blocked TRC code 'D' on to_leg" begin
            to_rec = _test_leg_record(org="ORD", dst="LHR", trc="D", leg_seq=UInt8(1))
            cp = _test_connection(to_rec=to_rec)
            @test check_cnx_trfrest(cp, ctx) == FAIL_TRFREST
        end

        @testset "passes when blocked code is at a different leg_seq position" begin
            # trc="XA" but leg_seq=1 => trc[1]='X' => no block
            from_rec = _test_leg_record(org="JFK", dst="ORD", trc="XA", leg_seq=UInt8(1))
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
        # Expected ≈ 2990 NM; allow 5% tolerance
        dist = _haversine_distance(40.63, -73.78, 51.47, -0.45)
        @test 2800.0 < dist < 3200.0
        # Same point => zero distance
        @test _haversine_distance(0.0, 0.0, 0.0, 0.0) ≈ 0.0 atol=1e-6
    end

    # ── Geodesic Distance ─────────────────────────────────────────────────────

    @testset "Geodesic Distance" begin
        using ItinerarySearch: _haversine_distance, _vincenty_distance, _geodesic_distance

        # ORD (41.97N, 87.90W) → LHR (51.47N, 0.46W): expect 3400–3500 NM
        ord_lat, ord_lng = 41.97, -87.90
        lhr_lat, lhr_lng = 51.47, -0.46

        @testset "haversine ORD→LHR in expected range" begin
            d = _haversine_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test 3400.0 < d < 3500.0
        end

        @testset "vincenty ORD→LHR in expected range" begin
            d = _vincenty_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test 3400.0 < d < 3500.0
        end

        @testset "haversine and vincenty agree within 0.5% for ORD→LHR" begin
            dh = _haversine_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            dv = _vincenty_distance(ord_lat, ord_lng, lhr_lat, lhr_lng)
            @test abs(dh - dv) / dv < 0.005
        end

        @testset "near-antipodal SYD→SCL > 5000 NM (both formulas)" begin
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

        @test length(rules) == 9
        @test rules[1] === check_cnx_roundtrip
        @test rules[2] === check_cnx_scope
        @test rules[3] === check_cnx_interline
        @test rules[4] isa MCTRule
        @test rules[5] === check_cnx_opdays
        @test rules[6] === check_cnx_suppcodes
        @test rules[7] isa MAFTRule
        @test rules[8] isa CircuityRule
        @test rules[9] === check_cnx_trfrest
        # All elements are callable
        @test all(r -> applicable(r, GraphConnection(), (config=config, constraints=constraints, gc_cache=Dict{UInt64,Float64}(), mct_cache=Dict{UInt64,MCTResult}(), build_stats=BuildStats())), rules)

        # Verify CircuityRule picks up defaults from constraints
        rule8 = rules[8]::CircuityRule
        @test rule8.factor == constraints.defaults.circuity_factor
        @test rule8.extra_miles == constraints.defaults.circuity_extra_miles
    end

end

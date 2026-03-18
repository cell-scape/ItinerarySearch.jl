using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Connection Building" begin

    # ── Test helpers ──────────────────────────────────────────────────────────

    function _make_station_record(code, country, region; lat=0.0, lng=0.0)
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

    function _make_leg_record(;
        airline="UA",
        flt_no=100,
        org="ORD",
        dst="LHR",
        pax_dep=Int16(840),    # 14:00
        pax_arr=Int16(540),    # 09:00
        arr_date_var=Int8(0),
        mct_status_dep='D',
        mct_status_arr='D',
        trc="",
        leg_seq=UInt8(1),
        distance=1000.0f0,
        frequency=UInt8(0x7f),   # all days
        codeshare_airline="",
        body_type='N',
        dep_term="1",
        arr_term="1",
        eff_date=UInt32(20260101),
        disc_date=UInt32(20261231),
        segment_hash=UInt64(0),
        wet_lease=false,
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
            pax_dep=pax_dep,
            pax_arr=pax_arr,
            ac_dep=pax_dep,
            ac_arr=pax_arr,
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
            eff_date=eff_date,
            disc_date=disc_date,
            frequency=frequency,
            mct_status_dep=mct_status_dep,
            mct_status_arr=mct_status_arr,
            trc=InlineString15(trc),
            trc_overflow=' ',
            record_serial=UInt32(1),
            row_number=UInt64(1),
            segment_hash=segment_hash,
            distance=Distance(distance),
            codeshare_airline=AirlineCode(codeshare_airline),
            codeshare_flt_no=Int16(0),
            dei_10=InlineString31(""),
            wet_lease=wet_lease,
            dei_127=InlineString31(""),
            prbd=InlineString31(""),
        )
    end

    # Standard mock context: empty MCT lookup falls back to global defaults
    # (DD = 60 min), so any cnx_time >= 60 passes MCTRule.
    # Uses a generous circuity_extra_miles so tests with default-coord stations
    # (lat=0, lng=0) don't fail on CircuityRule.
    function _mock_ctx(;
        scope=SCOPE_ALL,
        interline=INTERLINE_CODESHARE,
        constraints=SearchConstraints(
            defaults=ParameterSet(circuity_extra_miles=50_000.0)  # generous: never circuity-fail
        ),
    )
        (
            config = SearchConfig(scope=scope, interline=interline),
            constraints = constraints,
            build_stats = BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache = Dict{UInt64, MCTResult}(),
            gc_cache = Dict{UInt64, Float64}(),
        )
    end

    # Build rules using empty MCT lookup (global defaults: DD=60, DI=90, etc.)
    # and the same generous constraints.
    _make_rules(; constraints=SearchConstraints(defaults=ParameterSet(circuity_extra_miles=50_000.0))) =
        build_cnx_rules(SearchConfig(), constraints, MCTLookup())

    # ── Nonstop self-connections ───────────────────────────────────────────────

    @testset "Nonstop self-connections created" begin
        # Station with 2 departures and 0 arrivals → 2 nonstops, no connections
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")
        lhr_rec = _make_station_record("LHR", "GB", "EUR")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)
        lhr_stn = GraphStation(lhr_rec)

        dep_hub = GraphStation(ord_rec)   # hub: departs to JFK and LHR

        r1 = _make_leg_record(org="ORD", dst="JFK", pax_dep=Int16(480), pax_arr=Int16(720))
        r2 = _make_leg_record(org="ORD", dst="LHR", pax_dep=Int16(900), pax_arr=Int16(1380))

        leg1 = GraphLeg(r1, dep_hub, jfk_stn)
        leg2 = GraphLeg(r2, dep_hub, lhr_stn)

        push!(dep_hub.departures, leg1)
        push!(dep_hub.departures, leg2)
        # No arrivals

        ctx   = _mock_ctx()
        rules = _make_rules()

        build_connections_at_station!(dep_hub, rules, ctx)

        @test dep_hub.stats.num_departures == Int32(2)
        @test dep_hub.stats.num_arrivals   == Int32(0)
        @test dep_hub.stats.num_nonstops   == Int32(2)
        @test dep_hub.stats.num_connections == Int32(0)
        @test dep_hub.stats.num_pairs_evaluated == Int32(0)

        # station.connections has exactly the 2 nonstop entries
        @test length(dep_hub.connections) == 2
        ns1 = dep_hub.connections[1]::GraphConnection
        ns2 = dep_hub.connections[2]::GraphConnection
        @test ns1.from_leg === ns1.to_leg
        @test ns2.from_leg === ns2.to_leg
    end

    # ── Valid connecting pair ──────────────────────────────────────────────────

    @testset "Valid connection found" begin
        # JFK→ORD arrives 09:00, ORD→LHR departs 11:00 → cnx_time=120 ≥ MCT_DD=60
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")
        lhr_rec = _make_station_record("LHR", "GB", "EUR")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)
        lhr_stn = GraphStation(lhr_rec)

        arr_rec = _make_leg_record(
            airline="UA", flt_no=100,
            org="JFK", dst="ORD",
            pax_dep=Int16(420),   # 07:00 dep
            pax_arr=Int16(540),   # 09:00 arr
            mct_status_arr='D',
        )
        dep_rec = _make_leg_record(
            airline="UA", flt_no=200,
            org="ORD", dst="LHR",
            pax_dep=Int16(660),   # 11:00 dep
            pax_arr=Int16(1080),  # 18:00 arr
            mct_status_dep='D',
        )

        arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
        dep_leg = GraphLeg(dep_rec, ord_stn, lhr_stn)

        push!(ord_stn.arrivals,   arr_leg)
        push!(ord_stn.departures, dep_leg)

        ctx   = _mock_ctx()
        rules = _make_rules()

        build_connections_at_station!(ord_stn, rules, ctx)

        # 1 nonstop (for dep_leg) + 1 connecting pair
        @test length(ord_stn.connections) == 2
        @test ord_stn.stats.num_nonstops == Int32(1)
        @test ord_stn.stats.num_connections == Int32(1)

        # Connection stored on legs
        @test length(arr_leg.connect_to)   == 1
        @test length(dep_leg.connect_from) == 1

        cp = arr_leg.connect_to[1]::GraphConnection
        @test cp.from_leg === arr_leg
        @test cp.to_leg   === dep_leg
        @test cp.cnx_time == Minutes(120)
        @test cp.mct      == Minutes(60)   # global MCT_DD default
    end

    # ── Self-reference skipped ─────────────────────────────────────────────────

    @testset "Self-reference skipped" begin
        # Same leg in both arrivals and departures → must not form a connecting pair
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)

        rec = _make_leg_record(org="JFK", dst="ORD", pax_dep=Int16(480), pax_arr=Int16(600))
        leg = GraphLeg(rec, jfk_stn, ord_stn)

        push!(ord_stn.arrivals,   leg)
        push!(ord_stn.departures, leg)   # same object in both lists

        ctx   = _mock_ctx()
        rules = _make_rules()

        build_connections_at_station!(ord_stn, rules, ctx)

        @test ord_stn.stats.num_pairs_evaluated == Int32(0)   # self skipped
        @test ord_stn.stats.num_connections     == Int32(0)

        # Only nonstop self-connection exists; no cross-pair connection
        @test length(ord_stn.connections) == 1
        ns = ord_stn.connections[1]::GraphConnection
        @test ns.from_leg === ns.to_leg
    end

    # ── Status bits set correctly ──────────────────────────────────────────────

    @testset "Status bits set correctly" begin

        @testset "Domestic: no INTERNATIONAL bit" begin
            # Both legs within US → STATUS_INTERNATIONAL must NOT be set
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lax_rec = _make_station_record("LAX", "US", "NAM")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lax_stn = GraphStation(lax_rec)

            arr_rec = _make_leg_record(
                org="JFK", dst="ORD",
                pax_arr=Int16(540),
                mct_status_arr='D',
            )
            dep_rec = _make_leg_record(
                org="ORD", dst="LAX",
                pax_dep=Int16(660),
                mct_status_dep='D',
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_international == Int32(0)
            @test ord_stn.stats.num_domestic      > Int32(0)
        end

        @testset "International: INTERNATIONAL bit set" begin
            # JFK (US) → ORD (US) → LHR (GB): different countries → international
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lhr_rec = _make_station_record("LHR", "GB", "EUR")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lhr_stn = GraphStation(lhr_rec)

            arr_rec = _make_leg_record(
                org="JFK", dst="ORD",
                pax_arr=Int16(540),
                mct_status_arr='D',
            )
            dep_rec = _make_leg_record(
                org="ORD", dst="LHR",
                pax_dep=Int16(660),
                mct_status_dep='D',
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lhr_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_connections  > Int32(0)
            cp = arr_leg.connect_to[1]::GraphConnection
            @test is_international(cp.status)
            @test ord_stn.stats.num_international == Int32(1)
        end

        @testset "Online connection: no INTERLINE or CODESHARE bit" begin
            # Both legs operated by UA, no codeshare → online
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lax_rec = _make_station_record("LAX", "US", "NAM")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lax_stn = GraphStation(lax_rec)

            arr_rec = _make_leg_record(
                airline="UA", org="JFK", dst="ORD",
                pax_arr=Int16(540), mct_status_arr='D',
            )
            dep_rec = _make_leg_record(
                airline="UA", org="ORD", dst="LAX",
                pax_dep=Int16(660), mct_status_dep='D',
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_connections > Int32(0)
            cp = arr_leg.connect_to[1]::GraphConnection
            @test !is_interline(cp.status)
            @test !is_codeshare(cp.status)
            @test ord_stn.stats.num_online   == Int32(1)
            @test ord_stn.stats.num_interline == Int32(0)
        end

        @testset "Interline connection: INTERLINE bit set" begin
            # UA arrives, AA departs (different operating carriers) → interline
            # Use INTERLINE_ALL mode so interline connections are allowed
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lhr_rec = _make_station_record("LHR", "GB", "EUR")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lhr_stn = GraphStation(lhr_rec)

            arr_rec = _make_leg_record(
                airline="UA", org="JFK", dst="ORD",
                pax_arr=Int16(540), mct_status_arr='I',
            )
            dep_rec = _make_leg_record(
                airline="AA", org="ORD", dst="LHR",
                pax_dep=Int16(660), mct_status_dep='I',
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lhr_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            # INTERLINE_ALL allows international interline connections
            ctx = _mock_ctx(interline=INTERLINE_ALL)
            build_connections_at_station!(ord_stn, _make_rules(), ctx)

            @test ord_stn.stats.num_connections > Int32(0)
            cp = arr_leg.connect_to[1]::GraphConnection
            @test is_interline(cp.status)
            @test ord_stn.stats.num_interline == Int32(1)
        end
    end

    # ── Validity window ────────────────────────────────────────────────────────

    @testset "Validity window computed" begin

        @testset "Intersection of eff/disc dates" begin
            # arr_leg: Jan–Jun, dep_leg: Apr–Dec → intersection Apr–Jun
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lax_rec = _make_station_record("LAX", "US", "NAM")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lax_stn = GraphStation(lax_rec)

            arr_rec = _make_leg_record(
                org="JFK", dst="ORD",
                pax_arr=Int16(540), mct_status_arr='D',
                eff_date=UInt32(20260101), disc_date=UInt32(20260630),
            )
            dep_rec = _make_leg_record(
                org="ORD", dst="LAX",
                pax_dep=Int16(660), mct_status_dep='D',
                eff_date=UInt32(20260401), disc_date=UInt32(20261231),
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_connections > Int32(0)
            cp = arr_leg.connect_to[1]::GraphConnection
            @test cp.valid_from == UInt32(20260401)
            @test cp.valid_to   == UInt32(20260630)
        end

        @testset "Non-overlapping validity windows skipped" begin
            # arr_leg: Jan–Mar, dep_leg: Jul–Dec → no overlap
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lax_rec = _make_station_record("LAX", "US", "NAM")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lax_stn = GraphStation(lax_rec)

            arr_rec = _make_leg_record(
                org="JFK", dst="ORD",
                pax_arr=Int16(540), mct_status_arr='D',
                eff_date=UInt32(20260101), disc_date=UInt32(20260331),
            )
            dep_rec = _make_leg_record(
                org="ORD", dst="LAX",
                pax_dep=Int16(660), mct_status_dep='D',
                eff_date=UInt32(20260701), disc_date=UInt32(20261231),
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_connections == Int32(0)
            @test isempty(arr_leg.connect_to)
        end

        @testset "AND of frequency bitmasks" begin
            # arr_leg: Mon/Wed/Fri (0x15), dep_leg: Mon/Tue/Wed (0x07)
            # intersection: Mon/Wed (0x05)
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lax_rec = _make_station_record("LAX", "US", "NAM")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lax_stn = GraphStation(lax_rec)

            arr_rec = _make_leg_record(
                org="JFK", dst="ORD",
                pax_arr=Int16(540), mct_status_arr='D',
                frequency=UInt8(0x15),  # Mon=1, Wed=4, Fri=16 → 0b0010101
            )
            dep_rec = _make_leg_record(
                org="ORD", dst="LAX",
                pax_dep=Int16(660), mct_status_dep='D',
                frequency=UInt8(0x07),  # Mon=1, Tue=2, Wed=4 → 0b0000111
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_connections > Int32(0)
            cp = arr_leg.connect_to[1]::GraphConnection
            @test cp.valid_days == UInt8(0x05)   # Mon & Wed only
        end

        @testset "Zero DOW intersection skipped" begin
            # arr_leg: Mon only (0x01), dep_leg: Tue only (0x02) → no overlap
            jfk_rec = _make_station_record("JFK", "US", "NAM")
            ord_rec = _make_station_record("ORD", "US", "NAM")
            lax_rec = _make_station_record("LAX", "US", "NAM")

            jfk_stn = GraphStation(jfk_rec)
            ord_stn = GraphStation(ord_rec)
            lax_stn = GraphStation(lax_rec)

            arr_rec = _make_leg_record(
                org="JFK", dst="ORD",
                pax_arr=Int16(540), mct_status_arr='D',
                frequency=UInt8(0x01),  # Mon only
            )
            dep_rec = _make_leg_record(
                org="ORD", dst="LAX",
                pax_dep=Int16(660), mct_status_dep='D',
                frequency=UInt8(0x02),  # Tue only
            )
            arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
            dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

            push!(ord_stn.arrivals,   arr_leg)
            push!(ord_stn.departures, dep_leg)

            build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

            @test ord_stn.stats.num_connections == Int32(0)
            @test isempty(arr_leg.connect_to)
        end
    end

    # ── StationStats accumulation ──────────────────────────────────────────────

    @testset "StationStats accumulated" begin
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")
        lhr_rec = _make_station_record("LHR", "GB", "EUR")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)
        lhr_stn = GraphStation(lhr_rec)

        arr_rec = _make_leg_record(
            airline="UA", org="JFK", dst="ORD",
            pax_arr=Int16(540), mct_status_arr='D',
            distance=800.0f0,
        )
        dep_rec = _make_leg_record(
            airline="UA", org="ORD", dst="LHR",
            pax_dep=Int16(660), mct_status_dep='D',
            distance=3500.0f0,
        )
        arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
        dep_leg = GraphLeg(dep_rec, ord_stn, lhr_stn)

        push!(ord_stn.arrivals,   arr_leg)
        push!(ord_stn.departures, dep_leg)

        build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

        s = ord_stn.stats
        @test s.num_departures == Int32(1)
        @test s.num_arrivals   == Int32(1)
        @test s.num_connections == Int32(1)
        @test s.num_pairs_evaluated >= Int32(1)

        # Carrier tracking: UA appears in both legs
        @test AirlineCode("UA") in s.unique_carriers

        # Equipment tracking: "738" appears in the leg records
        @test InlineString7("738") in s.unique_equipment

        # Distance tracking
        @test s.total_dep_distance ≈ 3500.0
        @test s.total_arr_distance ≈ 800.0

        # avg_ground_time: cnx_time = 660 - 540 = 120 minutes
        @test s.avg_ground_time ≈ 120.0
    end

    # ── Through-flight detection ───────────────────────────────────────────────

    @testset "Through-flight detection" begin
        # Two legs sharing the same GraphSegment with a non-zero segment_hash
        # → is_through=true, STATUS_THROUGH set
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")
        lax_rec = _make_station_record("LAX", "US", "NAM")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)
        lax_stn = GraphStation(lax_rec)

        shared_seg_hash = UInt64(0xDEADBEEF12345678)

        arr_rec = _make_leg_record(
            airline="UA", flt_no=300, leg_seq=UInt8(1),
            org="JFK", dst="ORD",
            pax_arr=Int16(540), mct_status_arr='D',
            segment_hash=shared_seg_hash,
        )
        dep_rec = _make_leg_record(
            airline="UA", flt_no=300, leg_seq=UInt8(2),
            org="ORD", dst="LAX",
            pax_dep=Int16(660), mct_status_dep='D',
            segment_hash=shared_seg_hash,
        )

        arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
        dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

        # Link both legs to the same GraphSegment instance
        seg_rec = SegmentRecord(
            segment_hash=shared_seg_hash,
            airline=AirlineCode("UA"),
            flt_no=Int16(300),
            op_suffix=' ',
            itin_var=UInt8(1),
            itin_var_overflow=' ',
            svc_type='J',
            operating_date=UInt32(20260101),
            num_legs=UInt8(2),
            first_leg_seq=UInt8(1),
            last_leg_seq=UInt8(2),
            segment_org=StationCode("JFK"),
            segment_dst=StationCode("LAX"),
            flown_distance=Distance(1800.0),
            market_distance=Distance(1800.0),
            segment_circuity=Float32(1.0),
            segment_pax_dep=Int16(480),
            segment_pax_arr=Int16(780),
            segment_ac_dep=Int16(480),
            segment_ac_arr=Int16(780),
        )
        shared_seg = GraphSegment(record=seg_rec, operating_airline=AirlineCode("UA"))
        arr_leg.segment = shared_seg
        dep_leg.segment = shared_seg

        push!(ord_stn.arrivals,   arr_leg)
        push!(ord_stn.departures, dep_leg)

        build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

        @test ord_stn.stats.num_connections > Int32(0)
        cp = arr_leg.connect_to[1]::GraphConnection
        @test cp.is_through
        @test is_through(cp.status)
        @test ord_stn.stats.num_through == Int32(1)
    end

    # ── Multi-station build_connections! ──────────────────────────────────────

    @testset "build_connections! iterates all stations" begin
        # Two connect-point stations: ORD and LAX; each has 1 arr + 1 dep pair
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")
        lax_rec = _make_station_record("LAX", "US", "NAM")
        sfo_rec = _make_station_record("SFO", "US", "NAM")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)
        lax_stn = GraphStation(lax_rec)
        sfo_stn = GraphStation(sfo_rec)

        # ORD leg pair: JFK→ORD arrives 09:00, ORD→LAX departs 11:00
        arr1 = GraphLeg(
            _make_leg_record(org="JFK", dst="ORD", pax_arr=Int16(540), mct_status_arr='D'),
            jfk_stn, ord_stn,
        )
        dep1 = GraphLeg(
            _make_leg_record(org="ORD", dst="LAX", pax_dep=Int16(660), mct_status_dep='D'),
            ord_stn, lax_stn,
        )
        push!(ord_stn.arrivals, arr1)
        push!(ord_stn.departures, dep1)

        # LAX leg pair: ORD→LAX arrives 14:00, LAX→SFO departs 16:00
        arr2 = GraphLeg(
            _make_leg_record(org="ORD", dst="LAX", pax_arr=Int16(840), mct_status_arr='D'),
            ord_stn, lax_stn,
        )
        dep2 = GraphLeg(
            _make_leg_record(org="LAX", dst="SFO", pax_dep=Int16(960), mct_status_dep='D'),
            lax_stn, sfo_stn,
        )
        push!(lax_stn.arrivals, arr2)
        push!(lax_stn.departures, dep2)

        stations = Dict{StationCode, GraphStation}(
            StationCode("ORD") => ord_stn,
            StationCode("LAX") => lax_stn,
        )

        ctx   = _mock_ctx()
        rules = _make_rules()

        build_connections!(stations, rules, ctx)

        # Both stations should have at least one accepted connection
        @test ord_stn.stats.num_connections >= Int32(1)
        @test lax_stn.stats.num_connections >= Int32(1)
        @test !isempty(arr1.connect_to)
        @test !isempty(arr2.connect_to)
    end

    # ── Rule-chain short-circuit (MCT failure) ────────────────────────────────

    @testset "MCT failure rejects connection" begin
        # cnx_time = 30 min < MCT_DD default 60 min → rejected
        jfk_rec = _make_station_record("JFK", "US", "NAM")
        ord_rec = _make_station_record("ORD", "US", "NAM")
        lax_rec = _make_station_record("LAX", "US", "NAM")

        jfk_stn = GraphStation(jfk_rec)
        ord_stn = GraphStation(ord_rec)
        lax_stn = GraphStation(lax_rec)

        arr_rec = _make_leg_record(
            org="JFK", dst="ORD",
            pax_arr=Int16(540),    # 09:00 arr
            mct_status_arr='D',
        )
        dep_rec = _make_leg_record(
            org="ORD", dst="LAX",
            pax_dep=Int16(570),    # 09:30 dep → cnx_time = 30 min
            mct_status_dep='D',
        )
        arr_leg = GraphLeg(arr_rec, jfk_stn, ord_stn)
        dep_leg = GraphLeg(dep_rec, ord_stn, lax_stn)

        push!(ord_stn.arrivals,   arr_leg)
        push!(ord_stn.departures, dep_leg)

        build_connections_at_station!(ord_stn, _make_rules(), _mock_ctx())

        @test ord_stn.stats.num_connections == Int32(0)
        @test isempty(arr_leg.connect_to)
        @test isempty(dep_leg.connect_from)
    end

end

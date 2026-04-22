using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "DFS Search" begin

    # ── Test helpers ──────────────────────────────────────────────────────────

    function _stn_rec(code, country, region; latitude=0.0, longitude=0.0, city="", state="")
        StationRecord(
            code=StationCode(code),
            country=InlineString3(country),
            state=InlineString3(state),
            city=InlineString3(city),
            region=InlineString3(region),
            latitude=latitude,
            longitude=longitude,
            utc_offset=Int16(0),
        )
    end

    # Realistic lat/lng values for JFK, ORD, LHR
    JFK_LAT = 40.6413
    JFK_LNG = -73.7781
    ORD_LAT = 41.9742
    ORD_LNG = -87.9073
    LHR_LAT = 51.4775
    LHR_LNG = -0.4614

    function _leg_rec(;
        carrier="UA",
        flight_number=100,
        departure_station="JFK",
        arrival_station="ORD",
        passenger_departure_time=Int16(480),
        passenger_arrival_time=Int16(600),
        arrival_date_variation=Int8(0),
        distance=800.0f0,
        aircraft_type="738",
        frequency=UInt8(0x7f),           # all days
        effective_date=UInt32(20260101),
        discontinue_date=UInt32(20261231),
        dep_intl_dom='D',
        arr_intl_dom='D',
        leg_sequence_number=UInt8(1),
        traffic_restriction_for_leg="",
        departure_utc_offset=Int16(0),
        arrival_utc_offset=Int16(0),
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
            passenger_departure_time=passenger_departure_time,
            passenger_arrival_time=passenger_arrival_time,
            aircraft_departure_time=passenger_departure_time,
            aircraft_arrival_time=passenger_arrival_time,
            departure_utc_offset=departure_utc_offset,
            arrival_utc_offset=arrival_utc_offset,
            departure_date_variation=Int8(0),
            arrival_date_variation=arrival_date_variation,
            aircraft_type=InlineString7(aircraft_type),
            body_type='N',
            departure_terminal=InlineString3("1"),
            arrival_terminal=InlineString3("1"),
            aircraft_owner=AirlineCode(carrier),
            operating_date=UInt32(20260615),
            day_of_week=UInt8(1),
            effective_date=effective_date,
            discontinue_date=discontinue_date,
            frequency=frequency,
            dep_intl_dom=dep_intl_dom,
            arr_intl_dom=arr_intl_dom,
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

    # Build a minimal RuntimeContext for search tests.
    # Uses INTERLINE_ALL + generous circuity so test connections are not pruned.
    function _search_ctx(;
        max_stops=2,
        scope=SCOPE_ALL,
        interline=INTERLINE_ALL,
        constraints=nothing,
    )
        ps = ParameterSet(
            max_stops=Int16(max_stops),
            domestic_circuity_extra_miles=50_000.0,
            max_circuity=5.0,
        )
        sc = constraints === nothing ? SearchConstraints(defaults=ps) : constraints
        RuntimeContext(
            config=SearchConfig(scope=scope, interline=interline),
            constraints=sc,
            itn_rules=build_itn_rules(SearchConfig(scope=scope, interline=interline)),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
        )
    end

    # Build a 3-station graph: JFK → ORD → LHR
    #   Leg 1 (jfk_ord): JFK departs 08:00, ORD arrives 10:00
    #   Leg 2 (ord_lhr): ORD departs 12:00, LHR arrives 22:00+1
    #   Direct leg (jfk_lhr): JFK departs 09:00, LHR arrives 21:00+1
    # Returns (stations, jfk_stn, ord_stn, lhr_stn, leg_jfk_ord, leg_ord_lhr, leg_jfk_lhr)
    function _build_graph(;
        frequency_1=UInt8(0x7f),
        frequency_2=UInt8(0x7f),
        frequency_ns=UInt8(0x7f),
        eff=UInt32(20260101),
        disc=UInt32(20261231),
        eqp1="738",
        eqp2="738",
        jfk_lat=JFK_LAT, jfk_lng=JFK_LNG,
        ord_lat=ORD_LAT, ord_lng=ORD_LNG,
        lhr_lat=LHR_LAT, lhr_lng=LHR_LNG,
        jfk_country="US", jfk_region="NAM",
        ord_country="US", ord_region="NAM",
        lhr_country="GB", lhr_region="EUR",
    )
        jfk_stn = GraphStation(
            _stn_rec("JFK", jfk_country, jfk_region;
                latitude=jfk_lat, longitude=jfk_lng, city="NYC", state="NY")
        )
        ord_stn = GraphStation(
            _stn_rec("ORD", ord_country, ord_region;
                latitude=ord_lat, longitude=ord_lng, city="CHI", state="IL")
        )
        lhr_stn = GraphStation(
            _stn_rec("LHR", lhr_country, lhr_region;
                latitude=lhr_lat, longitude=lhr_lng, city="LON")
        )

        # Leg 1: JFK → ORD  dep 08:00 (480 min)  arr 10:00 (600 min)
        rec1 = _leg_rec(
            carrier="UA", flight_number=200,
            departure_station="JFK", arrival_station="ORD",
            passenger_departure_time=Int16(480), passenger_arrival_time=Int16(600),
            distance=800.0f0, aircraft_type=eqp1,
            frequency=frequency_1, effective_date=eff, discontinue_date=disc,
            arr_intl_dom='D', dep_intl_dom='D',
        )
        # Leg 2: ORD → LHR  dep 12:00 (720 min)  arr 22:00 (1320 min)
        rec2 = _leg_rec(
            carrier="UA", flight_number=916,
            departure_station="ORD", arrival_station="LHR",
            passenger_departure_time=Int16(720), passenger_arrival_time=Int16(1320),
            distance=3941.0f0, aircraft_type=eqp2,
            frequency=frequency_2, effective_date=eff, discontinue_date=disc,
            arr_intl_dom='I', dep_intl_dom='D',
        )
        # Nonstop: JFK → LHR  dep 09:00 (540 min)  arr 21:00 (1260 min)
        rec_ns = _leg_rec(
            carrier="UA", flight_number=100,
            departure_station="JFK", arrival_station="LHR",
            passenger_departure_time=Int16(540), passenger_arrival_time=Int16(1260),
            distance=3451.0f0, aircraft_type="789",
            frequency=frequency_ns, effective_date=eff, discontinue_date=disc,
            arr_intl_dom='I', dep_intl_dom='D',
        )

        leg1 = GraphLeg(rec1, jfk_stn, ord_stn)
        leg2 = GraphLeg(rec2, ord_stn, lhr_stn)
        leg_ns = GraphLeg(rec_ns, jfk_stn, lhr_stn)

        # Register legs at stations
        push!(jfk_stn.departures, leg1)
        push!(jfk_stn.departures, leg_ns)
        push!(ord_stn.arrivals, leg1)
        push!(ord_stn.departures, leg2)
        push!(lhr_stn.arrivals, leg2)
        push!(lhr_stn.arrivals, leg_ns)

        # Build connections using the standard rule chain
        # MCT: JFK→ORD arrives 10:00, ORD→LHR departs 12:00 → cnx_time=120 ≥ MCT_DD=60
        constraints = SearchConstraints(
            defaults=ParameterSet(
                max_stops=Int16(2),
                domestic_circuity_extra_miles=50_000.0,
                max_circuity=5.0,
            )
        )
        cnx_ctx = (
            config=SearchConfig(interline=INTERLINE_ALL),
            constraints=constraints,
            build_stats=BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache=Dict{MCTCacheKey,MCTResult}(),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
            target_date=UInt32(0),
            mct_selections=MCTSelectionRow[],
        )
        cnx_rules = build_cnx_rules(SearchConfig(interline=INTERLINE_ALL), constraints, MCTLookup())

        stations = Dict{StationCode,GraphStation}(
            StationCode("JFK") => jfk_stn,
            StationCode("ORD") => ord_stn,
            StationCode("LHR") => lhr_stn,
        )
        build_connections!(stations, cnx_rules, cnx_ctx)

        return (stations, jfk_stn, ord_stn, lhr_stn, leg1, leg2, leg_ns)
    end

    # ── Nonstop found ──────────────────────────────────────────────────────────

    @testset "Nonstop found" begin
        stations, jfk, ord, lhr, _, _, leg_ns = _build_graph()
        ctx = _search_ctx()

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        nonstops = filter(i -> i.num_stops == Int16(0), itns)
        @test length(nonstops) >= 1
        # Nonstop has exactly 1 connection (the self-connection)
        @test all(i -> length(i.connections) == 1, nonstops)
    end

    # ── 1-stop found ──────────────────────────────────────────────────────────

    @testset "1-stop found" begin
        stations, _, _, _, _, _, _ = _build_graph()
        ctx = _search_ctx()

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        one_stops = filter(i -> i.num_stops == Int16(1), itns)
        @test length(one_stops) >= 1
        # 1-stop itinerary has 2 connections
        @test all(i -> length(i.connections) == 2, one_stops)
    end

    # ── Date filtering ─────────────────────────────────────────────────────────

    @testset "Date filtering" begin
        # Legs valid Jan–Jun 2026; search on Jul 15 → no results
        stations, _, _, _, _, _, _ = _build_graph(
            eff=UInt32(20260101),
            disc=UInt32(20260630),
        )
        ctx = _search_ctx()

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 7, 15), ctx,
        )
        @test isempty(itns)
    end

    # ── DOW filtering ──────────────────────────────────────────────────────────

    @testset "DOW filtering" begin
        # Legs run Mon only (0x01 = bit 0); 2026-06-15 is a Monday (ISO 1)
        # 2026-06-16 is a Tuesday (ISO 2, bit 1) → no results
        stations, _, _, _, _, _, _ = _build_graph(
            frequency_1=UInt8(0x01),
            frequency_2=UInt8(0x01),
            frequency_ns=UInt8(0x01),
        )
        ctx = _search_ctx()

        # Monday search: should find itineraries
        itns_mon = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )
        @test !isempty(itns_mon)

        # Tuesday search: should find nothing
        itns_tue = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 16), ctx,
        )
        @test isempty(itns_tue)
    end

    # ── max_stops=0 returns nonstops only ─────────────────────────────────────

    @testset "max_stops=0 returns nonstops only" begin
        stations, _, _, _, _, _, _ = _build_graph()
        ctx = _search_ctx(max_stops=0)

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        @test !isempty(itns)
        @test all(i -> i.num_stops == Int16(0), itns)
    end

    # ── Market distance and circuity computed ─────────────────────────────────

    @testset "Market distance and circuity computed" begin
        stations, _, _, _, _, _, _ = _build_graph()
        ctx = _search_ctx()

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        @test !isempty(itns)
        for itn in itns
            # Market distance is computed from haversine; should be > 0
            @test itn.market_distance > Distance(0)
            # Circuity = total_distance / market_distance; should be >= 1 for real routes
            @test itn.circuity >= 0.0f0
            # Total distance should be positive
            @test itn.total_distance > Distance(0)
        end

        # Market distance is cached in gc_cache after first call
        gc_key = (StationCode("JFK"), StationCode("LHR"))
        @test haskey(ctx.gc_cache, gc_key)
        @test ctx.gc_cache[gc_key] > 0.0
    end

    # ── num_eqp_changes counted ────────────────────────────────────────────────

    @testset "num_eqp_changes counted" begin
        # Build graph with different equipment on the two legs
        stations, _, _, _, _, _, _ = _build_graph(eqp1="738", eqp2="789")
        ctx = _search_ctx()

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        one_stops = filter(i -> i.num_stops == Int16(1), itns)
        @test !isempty(one_stops)
        # Different equipment on each leg → 1 equipment change
        @test any(i -> i.num_eqp_changes == Int16(1), one_stops)
    end

    # ── Geographic diversity ───────────────────────────────────────────────────

    @testset "Geographic diversity" begin
        # JFK(US/NAM) → ORD(US/NAM) → LHR(GB/EUR) touches US and GB → 2 countries
        stations, _, _, _, _, _, _ = _build_graph()
        ctx = _search_ctx()

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        one_stops = filter(i -> i.num_stops == Int16(1), itns)
        @test !isempty(one_stops)
        @test any(i -> i.num_countries >= Int16(2), one_stops)

        # Region diversity: NAM and EUR
        @test any(i -> i.num_regions >= Int16(2), one_stops)

        # Metro diversity: New York, Chicago, London
        @test any(i -> i.num_metros >= Int16(2), one_stops)
    end

    # ── _is_valid_on_date ─────────────────────────────────────────────────────

    @testset "_is_valid_on_date" begin
        using ItinerarySearch: _is_valid_on_date

        # Build a minimal connection to test the helper directly
        jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
        ord_stn = GraphStation(_stn_rec("ORD", "US", "NAM"))
        rec = _leg_rec(
            effective_date=UInt32(20260601), discontinue_date=UInt32(20260630),
            frequency=UInt8(0x01),  # Mon only
        )
        leg = GraphLeg(rec, jfk_stn, ord_stn)
        ns_cp = nonstop_connection(leg, jfk_stn)
        # ns_cp.valid_from=20260601, valid_to=20260630, valid_days=0x01 (Mon)

        # In window, correct DOW (Monday = dow_bit(1))
        @test _is_valid_on_date(ns_cp, UInt32(20260615), dow_bit(1))

        # In window, wrong DOW (Tuesday)
        @test !_is_valid_on_date(ns_cp, UInt32(20260616), dow_bit(2))

        # Before window
        @test !_is_valid_on_date(ns_cp, UInt32(20260531), dow_bit(1))

        # After window
        @test !_is_valid_on_date(ns_cp, UInt32(20260701), dow_bit(1))

        # Exactly on valid_from boundary
        @test _is_valid_on_date(ns_cp, UInt32(20260601), dow_bit(1))

        # Exactly on valid_to boundary (June 30 is a Tuesday); test with Tuesday DOW
        # The connection runs Mon only (0x01), so Tuesday DOW bit (2) should fail
        @test !_is_valid_on_date(ns_cp, UInt32(20260630), dow_bit(2))  # Tuesday, not in Mon-only mask
    end

    # ── _compute_elapsed ──────────────────────────────────────────────────────

    @testset "_compute_elapsed" begin
        using ItinerarySearch: _compute_elapsed

        @testset "Empty itinerary returns 0" begin
            itn = Itinerary()
            @test _compute_elapsed(itn) == Int32(0)
        end

        @testset "Nonstop: dep 08:00 arr 10:00" begin
            jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
            ord_stn = GraphStation(_stn_rec("ORD", "US", "NAM"))
            rec = _leg_rec(passenger_departure_time=Int16(480), passenger_arrival_time=Int16(600), arrival_date_variation=Int8(0))
            leg = GraphLeg(rec, jfk_stn, ord_stn)
            cp = nonstop_connection(leg, jfk_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            # elapsed = 600 - 480 = 120 min
            @test _compute_elapsed(itn) == Int32(120)
        end

        @testset "Overnight flight: arrival_date_variation=1" begin
            jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
            lhr_stn = GraphStation(_stn_rec("LHR", "GB", "EUR"))
            rec = _leg_rec(passenger_departure_time=Int16(540), passenger_arrival_time=Int16(1260), arrival_date_variation=Int8(0))
            leg = GraphLeg(rec, jfk_stn, lhr_stn)
            cp = nonstop_connection(leg, jfk_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            # elapsed = 1260 - 540 = 720 min (no date var in this record)
            @test _compute_elapsed(itn) == Int32(720)
        end

        @testset "arr_date_var adds 1440 per day" begin
            jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
            lhr_stn = GraphStation(_stn_rec("LHR", "GB", "EUR"))
            # dep 23:00 (1380), arr 07:00 next day (420 + 1440)
            rec = _leg_rec(passenger_departure_time=Int16(1380), passenger_arrival_time=Int16(420), arrival_date_variation=Int8(1))
            leg = GraphLeg(rec, jfk_stn, lhr_stn)
            cp = nonstop_connection(leg, jfk_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            # elapsed = (420 + 1440) - 1380 = 480 min
            @test _compute_elapsed(itn) == Int32(480)
        end

        @testset "UTC elapsed: ORD (UTC-5) dep 09:00 → LHR (UTC+0) arr 22:00" begin
            # ORD offset = -300 min (UTC-5), LHR offset = 0 min (UTC+0)
            # local dep = 09:00 = 540 min, local arr = 22:00 = 1320 min
            # utc_dep = 540 - (-300) = 840  (14:00 UTC)
            # utc_arr = 1320 - 0     = 1320 (22:00 UTC)
            # elapsed = 1320 - 840 = 480 min (8h block) — local math would give 780 (13h)
            ord_stn = GraphStation(_stn_rec("ORD", "US", "NAM"))
            lhr_stn = GraphStation(_stn_rec("LHR", "GB", "EUR"))
            rec = _leg_rec(
                departure_station="ORD", arrival_station="LHR",
                passenger_departure_time=Int16(540), passenger_arrival_time=Int16(1320),
                departure_utc_offset=Int16(-300), arrival_utc_offset=Int16(0),
                arrival_date_variation=Int8(0),
            )
            leg = GraphLeg(rec, ord_stn, lhr_stn)
            cp = nonstop_connection(leg, ord_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            @test _compute_elapsed(itn) == Int32(480)
        end

        @testset "UTC elapsed: same-TZ nonstop is unchanged" begin
            # JFK (UTC-5, offset=-300) dep 08:00 → BOS (UTC-5, offset=-300) arr 09:20
            # utc_dep = 480 - (-300) = 780, utc_arr = 560 - (-300) = 860
            # elapsed = 860 - 780 = 80 min — same as local-time diff
            jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
            bos_stn = GraphStation(_stn_rec("BOS", "US", "NAM"))
            rec = _leg_rec(
                departure_station="JFK", arrival_station="BOS",
                passenger_departure_time=Int16(480), passenger_arrival_time=Int16(560),
                departure_utc_offset=Int16(-300), arrival_utc_offset=Int16(-300),
                arrival_date_variation=Int8(0),
            )
            leg = GraphLeg(rec, jfk_stn, bos_stn)
            cp = nonstop_connection(leg, jfk_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            @test _compute_elapsed(itn) == Int32(80)
        end

        @testset "overnight with missing arrival_date_variation infers +1 day" begin
            # Regression test for LH-style records in uaoa_ssim.new.dat: the
            # SSIM file leaves column 194 blank for some non-UA carriers, so
            # arrival_date_variation parses to 0 even for overnight flights.
            # Before the fix, _compute_elapsed would produce a negative total
            # and the outer `max(0, total)` clamp would silently return 0.
            #
            # Example from real data — LH 431 ORD→FRA:
            #   pax_dep = 1605 (16:05 CST),   dep_utc_offset = -300 (UTC-5)
            #   pax_arr = 0725 (07:25 CET),   arr_utc_offset = +120 (UTC+2)
            #   arrival_date_variation = 0 (SHOULD be 1 but source left blank)
            # UTC: dep = 1605 - (-300) = 1905; arr = 0725 - 120 = 605
            #      block = 605 - 1905 = -1300  → with +1440 rollover → +140? No.
            # Actual: 8h20m = 500 min.  arr = 605 + 1440 = 2045; 2045-1905 = 140.
            # Hmm that's 2h20m which is wrong.  Let me use times that give 500:
            #   pax_dep = 1200 (12:00 CST),   dep_utc = 1200-(-300) = 1500
            #   pax_arr = 0320 (03:20 CET),   arr_utc = 0320-120 = 200
            #   Want block = 500 → arr needs to be 2000 UTC → +1440 gives 1640 (no).
            # Simpler: construct a case where block without +1440 is negative,
            # and verify the inferred day rollover gives a sensible positive
            # value.
            ord_stn = GraphStation(_stn_rec("ORD", "US", "NAM"))
            fra_stn = GraphStation(_stn_rec("FRA", "DE", "EUR"))
            rec = _leg_rec(
                carrier="LH", flight_number=431,
                departure_station="ORD", arrival_station="FRA",
                passenger_departure_time=Int16(1200),  # 20:00 local ORD
                passenger_arrival_time=Int16(660),     # 11:00 local FRA
                departure_utc_offset=Int16(-300),      # UTC-5
                arrival_utc_offset=Int16(120),         # UTC+2
                arrival_date_variation=Int8(0),        # BUG: source didn't flag overnight
            )
            leg = GraphLeg(rec, ord_stn, fra_stn)
            cp = nonstop_connection(leg, ord_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            # Raw UTC math: dep=1500, arr=540, diff=-960 (negative → overnight).
            # With +1440 day rollover: 1980 - 1500 = 480 min = 8h.
            elapsed = _compute_elapsed(itn)
            @test elapsed == Int32(480)
            @test elapsed > Int32(0)   # critical: not silently clamped to 0
        end

        @testset "_leg_utc_block: blank arr_date_var on overnight infers +1 day" begin
            # Direct test of the shared helper; same scenario as the
            # Itinerary-level test above but exercises the helper alone so
            # downstream consumers (flight_time accumulator, MAFT rule,
            # CSV/JSON output flight_minutes) all benefit.
            using ItinerarySearch: _leg_utc_block
            ord = GraphStation(_stn_rec("ORD", "US", "NAM"))
            fra = GraphStation(_stn_rec("FRA", "DE", "EUR"))
            rec = _leg_rec(
                carrier="LH", flight_number=431,
                departure_station="ORD", arrival_station="FRA",
                passenger_departure_time=Int16(1200),
                passenger_arrival_time=Int16(660),
                departure_utc_offset=Int16(-300),
                arrival_utc_offset=Int16(120),
                arrival_date_variation=Int8(0),    # source missed it
            )
            @test _leg_utc_block(rec) == Int32(480)  # 8h, not 0
        end

        @testset "_leg_utc_block: same-day flight unchanged" begin
            using ItinerarySearch: _leg_utc_block
            jfk = GraphStation(_stn_rec("JFK", "US", "NAM"))
            bos = GraphStation(_stn_rec("BOS", "US", "NAM"))
            rec = _leg_rec(
                departure_station="JFK", arrival_station="BOS",
                passenger_departure_time=Int16(480),
                passenger_arrival_time=Int16(560),
                departure_utc_offset=Int16(-300),
                arrival_utc_offset=Int16(-300),
                arrival_date_variation=Int8(0),
            )
            @test _leg_utc_block(rec) == Int32(80)  # JFK→BOS, 80 min, no rollover
        end

        @testset "arrival_date_variation=1 explicitly still works" begin
            # When the source DOES flag overnight correctly, behaviour is
            # unchanged — we don't double-add 1440.
            jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
            lhr_stn = GraphStation(_stn_rec("LHR", "GB", "EUR"))
            rec = _leg_rec(
                carrier="UA", flight_number=99,
                departure_station="JFK", arrival_station="LHR",
                passenger_departure_time=Int16(1260),  # 21:00 EST
                passenger_arrival_time=Int16(480),     # 08:00 GMT next day
                departure_utc_offset=Int16(-300),
                arrival_utc_offset=Int16(0),
                arrival_date_variation=Int8(1),        # explicit
            )
            leg = GraphLeg(rec, jfk_stn, lhr_stn)
            cp = nonstop_connection(leg, jfk_stn)
            itn = Itinerary(connections=GraphConnection[cp])
            # UTC: dep = 1260-(-300) = 1560; arr = 480-0 + 1*1440 = 1920
            # block = 1920 - 1560 = 360 min = 6h.
            @test _compute_elapsed(itn) == Int32(360)
        end
    end

    # ── UTC elapsed via search_itineraries ────────────────────────────────────

    @testset "UTC elapsed time" begin
        using ItinerarySearch: _compute_elapsed

        # Verify that elapsed_time on committed itineraries uses UTC math.
        # Build a nonstop JFK→LHR with UTC offsets: JFK=-300, LHR=0.
        # Local: dep 09:00 (540), arr 22:00 (1320) → local diff = 780 (13h)
        # UTC:   dep 840 (14:00), arr 1320 (22:00) → UTC diff  = 480 (8h)
        jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM"))
        lhr_stn = GraphStation(_stn_rec("LHR", "GB", "EUR"))

        rec_ns = _leg_rec(
            carrier="UA", flight_number=100,
            departure_station="JFK", arrival_station="LHR",
            passenger_departure_time=Int16(540), passenger_arrival_time=Int16(1320),
            departure_utc_offset=Int16(-300), arrival_utc_offset=Int16(0),
            arrival_date_variation=Int8(0),
            distance=3451.0f0,
        )
        leg_ns = GraphLeg(rec_ns, jfk_stn, lhr_stn)
        push!(jfk_stn.departures, leg_ns)
        push!(lhr_stn.arrivals, leg_ns)

        constraints = SearchConstraints(
            defaults=ParameterSet(
                max_stops=Int16(0),
                domestic_circuity_extra_miles=50_000.0,
                max_circuity=5.0,
            ),
        )
        cnx_ctx = (
            config=SearchConfig(interline=INTERLINE_ALL),
            constraints=constraints,
            build_stats=BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache=Dict{MCTCacheKey,MCTResult}(),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
            target_date=UInt32(0),
            mct_selections=MCTSelectionRow[],
        )
        cnx_rules = build_cnx_rules(SearchConfig(interline=INTERLINE_ALL), constraints, MCTLookup())
        stations = Dict{StationCode,GraphStation}(
            StationCode("JFK") => jfk_stn,
            StationCode("LHR") => lhr_stn,
        )
        build_connections!(stations, cnx_rules, cnx_ctx)

        ctx = _search_ctx(max_stops=0)
        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        @test !isempty(itns)
        nonstop = first(filter(i -> i.num_stops == Int16(0), itns))
        # UTC-corrected elapsed must be 480 min (8h), not 780 (13h local)
        @test nonstop.elapsed_time == Int32(480)
    end

    # ── Direction pruning ──────────────────────────────────────────────────────

    @testset "Direction pruning" begin
        using ItinerarySearch: _direction_ok

        # JFK→LHR bearing ≈ 51°; JFK→SYD bearing ≈ 266° → divergence ≈ 145°
        # With default max_divergence_deg=120, SYD should be pruned from JFK→LHR search.
        jfk_stn = GraphStation(_stn_rec("JFK", "US", "NAM";
            latitude=JFK_LAT, longitude=JFK_LNG))
        syd_stn = GraphStation(_stn_rec("SYD", "AU", "PAC";
            latitude=-33.8688, longitude=151.2093))
        lhr_stn = GraphStation(_stn_rec("LHR", "GB", "EUR";
            latitude=LHR_LAT, longitude=LHR_LNG))

        # Divergence ≈ 145° > 120° → SYD should be pruned (false)
        @test !_direction_ok(jfk_stn, syd_stn, lhr_stn)

        # With a looser threshold of 150°, SYD passes
        @test _direction_ok(jfk_stn, syd_stn, lhr_stn; max_divergence_deg=150.0)

        # When next_dst IS the final destination, always passes
        @test _direction_ok(jfk_stn, lhr_stn, lhr_stn)

        # When current station coordinates are zero, always passes (no data)
        zero_stn = GraphStation(_stn_rec("ZZZ", "US", "NAM"; latitude=0.0, longitude=0.0))
        @test _direction_ok(zero_stn, syd_stn, lhr_stn)
    end

    # ── SearchStats populated ──────────────────────────────────────────────────

    @testset "SearchStats populated" begin
        stations, _, _, _, _, _, _ = _build_graph()
        ctx = _search_ctx()

        @test ctx.search_stats.queries == Int32(0)
        @test ctx.search_stats.paths_found == Int32(0)

        search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        @test ctx.search_stats.queries == Int32(1)
        @test ctx.search_stats.paths_found > Int32(0)
        @test sum(ctx.search_stats.paths_by_stops) == ctx.search_stats.paths_found

        # Nonstop bucket (index 1) and 1-stop bucket (index 2) should both be > 0
        @test ctx.search_stats.paths_by_stops[1] > Int32(0)  # nonstop
        @test ctx.search_stats.paths_by_stops[2] > Int32(0)  # 1-stop

        # Second search increments query counter
        search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )
        @test ctx.search_stats.queries == Int32(2)
    end

    # ── Missing station returns empty ──────────────────────────────────────────

    @testset "Missing station returns empty" begin
        stations, _, _, _, _, _, _ = _build_graph()
        ctx = _search_ctx()

        # Unknown origin
        itns = search_itineraries(
            stations, StationCode("XXX"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )
        @test isempty(itns)

        # Unknown destination
        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("YYY"),
            Date(2026, 6, 15), ctx,
        )
        @test isempty(itns)
    end

    # ── Round-trip detection and splitting ────────────────────────────────────

    @testset "Round-trip detection and splitting" begin
        # Build a 3-station circular graph: A → B → C → A
        # A is at (0, 0), B is far east at (0, 90) — farthest from A,
        # C is at (0, 45) — on the way back.
        #
        # Leg 1: A → B  dep 08:00 arr 10:00   distance=3000
        # Leg 2: B → C  dep 12:00 arr 14:00   distance=2000
        # Leg 3: C → A  dep 16:00 arr 18:00   distance=2000
        #
        # Round-trip detected when searching A → A.
        # B (at longitude=90) is farthest from A (at longitude=0) → split_idx=1 (A→B outbound)
        # Outbound: A→B (1 leg, 0 stops)
        # Return:   B→C→A (2 legs, 1 stop)

        a_stn = GraphStation(_stn_rec("AAA", "US", "NAM"; latitude=0.0, longitude=0.0))
        b_stn = GraphStation(_stn_rec("BBB", "US", "NAM"; latitude=0.0, longitude=90.0))
        c_stn = GraphStation(_stn_rec("CCC", "US", "NAM"; latitude=0.0, longitude=45.0))

        rec_ab = _leg_rec(
            carrier="UA", flight_number=1,
            departure_station="AAA", arrival_station="BBB",
            passenger_departure_time=Int16(480), passenger_arrival_time=Int16(600),
            distance=3000.0f0,
        )
        rec_bc = _leg_rec(
            carrier="UA", flight_number=2,
            departure_station="BBB", arrival_station="CCC",
            passenger_departure_time=Int16(720), passenger_arrival_time=Int16(840),
            distance=2000.0f0,
        )
        rec_ca = _leg_rec(
            carrier="UA", flight_number=3,
            departure_station="CCC", arrival_station="AAA",
            passenger_departure_time=Int16(960), passenger_arrival_time=Int16(1080),
            distance=2000.0f0,
        )

        leg_ab = GraphLeg(rec_ab, a_stn, b_stn)
        leg_bc = GraphLeg(rec_bc, b_stn, c_stn)
        leg_ca = GraphLeg(rec_ca, c_stn, a_stn)

        push!(a_stn.departures, leg_ab)
        push!(b_stn.arrivals, leg_ab)
        push!(b_stn.departures, leg_bc)
        push!(c_stn.arrivals, leg_bc)
        push!(c_stn.departures, leg_ca)
        push!(a_stn.arrivals, leg_ca)

        rt_constraints = SearchConstraints(
            defaults=ParameterSet(
                max_stops=Int16(3),
                domestic_circuity_extra_miles=500_000.0,
                max_circuity=50.0,
            ),
        )
        cnx_ctx = (
            config=SearchConfig(interline=INTERLINE_ALL),
            constraints=rt_constraints,
            build_stats=BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache=Dict{MCTCacheKey,MCTResult}(),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
            target_date=UInt32(0),
            mct_selections=MCTSelectionRow[],
        )
        cnx_rules = build_cnx_rules(
            SearchConfig(interline=INTERLINE_ALL), rt_constraints, MCTLookup()
        )
        rt_stations = Dict{StationCode,GraphStation}(
            StationCode("AAA") => a_stn,
            StationCode("BBB") => b_stn,
            StationCode("CCC") => c_stn,
        )
        build_connections!(rt_stations, cnx_rules, cnx_ctx)

        # With allow_roundtrips=false (default): A→A returns empty
        ctx_no_rt = RuntimeContext(
            config=SearchConfig(interline=INTERLINE_ALL, allow_roundtrips=false),
            constraints=rt_constraints,
            itn_rules=build_itn_rules(SearchConfig(interline=INTERLINE_ALL)),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
        )
        itns_no_rt = search_itineraries(
            rt_stations, StationCode("AAA"), StationCode("AAA"),
            Date(2026, 6, 15), ctx_no_rt,
        )
        @test isempty(itns_no_rt)

        # With allow_roundtrips=true: A→A returns split halves
        ctx_rt = RuntimeContext(
            config=SearchConfig(interline=INTERLINE_ALL, allow_roundtrips=true),
            constraints=rt_constraints,
            itn_rules=build_itn_rules(SearchConfig(interline=INTERLINE_ALL)),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
        )
        itns_rt = search_itineraries(
            rt_stations, StationCode("AAA"), StationCode("AAA"),
            Date(2026, 6, 15), ctx_rt,
        )
        @test length(itns_rt) >= 2

        # Each committed half should have a non-circular origin/destination
        # (the split halves should not themselves be round-trips).
        # Use to_leg.org as the true departure station (to_leg is always the
        # departing leg, even for nonstop self-connections).
        for itn in itns_rt
            if !isempty(itn.connections)
                first_org_code = itn.connections[1].to_leg.org.code
                last_cp = itn.connections[end]
                last_dst_code = (
                    last_cp.to_leg === last_cp.from_leg ?
                        last_cp.from_leg.dst.code :
                        last_cp.to_leg.dst.code
                )
                @test first_org_code != last_dst_code
            end
        end

        # Verify that B (farthest from A) appears as the destination of at least
        # one committed half — the outbound A→B half
        committed_dsts = [
            begin
                last_cp = itn.connections[end]
                last_cp.to_leg === last_cp.from_leg ?
                    last_cp.from_leg.dst.code : last_cp.to_leg.dst.code
            end
            for itn in itns_rt if !isempty(itn.connections)
        ]
        @test StationCode("BBB") in committed_dsts
    end

    # ── RuntimeContext default construction ───────────────────────────────────

    @testset "RuntimeContext default construction" begin
        ctx = RuntimeContext()
        @test ctx.config isa SearchConfig
        @test ctx.constraints isa SearchConstraints
        @test isempty(ctx.cnx_rules)
        @test isempty(ctx.itn_rules)
        @test ctx.target_date == UInt32(0)
        @test ctx.target_dow == StatusBits(0)
        @test ctx.utc_dep_origin == Int32(0)
        @test ctx._max_elapsed_threshold == Int32(2160)
        @test ctx._circuity_threshold == 2.5
        @test isempty(ctx.results)
        @test ctx.search_stats isa SearchStats
        @test ctx.build_stats isa BuildStats
    end

    # ── Elapsed-time DFS pruning ───────────────────────────────────────────────

    @testset "Elapsed-time DFS pruning" begin
        # Build the standard 3-station graph (JFK→ORD→LHR).
        # All UTC offsets are 0, so local times equal UTC.
        #
        # Leg 1: JFK dep 08:00 (480 min), ORD arr 10:00 (600 min)  block=120 min
        # Leg 2: ORD dep 12:00 (720 min), LHR arr 22:00 (1320 min) block=600 min
        # Total elapsed (UTC):
        #   utc_dep_origin = 480
        #   next_utc_arr of LHR = 1320
        #   est_elapsed = 1320 - 480 = 840 min
        #
        # With max_elapsed=480 → _max_elapsed_threshold = round(1.5*480) = 720
        # 840 > 720 → the connecting leg to LHR is pruned → only nonstop survives.
        #
        # Nonstop JFK→LHR: dep 09:00 (540), arr 21:00 (1260)
        #   est_elapsed = 1260 - 480 = 780 — but the nonstop is handled via the
        #   departure loop (not _dfs!), so it is never subject to the _dfs! prune.
        # Therefore: tight max_elapsed prunes the 1-stop but nonstop is unaffected.
        stations, _, _, _, _, _, _ = _build_graph()

        tight_ps = ParameterSet(
            max_stops=Int16(2),
            max_elapsed=Int32(480),              # tight: 8h max elapsed
            domestic_circuity_extra_miles=50_000.0,
            max_circuity=10.0,                   # generous: don't prune on circuity
        )
        ctx_tight = RuntimeContext(
            config=SearchConfig(scope=SCOPE_ALL, interline=INTERLINE_ALL),
            constraints=SearchConstraints(defaults=tight_ps),
            itn_rules=build_itn_rules(SearchConfig(scope=SCOPE_ALL, interline=INTERLINE_ALL)),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
        )

        itns_tight = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx_tight,
        )

        # Threshold = round(1.5*480) = 720; est_elapsed for 1-stop leg = 840 > 720
        @test ctx_tight._max_elapsed_threshold == Int32(720)
        # 1-stop itineraries must be absent (pruned by elapsed-time check)
        @test !any(i -> i.num_stops == Int16(1), itns_tight)

        # Verify with a generous threshold: 1-stop reappears
        loose_ps = ParameterSet(
            max_stops=Int16(2),
            max_elapsed=Int32(1440),             # default 24h
            domestic_circuity_extra_miles=50_000.0,
            max_circuity=10.0,
        )
        ctx_loose = RuntimeContext(
            config=SearchConfig(scope=SCOPE_ALL, interline=INTERLINE_ALL),
            constraints=SearchConstraints(defaults=loose_ps),
            itn_rules=build_itn_rules(SearchConfig(scope=SCOPE_ALL, interline=INTERLINE_ALL)),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
        )

        itns_loose = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx_loose,
        )
        @test any(i -> i.num_stops == Int16(1), itns_loose)
    end

    # ── Circuity DFS pruning ───────────────────────────────────────────────────

    @testset "Circuity DFS pruning" begin
        # Construct a graph where the connecting leg has an enormous distance so
        # that adding it would push the candidate circuity above the threshold.
        #
        # Market: JFK → LHR, great-circle ≈ 3,451 mi (computed at search time).
        # Leg 1: JFK → ORD  distance = 800 mi  (same as standard graph)
        # Leg 2: ORD → LHR  distance = 999_999 mi  (absurdly large)
        #
        # candidate_dist = 800 + 999_999 = 1,000,799 mi
        # candidate_circ = 1_000_799 / ~3451 ≈ 290 >> 2.5 → pruned
        #
        # With max_circuity=2.5 the 1-stop should be pruned.
        # Nonstop (JFK→LHR distance=3451) has circuity ≈ 1.0 → survives.

        jfk_stn = GraphStation(
            _stn_rec("JFK", "US", "NAM"; latitude=JFK_LAT, longitude=JFK_LNG, city="NYC", state="NY")
        )
        ord_stn = GraphStation(
            _stn_rec("ORD", "US", "NAM"; latitude=ORD_LAT, longitude=ORD_LNG, city="CHI", state="IL")
        )
        lhr_stn = GraphStation(
            _stn_rec("LHR", "GB", "EUR"; latitude=LHR_LAT, longitude=LHR_LNG, city="LON")
        )

        rec1 = _leg_rec(
            carrier="UA", flight_number=200,
            departure_station="JFK", arrival_station="ORD",
            passenger_departure_time=Int16(480), passenger_arrival_time=Int16(600),
            distance=800.0f0,
        )
        # Leg 2 has absurdly large distance to force candidate_circ >> threshold
        rec2 = _leg_rec(
            carrier="UA", flight_number=916,
            departure_station="ORD", arrival_station="LHR",
            passenger_departure_time=Int16(720), passenger_arrival_time=Int16(1320),
            distance=999_999.0f0,
        )
        rec_ns = _leg_rec(
            carrier="UA", flight_number=100,
            departure_station="JFK", arrival_station="LHR",
            passenger_departure_time=Int16(540), passenger_arrival_time=Int16(1260),
            distance=3451.0f0,
        )

        leg1   = GraphLeg(rec1,   jfk_stn, ord_stn)
        leg2   = GraphLeg(rec2,   ord_stn, lhr_stn)
        leg_ns = GraphLeg(rec_ns, jfk_stn, lhr_stn)

        push!(jfk_stn.departures, leg1)
        push!(jfk_stn.departures, leg_ns)
        push!(ord_stn.arrivals,   leg1)
        push!(ord_stn.departures, leg2)
        push!(lhr_stn.arrivals,   leg2)
        push!(lhr_stn.arrivals,   leg_ns)

        constraints = SearchConstraints(
            defaults=ParameterSet(
                max_stops=Int16(2),
                max_elapsed=Int32(99_999),       # generous: don't prune on elapsed
                domestic_circuity_extra_miles=50_000.0,
                max_circuity=2.5,                # tight circuity threshold
            )
        )
        cnx_ctx = (
            config=SearchConfig(interline=INTERLINE_ALL),
            constraints=constraints,
            build_stats=BuildStats(rule_pass=zeros(Int64, 9), rule_fail=zeros(Int64, 9)),
            mct_cache=Dict{MCTCacheKey,MCTResult}(),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
            target_date=UInt32(0),
            mct_selections=MCTSelectionRow[],
        )
        cnx_rules = build_cnx_rules(SearchConfig(interline=INTERLINE_ALL), constraints, MCTLookup())

        stations = Dict{StationCode,GraphStation}(
            StationCode("JFK") => jfk_stn,
            StationCode("ORD") => ord_stn,
            StationCode("LHR") => lhr_stn,
        )
        build_connections!(stations, cnx_rules, cnx_ctx)

        ctx = RuntimeContext(
            config=SearchConfig(scope=SCOPE_ALL, interline=INTERLINE_ALL),
            constraints=constraints,
            itn_rules=build_itn_rules(SearchConfig(scope=SCOPE_ALL, interline=INTERLINE_ALL)),
            gc_cache=Dict{Tuple{StationCode,StationCode},Float64}(),
        )

        itns = search_itineraries(
            stations, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15), ctx,
        )

        # Circuity threshold pre-computed correctly
        @test ctx._circuity_threshold == 2.5

        # 1-stop itineraries must be absent (pruned by circuity check)
        @test !any(i -> i.num_stops == Int16(1), itns)

        # Nonstop must still be found
        @test any(i -> i.num_stops == Int16(0), itns)
    end

end

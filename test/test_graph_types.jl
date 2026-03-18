using Test
using ItinerarySearch
using InlineStrings
using Dates

@testset "Graph Types" begin
    # Helper to create a minimal StationRecord
    function _test_station_record(code, country, region)
        StationRecord(
            code=StationCode(code),
            country=InlineString3(country),
            state=InlineString3(""),
            metro_area=InlineString3(""),
            region=InlineString3(region),
            lat=0.0,
            lng=0.0,
            utc_offset=Int16(0),
        )
    end

    # Helper to create a minimal LegRecord
    function _test_leg_record(;
        airline="UA",
        flt_no=100,
        org="ORD",
        dst="LHR",
        eqp="777",
        distance=3950.0f0,
        eff_date=Date(2026, 1, 1),
        disc_date=Date(2026, 12, 31),
        frequency=0x7f,
    )
        LegRecord(
            airline=AirlineCode(airline),
            flt_no=Int16(flt_no),
            operational_suffix=' ',
            itin_var=UInt8(1),
            itin_var_overflow=' ',
            leg_seq=UInt8(1),
            svc_type='J',
            org=StationCode(org),
            dst=StationCode(dst),
            pax_dep=Int16(540),
            pax_arr=Int16(1200),
            ac_dep=Int16(530),
            ac_arr=Int16(1190),
            dep_utc_offset=Int16(-360),
            arr_utc_offset=Int16(0),
            dep_date_var=Int8(0),
            arr_date_var=Int8(0),
            eqp=InlineString7(eqp),
            body_type='W',
            dep_term=InlineString3("1"),
            arr_term=InlineString3("5"),
            aircraft_owner=AirlineCode(airline),
            operating_date=pack_date(eff_date),
            day_of_week=UInt8(0),
            eff_date=pack_date(eff_date),
            disc_date=pack_date(disc_date),
            frequency=UInt8(frequency),
            mct_status_dep='D',
            mct_status_arr='I',
            trc=InlineString15(""),
            trc_overflow=' ',
            record_serial=UInt32(12345),
            row_number=UInt64(1),
            segment_hash=UInt64(99999),
            distance=Distance(distance),
            codeshare_airline=AirlineCode(""),
            codeshare_flt_no=Int16(0),
            dei_10=InlineString31(""),
            wet_lease=false,
            dei_127=InlineString31(""),
            prbd=InlineString31(""),
        )
    end

    @testset "GraphStation construction" begin
        rec = _test_station_record("ORD", "US", "NAM")
        stn = GraphStation(rec)
        @test stn.code == StationCode("ORD")
        @test stn.region == InlineString3("NAM")
        @test stn.country == InlineString3("US")
        @test isempty(stn.departures)
        @test isempty(stn.arrivals)
        @test isempty(stn.connections)
        @test stn.stats.num_departures == 0
    end

    @testset "GraphStation @kwdef defaults" begin
        stn = GraphStation()
        @test stn.code == NO_STATION
        @test isempty(stn.departures)
    end

    @testset "GraphLeg construction" begin
        org_stn = GraphStation(_test_station_record("ORD", "US", "NAM"))
        dst_stn = GraphStation(_test_station_record("LHR", "GB", "EUR"))
        leg_rec = _test_leg_record()
        leg = GraphLeg(leg_rec, org_stn, dst_stn)
        @test leg.record === leg_rec
        @test leg.org === org_stn
        @test leg.dst === dst_stn
        @test isempty(leg.connect_to)
        @test isempty(leg.connect_from)
    end

    @testset "GraphSegment construction" begin
        seg = GraphSegment()
        @test seg.operating_airline == NO_AIRLINE
        @test !seg.is_codeshare
        @test isempty(seg.legs)
    end

    @testset "GraphConnection — nonstop self-connection" begin
        org_stn = GraphStation(_test_station_record("ORD", "US", "NAM"))
        dst_stn = GraphStation(_test_station_record("LHR", "GB", "EUR"))
        leg = GraphLeg(_test_leg_record(), org_stn, dst_stn)
        cp = nonstop_connection(leg, org_stn)
        @test cp.from_leg === cp.to_leg   # self-connection
        @test cp.from_leg === leg
        @test cp.cnx_time == Minutes(0)
        @test cp.mct == Minutes(0)
        @test cp.station === org_stn
        @test cp.valid_from == pack_date(Date(2026, 1, 1))
        @test cp.valid_to == pack_date(Date(2026, 12, 31))
        @test cp.valid_days == UInt8(0x7f)   # all days
    end

    @testset "GraphConnection — real connection" begin
        stn = GraphStation(_test_station_record("ORD", "US", "NAM"))
        org1 = GraphStation(_test_station_record("JFK", "US", "NAM"))
        dst1 = GraphStation(_test_station_record("LHR", "GB", "EUR"))
        leg1 = GraphLeg(_test_leg_record(org="JFK", dst="ORD"), org1, stn)
        leg2 = GraphLeg(_test_leg_record(org="ORD", dst="LHR"), stn, dst1)
        cp = GraphConnection(
            from_leg=leg1,
            to_leg=leg2,
            station=stn,
            cnx_time=Minutes(90),
            mct=Minutes(60),
            mxct=Minutes(480),
        )
        @test cp.from_leg === leg1
        @test cp.to_leg === leg2
        @test cp.cnx_time == Minutes(90)
        @test cp.mct == Minutes(60)
        @test !(cp.from_leg === cp.to_leg)   # not a nonstop
    end

    @testset "Itinerary construction" begin
        itn = Itinerary()
        @test isempty(itn.connections)
        @test itn.status == StatusBits(0)
        @test itn.num_stops == Int16(0)
        @test itn.num_eqp_changes == Int16(0)
        @test itn.elapsed_time == Int32(0)
        @test itn.total_distance == Distance(0)
        @test itn.market_distance == Distance(0)
        @test itn.circuity == Float32(0)
        @test itn.num_metros == Int16(0)
        @test itn.num_countries == Int16(0)
    end
end

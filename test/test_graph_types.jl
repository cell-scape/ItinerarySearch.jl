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
            city=InlineString3(""),
            region=InlineString3(region),
            latitude=0.0,
            longitude=0.0,
            utc_offset=Int16(0),
        )
    end

    # Helper to create a minimal LegRecord
    function _test_leg_record(;
        carrier="UA",
        flight_number=100,
        departure_station="ORD",
        arrival_station="LHR",
        aircraft_type="777",
        distance=3950.0f0,
        effective_date=Date(2026, 1, 1),
        discontinue_date=Date(2026, 12, 31),
        frequency=0x7f,
    )
        LegRecord(
            carrier=AirlineCode(carrier),
            flight_number=Int16(flight_number),
            operational_suffix=' ',
            itinerary_var_id=UInt8(1),
            itinerary_var_overflow=' ',
            leg_sequence_number=UInt8(1),
            service_type='J',
            departure_station=StationCode(departure_station),
            arrival_station=StationCode(arrival_station),
            passenger_departure_time=Int16(540),
            passenger_arrival_time=Int16(1200),
            aircraft_departure_time=Int16(530),
            aircraft_arrival_time=Int16(1190),
            departure_utc_offset=Int16(-360),
            arrival_utc_offset=Int16(0),
            departure_date_variation=Int8(0),
            arrival_date_variation=Int8(0),
            aircraft_type=InlineString7(aircraft_type),
            body_type='W',
            departure_terminal=InlineString3("1"),
            arrival_terminal=InlineString3("5"),
            aircraft_owner=AirlineCode(carrier),
            operating_date=pack_date(effective_date),
            day_of_week=UInt8(0),
            effective_date=pack_date(effective_date),
            discontinue_date=pack_date(discontinue_date),
            frequency=UInt8(frequency),
            dep_intl_dom='D',
            arr_intl_dom='I',
            traffic_restriction_for_leg=InlineString15(""),
            traffic_restriction_overflow=' ',
            record_serial=UInt32(12345),
            row_number=UInt64(1),
            segment_hash=UInt64(99999),
            distance=Distance(distance),
            operating_carrier=AirlineCode(""),
            operating_flight_number=Int16(0),
            dei_10="",
            wet_lease=false,
            dei_127="",
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
        leg1 = GraphLeg(_test_leg_record(departure_station="JFK", arrival_station="ORD"), org1, stn)
        leg2 = GraphLeg(_test_leg_record(departure_station="ORD", arrival_station="LHR"), stn, dst1)
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

using Test
using ItinerarySearch
using InlineStrings
using Dates

include("test_helpers.jl")

@testset "ItinerarySearch" begin
    @testset "Module loads" begin
        @test true  # Module loaded successfully
    end

    @testset "Type Aliases" begin
        # Aliases are concrete types, not abstract
        @test StationCode === InlineString3
        @test AirlineCode === InlineString3
        @test FlightNumber === Int16
        @test Minutes === Int16
        @test Distance === Float32
        @test StatusBits === UInt16

        # isbits (stack-allocated, no GC pressure)
        @test isbitstype(StationCode)
        @test isbitstype(AirlineCode)

        # Construction and comparison
        stn = StationCode("ORD")
        @test stn == InlineString7("ORD")
        @test sizeof(StationCode) == 4  # InlineString3 is 4 bytes (3 chars + length byte)
    end

    @testset "Enums" begin
        # MCTStatus: 1-indexed to match TripBuilder and SSIM8 array indices
        @test Int8(MCT_DD) == 1
        @test Int8(MCT_II) == 4

        # Cabin
        @test Int8(CABIN_J) == 0
        @test Int8(CABIN_Y) == 2

        # ScopeMode
        @test Int8(SCOPE_ALL) == 0

        # InterlineMode
        @test Int8(INTERLINE_ONLINE) == 0
        @test Int8(INTERLINE_ALL) == 2

        # parse_mct_status
        @test parse_mct_status("DD") == MCT_DD
        @test parse_mct_status("II") == MCT_II
        @test_throws ErrorException parse_mct_status("XX")
    end

    include("test_jet_aqua.jl")
    include("test_status.jl")
    include("test_stats.jl")
    include("test_graph_types.jl")
    include("test_constraints.jl")
    include("test_config.jl")
    include("test_compression.jl")
    include("test_ingest.jl")
    include("test_store.jl")
    include("test_schedule_queries.jl")
    include("test_mct_lookup.jl")
    include("test_rules_cnx.jl")
    include("test_rules_itn.jl")
    include("test_connect.jl")
    include("test_search.jl")
    include("test_builder.jl")
    include("test_formats.jl")
    include("test_integration_graph.jl")
    include("test_instrumentation.jl")
    include("test_observe.jl")
    include("test_logging.jl")
    include("test_cli.jl")
    include("test_server.jl")
    include("test_newssim_ingest.jl")

    @testset "SSIM Parsing Helpers" begin
        using ItinerarySearch: parse_ddmonyy, parse_hhmm, parse_frequency_bitmask
        using ItinerarySearch: parse_date_var, parse_utc_offset, parse_serial

        @testset "Date parsing" begin
            @test parse_ddmonyy("01JAN26") == Date(2026, 1, 1)
            @test parse_ddmonyy("31DEC25") == Date(2025, 12, 31)
            @test parse_ddmonyy("15JUN26") == Date(2026, 6, 15)
        end

        @testset "Time parsing" begin
            @test parse_hhmm("0900") == Int16(540)
            @test parse_hhmm("0000") == Int16(0)
            @test parse_hhmm("2359") == Int16(1439)
            @test parse_hhmm("2400") == Int16(0)
            @test parse_hhmm("    ") == Int16(0)
        end

        @testset "Frequency bitmask" begin
            @test parse_frequency_bitmask("1234567") == UInt8(0b1111111)
            @test parse_frequency_bitmask("1 3 5 7") == UInt8(0b1010101)
            @test parse_frequency_bitmask("      7") == UInt8(0b1000000)
            @test parse_frequency_bitmask(" 2     ") == UInt8(0b0000010)
        end

        @testset "Date variation" begin
            @test parse_date_var("0") == Int8(0)
            @test parse_date_var("1") == Int8(1)
            @test parse_date_var("2") == Int8(2)
            @test parse_date_var("A") == Int8(-1)
            @test parse_date_var(" ") == Int8(0)
        end

        @testset "UTC offset" begin
            @test parse_utc_offset("+0500") == Int16(300)
            @test parse_utc_offset("-0600") == Int16(-360)
            @test parse_utc_offset("+0000") == Int16(0)
        end

        @testset "Serial number" begin
            @test parse_serial("000001") == UInt32(1)
            @test parse_serial("      ") == UInt32(0)
        end
    end

    @testset "Record Types" begin
        @testset "LegRecord" begin
            # dei_10/dei_127 are String (variable-length), so LegRecord is not isbits
            @test !isbitstype(LegRecord)
            # Fieldcount matches spec
            @test fieldcount(LegRecord) == 41

            # Construction via keyword constructor
            leg = LegRecord(
                carrier = AirlineCode("UA"),
                flight_number = Int16(1234),
                operational_suffix = ' ',
                itinerary_var_id = UInt8(1),
                itinerary_var_overflow = ' ',
                leg_sequence_number = UInt8(1),
                service_type = 'J',
                departure_station = StationCode("ORD"),
                arrival_station = StationCode("LHR"),
                passenger_departure_time = Int16(540),
                passenger_arrival_time = Int16(1320),
                aircraft_departure_time = Int16(535),
                aircraft_arrival_time = Int16(1325),
                departure_utc_offset = Int16(-360),
                arrival_utc_offset = Int16(0),
                departure_date_variation = Int8(0),
                arrival_date_variation = Int8(0),
                aircraft_type = InlineString7("789"),
                body_type = 'W',
                departure_terminal = InlineString3("1"),
                arrival_terminal = InlineString3("2"),
                aircraft_owner = AirlineCode("UA"),
                operating_date = UInt32(20260615),
                day_of_week = UInt8(1),
                effective_date = UInt32(20260601),
                discontinue_date = UInt32(20261031),
                frequency = UInt8(0b1111111),
                dep_intl_dom = 'D',
                arr_intl_dom = 'I',
                traffic_restriction_for_leg = InlineString15(""),
                traffic_restriction_overflow = ' ',
                record_serial = UInt32(1),
                row_number = UInt64(1),
                segment_hash = UInt64(0),
                distance = Float32(3941.0),
                administrating_carrier = AirlineCode(""),
                administrating_carrier_flight_number = Int16(0),
                dei_10 = "",
                wet_lease = false,
                dei_127 = "",
                prbd = InlineString31("JCDZPY"),
            )
            @test leg.carrier == AirlineCode("UA")
            @test leg.flight_number == Int16(1234)
            @test leg.departure_station == StationCode("ORD")
            @test leg.arrival_station == StationCode("LHR")
        end

        @testset "Display functions" begin
            leg = LegRecord(
                carrier = AirlineCode("UA"),
                flight_number = Int16(354),
                operational_suffix = ' ',
                itinerary_var_id = UInt8(1),
                itinerary_var_overflow = ' ',
                leg_sequence_number = UInt8(2),
                service_type = 'J',
                departure_station = StationCode("ORD"),
                arrival_station = StationCode("LHR"),
                passenger_departure_time = Int16(0), passenger_arrival_time = Int16(0),
                aircraft_departure_time = Int16(0), aircraft_arrival_time = Int16(0),
                departure_utc_offset = Int16(0), arrival_utc_offset = Int16(0),
                departure_date_variation = Int8(0), arrival_date_variation = Int8(0),
                aircraft_type = InlineString7(""), body_type = ' ',
                departure_terminal = InlineString3(""), arrival_terminal = InlineString3(""),
                aircraft_owner = AirlineCode(""),
                operating_date = UInt32(0), day_of_week = UInt8(0),
                effective_date = UInt32(0), discontinue_date = UInt32(0),
                frequency = UInt8(0),
                dep_intl_dom = ' ', arr_intl_dom = ' ',
                traffic_restriction_for_leg = InlineString15(""), traffic_restriction_overflow = ' ',
                record_serial = UInt32(0), row_number = UInt64(0),
                segment_hash = UInt64(0), distance = Float32(0),
                administrating_carrier = AirlineCode(""),
                administrating_carrier_flight_number = Int16(0),
                dei_10 = "", wet_lease = false,
                dei_127 = "",
                prbd = InlineString31(""),
            )
            @test flight_id(leg) == "UA 354"
            @test segment_id(leg) == "UA 354/1/J"
            @test full_id(leg) == "UA 354/1/J/L02"
        end

        @testset "StationRecord" begin
            @test isbitstype(StationRecord)
            stn = StationRecord(
                code = StationCode("ORD"),
                country = InlineString3("US"),
                state = InlineString3("IL"),
                city = InlineString3("CHI"),
                region = InlineString3("NOA"),
                latitude = 41.9742,
                longitude = -87.9073,
                utc_offset = Int16(-360),
            )
            @test stn.code == StationCode("ORD")
            @test stn.country == InlineString3("US")
        end

        @testset "MCTResult" begin
            @test isbitstype(MCTResult)
            mct = MCTResult(
                time = Int16(90),
                queried_status = MCT_II,
                matched_status = MCT_II,
                suppressed = false,
                source = SOURCE_EXCEPTION,
                specificity = UInt32(100),
            )
            @test mct.time == Int16(90)
            @test mct.queried_status == MCT_II
            @test mct.suppressed == false
        end

        @testset "SegmentRecord" begin
            @test isbitstype(SegmentRecord)
            seg = SegmentRecord(
                segment_hash = UInt64(12345),
                carrier = AirlineCode("UA"),
                flight_number = Int16(1234),
                operational_suffix = ' ',
                itinerary_var_id = UInt8(1),
                itinerary_var_overflow = ' ',
                service_type = 'J',
                operating_date = UInt32(20260615),
                num_legs = UInt8(1),
                first_leg_seq = UInt8(1),
                last_leg_seq = UInt8(1),
                segment_departure_station = StationCode("ORD"),
                segment_arrival_station = StationCode("LHR"),
                flown_distance = Float32(3941.0),
                market_distance = Float32(3941.0),
                segment_circuity = Float32(1.0),
                segment_passenger_departure_time = Int16(540),
                segment_passenger_arrival_time = Int16(1320),
                segment_aircraft_departure_time = Int16(535),
                segment_aircraft_arrival_time = Int16(1325),
            )
            @test seg.num_legs == UInt8(1)
            @test seg.segment_circuity ≈ 1.0f0
        end

        @testset "pack_date / unpack_date" begin
            d = Date(2026, 6, 15)
            packed = pack_date(d)
            @test packed == UInt32(20260615)
            @test unpack_date(packed) == d
            # Round-trip
            @test unpack_date(pack_date(Date(2000, 1, 1))) == Date(2000, 1, 1)
            @test unpack_date(pack_date(Date(2099, 12, 31))) == Date(2099, 12, 31)
        end
    end

    @testset "AbstractStore Interface" begin
        @test isabstracttype(AbstractStore)
        @test hasmethod(load_schedule!, Tuple{AbstractStore, SearchConfig})
        @test hasmethod(query_legs, Tuple{AbstractStore, StationCode, StationCode, Date})
        @test hasmethod(query_station, Tuple{AbstractStore, StationCode})
        @test hasmethod(get_departures, Tuple{AbstractStore, StationCode, Date})
        @test hasmethod(get_arrivals, Tuple{AbstractStore, StationCode, Date})
        @test hasmethod(query_market_distance, Tuple{AbstractStore, StationCode, StationCode})
        @test hasmethod(query_segment, Tuple{AbstractStore, UInt64})
        @test hasmethod(query_segment_stops, Tuple{AbstractStore, UInt64})
        @test hasmethod(table_stats, Tuple{AbstractStore})
        @test hasmethod(query_schedule_legs, Tuple{AbstractStore, Date, Date})
        @test hasmethod(query_schedule_segments, Tuple{AbstractStore, Date, Date})
    end
end

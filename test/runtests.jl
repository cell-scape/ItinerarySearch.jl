using Test
using ItinerarySearch
using InlineStrings

@testset "ItinerarySearch" begin
    @testset "Module loads" begin
        @test true  # Module loaded successfully
    end

    @testset "Type Aliases" begin
        # Aliases are concrete types, not abstract
        @test StationCode === InlineString7
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
        @test sizeof(StationCode) == 8  # InlineString7 is 8 bytes (7 chars + length byte)
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
end

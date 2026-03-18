using Test
using ItinerarySearch

@testset "StatusBits" begin
    @testset "Day-of-week constants" begin
        @test DOW_MON == StatusBits(0x0001)
        @test DOW_SUN == StatusBits(0x0040)
        @test DOW_MASK == StatusBits(0x007f)
        all_days = DOW_MON | DOW_TUE | DOW_WED | DOW_THU | DOW_FRI | DOW_SAT | DOW_SUN
        @test all_days == DOW_MASK
    end

    @testset "Classification constants" begin
        @test STATUS_INTERNATIONAL == StatusBits(0x0080)
        @test STATUS_INTERLINE     == StatusBits(0x0100)
        @test STATUS_ROUNDTRIP     == StatusBits(0x0200)
        @test STATUS_CODESHARE     == StatusBits(0x0400)
        @test STATUS_THROUGH       == StatusBits(0x0800)
        @test STATUS_WETLEASE      == StatusBits(0x1000)
        # No overlaps with DOW
        @test (STATUS_INTERNATIONAL & DOW_MASK) == StatusBits(0)
    end

    @testset "Query helpers" begin
        s = STATUS_INTERNATIONAL | STATUS_CODESHARE | DOW_MON | DOW_FRI
        @test is_international(s)
        @test !is_interline(s)
        @test is_codeshare(s)
        @test !is_roundtrip(s)
        @test !is_through(s)
        @test !is_wetlease(s)
    end

    @testset "dow_bit" begin
        @test dow_bit(1) == DOW_MON
        @test dow_bit(7) == DOW_SUN
        @test dow_bit(3) == DOW_WED
    end

    @testset "Sentinel constants" begin
        @test WILDCARD_STATION == StationCode("*")
        @test WILDCARD_AIRLINE == AirlineCode("*")
        @test NO_STATION == StationCode("")
        @test NO_MINUTES == Minutes(-1)
        @test NO_DISTANCE == Distance(-1.0f0)
        # Wildcard != empty
        @test WILDCARD_STATION != NO_STATION
        @test WILDCARD_AIRLINE != NO_AIRLINE
    end
end

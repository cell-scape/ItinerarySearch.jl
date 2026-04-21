# test/test_ssim_parsing.jl — low-level SSIM scalar parsers:
# parse_ddmonyy / parse_hhmm / parse_frequency_bitmask / parse_date_var /
# parse_utc_offset / parse_serial.

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

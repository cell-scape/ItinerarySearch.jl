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
        @test STATUS_INTERNATIONAL  == StatusBits(0x0080)
        @test STATUS_INTERLINE      == StatusBits(0x0100)
        @test STATUS_ROUNDTRIP      == StatusBits(0x0200)
        @test STATUS_CODESHARE      == StatusBits(0x0400)
        @test STATUS_THROUGH        == StatusBits(0x0800)
        @test STATUS_WETLEASE       == StatusBits(0x1000)
        @test STATUS_CNX_OP_THROUGH == StatusBits(0x2000)
        # No overlaps with DOW
        @test (STATUS_INTERNATIONAL & DOW_MASK) == StatusBits(0)
        # New bit doesn't overlap any other classification bit
        for other in (STATUS_INTERNATIONAL, STATUS_INTERLINE, STATUS_ROUNDTRIP,
                      STATUS_CODESHARE, STATUS_THROUGH, STATUS_WETLEASE)
            @test (STATUS_CNX_OP_THROUGH & other) == StatusBits(0)
        end
    end

    @testset "Query helpers" begin
        s = STATUS_INTERNATIONAL | STATUS_CODESHARE | DOW_MON | DOW_FRI
        @test is_international(s)
        @test !is_interline(s)
        @test is_codeshare(s)
        @test !is_roundtrip(s)
        @test !is_through(s)
        @test !is_wetlease(s)
        @test !is_cnx_op_through(s)
    end

    @testset "Codeshare and interline are independent" begin
        # Per the user-facing semantics:
        #   STATUS_CODESHARE = at least one leg is per-leg codeshare (rolled up)
        #   STATUS_INTERLINE = marketing changed at this cnx
        # The two are independent bits and any combination is valid.
        codeshare_only = StatusBits(STATUS_CODESHARE)
        interline_only = StatusBits(STATUS_INTERLINE)
        both = StatusBits(STATUS_CODESHARE | STATUS_INTERLINE)
        neither = StatusBits(0)
        @test  is_codeshare(codeshare_only) && !is_interline(codeshare_only)
        @test !is_codeshare(interline_only) &&  is_interline(interline_only)
        @test  is_codeshare(both)            &&  is_interline(both)
        @test !is_codeshare(neither)         && !is_interline(neither)
    end

    @testset "is_cnx_op_through qualifier on STATUS_INTERLINE" begin
        # STATUS_CNX_OP_THROUGH is only meaningful when STATUS_INTERLINE is
        # set — it qualifies the kind of interline (codeshare-mediated vs
        # operating-different).  Used by INTERLINE_CODESHARE filter mode.
        op_through = StatusBits(STATUS_INTERLINE | STATUS_CNX_OP_THROUGH)
        op_diff    = StatusBits(STATUS_INTERLINE)
        @test is_interline(op_through)    && is_cnx_op_through(op_through)
        @test is_interline(op_diff)       && !is_cnx_op_through(op_diff)
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

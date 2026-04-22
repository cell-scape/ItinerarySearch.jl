using Test
using ItinerarySearch

@testset "Circuity Tiers" begin
    @testset "CircuityTier struct" begin
        t = CircuityTier(250.0, 2.4)
        @test t.max_distance == 250.0
        @test t.factor == 2.4
        @test isbitstype(CircuityTier)
    end

    @testset "DEFAULT_CIRCUITY_TIERS values" begin
        d = DEFAULT_CIRCUITY_TIERS
        @test length(d) == 4
        @test d[1] == CircuityTier(250.0, 2.4)
        @test d[2] == CircuityTier(800.0, 1.9)
        @test d[3] == CircuityTier(2000.0, 1.5)
        @test d[4] == CircuityTier(Inf, 1.3)
    end

    @testset "_validate_circuity_tiers" begin
        using ItinerarySearch: _validate_circuity_tiers
        _validate_circuity_tiers([CircuityTier(100.0, 2.0), CircuityTier(Inf, 1.5)])
        @test_throws ArgumentError _validate_circuity_tiers(CircuityTier[])
        @test_throws ArgumentError _validate_circuity_tiers(
            [CircuityTier(500.0, 2.0), CircuityTier(200.0, 1.5)]
        )
        @test_throws ArgumentError _validate_circuity_tiers(
            [CircuityTier(100.0, 0.0), CircuityTier(Inf, 1.5)]
        )
    end
end

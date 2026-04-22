using Test
using ItinerarySearch
using ItinerarySearch: _validate_circuity_tiers, _circuity_factor_at, _effective_circuity_factor, _resolve_circuity_params

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
        @test _validate_circuity_tiers([CircuityTier(100.0, 2.0), CircuityTier(Inf, 1.5)]) === nothing
        @test_throws ArgumentError _validate_circuity_tiers(CircuityTier[])
        @test_throws ArgumentError _validate_circuity_tiers(
            [CircuityTier(500.0, 2.0), CircuityTier(200.0, 1.5)]
        )
        @test_throws ArgumentError _validate_circuity_tiers(
            [CircuityTier(100.0, 0.0), CircuityTier(Inf, 1.5)]
        )
        @test_throws ArgumentError _validate_circuity_tiers(
            [CircuityTier(500.0, 2.0), CircuityTier(500.0, 1.5)]
        )
    end

    @testset "_circuity_factor_at" begin
        d = DEFAULT_CIRCUITY_TIERS
        @test _circuity_factor_at(d,    0.0) == 2.4
        @test _circuity_factor_at(d,  250.0) == 2.4  # inclusive upper bound
        @test _circuity_factor_at(d,  251.0) == 1.9
        @test _circuity_factor_at(d,  800.0) == 1.9
        @test _circuity_factor_at(d,  801.0) == 1.5
        @test _circuity_factor_at(d, 2000.0) == 1.5
        @test _circuity_factor_at(d, 2001.0) == 1.3
        @test _circuity_factor_at(d, 99999.0) == 1.3
        # Non-Inf top tier: fall back to last factor
        finite = [CircuityTier(1000.0, 2.0), CircuityTier(99999.0, 1.4)]
        @test _circuity_factor_at(finite, 1_000_000.0) == 1.4  # exceeded all, use last
    end

    @testset "_effective_circuity_factor" begin
        # Inf ceiling: tier factor wins
        p_inf = ParameterSet(max_circuity = Inf)  # tiers default to DEFAULT_CIRCUITY_TIERS
        @test _effective_circuity_factor(p_inf,  100.0) == 2.4
        @test _effective_circuity_factor(p_inf, 5000.0) == 1.3

        # Ceiling below tier factor: ceiling wins
        p_cap = ParameterSet(max_circuity = 2.0)
        @test _effective_circuity_factor(p_cap,  100.0) == 2.0  # tier 2.4 → clamped
        @test _effective_circuity_factor(p_cap,  500.0) == 1.9  # tier below ceiling
        @test _effective_circuity_factor(p_cap, 5000.0) == 1.3
    end

    @testset "_resolve_circuity_params" begin
        sc_empty = SearchConstraints()
        p = _resolve_circuity_params(sc_empty, StationCode("ORD"), StationCode("LHR"))
        @test p === sc_empty.defaults

        # With an O-D match override
        ovr = MarketOverride(
            origin = StationCode("ORD"), destination = StationCode("LHR"),
            carrier = WILDCARD_AIRLINE,
            params = ParameterSet(
                circuity_tiers = [CircuityTier(Inf, 1.6)],
            ),
            specificity = UInt32(1000),
        )
        sc = SearchConstraints(overrides = [ovr])
        p1 = _resolve_circuity_params(sc, StationCode("ORD"), StationCode("LHR"))
        @test p1.circuity_tiers == [CircuityTier(Inf, 1.6)]

        # Different O-D falls back to defaults
        p2 = _resolve_circuity_params(sc, StationCode("DEN"), StationCode("SFO"))
        @test p2 === sc.defaults

        # Carrier is deliberately ignored — a carrier-specific override still
        # matches on O-D only for circuity.
        ovr_carrier = MarketOverride(
            origin = StationCode("ATL"), destination = StationCode("YYZ"),
            carrier = AirlineCode("UA"),    # specific carrier
            params = ParameterSet(circuity_tiers = [CircuityTier(Inf, 2.7)]),
            specificity = UInt32(1000),
        )
        sc_c = SearchConstraints(overrides = [ovr_carrier])
        p3 = _resolve_circuity_params(sc_c, StationCode("ATL"), StationCode("YYZ"))
        @test p3.circuity_tiers == [CircuityTier(Inf, 2.7)]  # matches despite carrier mismatch
    end

    @testset "load_circuity_tiers — sample file" begin
        path = joinpath(@__DIR__, "..", "data", "demo", "cirOvrdDflt.dat")
        tiers = load_circuity_tiers(path)
        @test length(tiers) == 4
        @test tiers[1] == CircuityTier(250.0, 2.4)
        @test tiers[2] == CircuityTier(800.0, 1.9)
        @test tiers[3] == CircuityTier(2000.0, 1.5)
        @test tiers[4] == CircuityTier(99999.0, 1.3)
    end

    @testset "load_circuity_tiers — empty HIGH → Inf" begin
        path = tempname() * ".csv"
        write(path, "HIGH,CIRCUITY\n250,2.4\n,1.3\n")
        try
            tiers = load_circuity_tiers(path)
            @test tiers == [CircuityTier(250.0, 2.4), CircuityTier(Inf, 1.3)]
        finally
            rm(path; force=true)
        end
    end

    @testset "load_circuity_tiers — malformed inputs" begin
        # Missing column
        p1 = tempname() * ".csv"
        write(p1, "HIGH\n250\n")
        @test_throws ArgumentError load_circuity_tiers(p1)
        rm(p1; force=true)

        # Descending thresholds
        p2 = tempname() * ".csv"
        write(p2, "HIGH,CIRCUITY\n800,1.9\n250,2.4\n")
        @test_throws ArgumentError load_circuity_tiers(p2)
        rm(p2; force=true)

        # Non-positive factor
        p3 = tempname() * ".csv"
        write(p3, "HIGH,CIRCUITY\n250,0\n800,1.9\n")
        @test_throws ArgumentError load_circuity_tiers(p3)
        rm(p3; force=true)
    end

    @testset "load_circuity_overrides — sample file" begin
        path = joinpath(@__DIR__, "..", "data", "demo", "cirOvrd.dat")
        ovrs = load_circuity_overrides(path)
        @test length(ovrs) == 7  # match the known row count in the sample

        # Spot-check a row
        atl_yyz = first(o for o in ovrs if o.origin == StationCode("ATL") &&
                                              o.destination == StationCode("YYZ"))
        @test atl_yyz.carrier == WILDCARD_AIRLINE
        @test atl_yyz.params.circuity_tiers == [CircuityTier(Inf, 2.7)]
        @test atl_yyz.specificity == UInt32(1000)
    end

    @testset "load_circuity_overrides — validation" begin
        # Missing ENTNM
        p1 = tempname() * ".csv"
        write(p1, "ORG,DEST,ENTNM,CRTY\nATL,YYZ,,2.7\n")
        @test_throws ArgumentError load_circuity_overrides(p1)
        rm(p1; force=true)

        # Non-positive CRTY
        p2 = tempname() * ".csv"
        write(p2, "ORG,DEST,ENTNM,CRTY\nATL,YYZ,*,0\n")
        @test_throws ArgumentError load_circuity_overrides(p2)
        rm(p2; force=true)
    end

    @testset "apply_circuity_files!" begin
        demo = joinpath(@__DIR__, "..", "data", "demo")
        sc = SearchConstraints()

        sc = apply_circuity_files!(sc;
            defaults_path  = joinpath(demo, "cirOvrdDflt.dat"),
            overrides_path = joinpath(demo, "cirOvrd.dat"),
        )

        # Defaults now come from the CSV
        @test sc.defaults.circuity_tiers[1] == CircuityTier(250.0, 2.4)
        @test length(sc.defaults.circuity_tiers) == 4

        # Overrides appended, sorted by descending specificity (all specificity=1000 here)
        @test length(sc.overrides) == 7
        @test all(o -> o.carrier == WILDCARD_AIRLINE, sc.overrides)
        @test all(o -> o.specificity == UInt32(1000), sc.overrides)
    end
end

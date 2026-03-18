using Test
using ItinerarySearch
using InlineStrings

@testset "Constraints" begin
    @testset "ParameterSet defaults" begin
        p = ParameterSet()
        @test p.min_mct_override == NO_MINUTES
        @test p.max_stops == Int16(2)
        @test p.circuity_factor == 2.0
        @test isempty(p.valid_codeshare_partners)  # empty = allow all
        @test p.max_leg_distance == Distance(Inf32)
    end

    @testset "MarketOverride defaults" begin
        o = MarketOverride()
        @test o.origin == WILDCARD_STATION
        @test o.destination == WILDCARD_STATION
        @test o.carrier == WILDCARD_AIRLINE
        @test o.specificity == UInt32(0)
    end

    @testset "SearchConstraints defaults" begin
        sc = SearchConstraints()
        @test sc.defaults.max_stops == Int16(2)
        @test isempty(sc.overrides)
        @test isempty(sc.closed_stations)
        @test isempty(sc.delays)
    end

    @testset "resolve_params — no overrides" begin
        sc = SearchConstraints()
        p = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("UA"))
        @test p === sc.defaults
    end

    @testset "resolve_params — exact O-D match" begin
        override = MarketOverride(
            origin=StationCode("ORD"),
            destination=StationCode("LHR"),
            params=ParameterSet(circuity_factor=3.0),
            specificity=UInt32(100)
        )
        sc = SearchConstraints(overrides=[override])
        p = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("UA"))
        @test p.circuity_factor == 3.0
    end

    @testset "resolve_params — wildcard origin" begin
        override = MarketOverride(
            destination=StationCode("LHR"),  # origin defaults to WILDCARD
            params=ParameterSet(max_stops=Int16(3)),
            specificity=UInt32(50)
        )
        sc = SearchConstraints(overrides=[override])
        # Any origin to LHR should match
        p = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("UA"))
        @test p.max_stops == Int16(3)
        p2 = resolve_params(sc, StationCode("JFK"), StationCode("LHR"), AirlineCode("AA"))
        @test p2.max_stops == Int16(3)
    end

    @testset "resolve_params — no match falls through to default" begin
        override = MarketOverride(
            origin=StationCode("ORD"),
            destination=StationCode("LHR"),
            params=ParameterSet(circuity_factor=3.0),
        )
        sc = SearchConstraints(overrides=[override])
        # Different O-D should NOT match
        p = resolve_params(sc, StationCode("JFK"), StationCode("CDG"), AirlineCode("UA"))
        @test p.circuity_factor == 2.0  # default
    end

    @testset "resolve_params — carrier-specific override" begin
        override = MarketOverride(
            carrier=AirlineCode("UA"),
            params=ParameterSet(max_elapsed=Int32(2880)),
        )
        sc = SearchConstraints(overrides=[override])
        p_ua = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("UA"))
        @test p_ua.max_elapsed == Int32(2880)
        p_aa = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("AA"))
        @test p_aa.max_elapsed == Int32(1440)  # default
    end

    @testset "SearchConfig new fields" begin
        cfg = SearchConfig()
        @test cfg.leading_days == 2
        @test cfg.metrics_level == :full
        @test cfg.graph_export_path == "data/output"
        @test cfg.constraints_path == "data/output"
        @test cfg.event_log_path == "data/output"
        @test cfg.output_formats == [:json, :yaml, :csv]
    end

    @testset "Simulation controls" begin
        sc = SearchConstraints(
            closed_stations=Set([StationCode("ORD")]),
            delays=Dict(StationCode("JFK") => Minutes(120)),
        )
        @test StationCode("ORD") in sc.closed_stations
        @test sc.delays[StationCode("JFK")] == Minutes(120)
    end
end

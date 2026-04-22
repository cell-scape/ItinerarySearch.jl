using Test
using ItinerarySearch
using InlineStrings

@testset "Constraints" begin
    @testset "ParameterSet defaults" begin
        p = ParameterSet()
        @test p.min_mct_override == NO_MINUTES
        @test p.max_stops == Int16(2)
        @test p.circuity_tiers == DEFAULT_CIRCUITY_TIERS
        @test p.max_circuity == Inf
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
            params=ParameterSet(circuity_tiers=[CircuityTier(Inf, 3.0)]),
            specificity=UInt32(100)
        )
        sc = SearchConstraints(overrides=[override])
        p = resolve_params(sc, StationCode("ORD"), StationCode("LHR"), AirlineCode("UA"))
        @test p.circuity_tiers == [CircuityTier(Inf, 3.0)]
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
            params=ParameterSet(circuity_tiers=[CircuityTier(Inf, 3.0)]),
        )
        sc = SearchConstraints(overrides=[override])
        # Different O-D should NOT match
        p = resolve_params(sc, StationCode("JFK"), StationCode("CDG"), AirlineCode("UA"))
        @test p.circuity_tiers == DEFAULT_CIRCUITY_TIERS
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
        @test cfg.event_log_enabled == false
        @test cfg.event_log_path == "data/output/events.jsonl"
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

    # ── Dict constructors ──────────────────────────────────────────────────
    @testset "ParameterSet(dict) — scalar fields" begin
        p = ParameterSet(Dict(:max_stops => 1, :max_connection_time => 300))
        @test p.max_stops == Int16(1)
        @test p.max_connection_time == Int16(300)
    end

    @testset "ParameterSet(dict) — String keys" begin
        p = ParameterSet(Dict("max_stops" => 2, "min_circuity" => 0.5))
        @test p.max_stops == Int16(2)
        @test p.min_circuity == 0.5
    end

    @testset "ParameterSet(dict) — unknown key errors" begin
        @test_throws ArgumentError ParameterSet(Dict(:not_a_field => 1))
    end

    @testset "ParameterSet(dict) — empty dict yields defaults" begin
        p = ParameterSet(Dict{Symbol,Any}())
        default = ParameterSet()
        @test p.max_stops == default.max_stops
        @test p.min_connection_time == default.min_connection_time
    end

    @testset "MarketOverride(dict) — String station/carrier" begin
        m = MarketOverride(Dict(
            :origin => "ORD",
            :destination => "LHR",
            :carrier => "UA",
            :specificity => UInt32(100),
        ))
        @test m.origin == StationCode("ORD")
        @test m.destination == StationCode("LHR")
        @test m.carrier == AirlineCode("UA")
        @test m.specificity == UInt32(100)
    end

    @testset "MarketOverride(dict) — nested params AbstractDict" begin
        m = MarketOverride(Dict(
            :origin => "ORD",
            :destination => "LHR",
            :params => Dict(:max_stops => 1, :min_circuity => 0.8),
        ))
        @test m.params.max_stops == Int16(1)
        @test m.params.min_circuity == 0.8
    end

    @testset "MarketOverride(dict) — unknown key errors" begin
        @test_throws ArgumentError MarketOverride(Dict(:not_a_field => 1))
    end

    @testset "SearchConstraints(dict) — nested defaults dict" begin
        sc = SearchConstraints(Dict(
            :defaults => Dict(:max_stops => 1, :max_circuity => 1.8),
        ))
        @test sc.defaults.max_stops == Int16(1)
        @test sc.defaults.max_circuity == 1.8
    end

    @testset "SearchConstraints(dict) — overrides Vector{Dict}" begin
        sc = SearchConstraints(Dict(
            :overrides => [
                Dict(:origin => "ORD", :destination => "LHR",
                     :specificity => UInt32(100)),
                Dict(:origin => "DEN", :destination => "LAX",
                     :specificity => UInt32(200)),
            ],
        ))
        @test length(sc.overrides) == 2
        @test sc.overrides[1].origin == StationCode("ORD")
        @test sc.overrides[2].specificity == UInt32(200)
    end

    @testset "SearchConstraints(dict) — mixed MarketOverride and Dict elements" begin
        sc = SearchConstraints(Dict(
            :overrides => [
                MarketOverride(origin=StationCode("ORD"),
                               destination=StationCode("LHR"),
                               specificity=UInt32(100)),
                Dict(:origin => "DEN", :destination => "LAX",
                     :specificity => UInt32(200)),
            ],
        ))
        @test length(sc.overrides) == 2
        @test sc.overrides[2].origin == StationCode("DEN")
    end

    @testset "SearchConstraints(dict) — unknown key errors" begin
        @test_throws ArgumentError SearchConstraints(Dict(:bogus => 1))
    end
end

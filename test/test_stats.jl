using Test
using ItinerarySearch
using InlineStrings

@testset "Stats Types" begin
    @testset "StationStats defaults" begin
        s = StationStats()
        @test s.num_departures == 0
        @test s.num_connections == 0
        @test isempty(s.unique_carriers)
        @test isempty(s.unique_equipment)
        @test s.avg_ground_time == 0.0
    end

    @testset "BuildStats defaults" begin
        b = BuildStats()
        @test b.total_connections == 0
        @test isempty(b.rule_pass)
        @test length(b.mct_time_hist) == 48
        @test b.build_time_ns == UInt64(0)
    end

    @testset "SearchStats defaults" begin
        s = SearchStats()
        @test s.paths_found == 0
        @test length(s.paths_by_stops) == 4
        @test s.layer1_hits == 0
    end

    @testset "MCTSelectionRow is isbits" begin
        @test isbitstype(MCTSelectionRow)
    end

    @testset "merge_station_stats! additive fields" begin
        a = StationStats(num_connections=10, num_international=3, avg_ground_time=60.0)
        b = StationStats(num_connections=5, num_international=2, avg_ground_time=90.0)
        merge_station_stats!(a, b)
        @test a.num_connections == 15
        @test a.num_international == 5
        # Weighted average: (60*10 + 90*5) / 15 = 70.0
        @test a.avg_ground_time ≈ 70.0
    end

    @testset "merge_station_stats! set union" begin
        a = StationStats()
        push!(a.unique_carriers, AirlineCode("UA"))
        b = StationStats()
        push!(b.unique_carriers, AirlineCode("AA"))
        push!(b.unique_carriers, AirlineCode("UA"))
        merge_station_stats!(a, b)
        @test length(a.unique_carriers) == 2
    end

    @testset "merge_station_stats! both empty" begin
        a = StationStats()
        b = StationStats()
        merge_station_stats!(a, b)
        @test a.avg_ground_time == 0.0
        @test a.num_connections == 0
    end

    @testset "merge_build_stats! rule counters" begin
        a = BuildStats(rule_pass=[10, 20], rule_fail=[1, 2])
        b = BuildStats(rule_pass=[5, 10], rule_fail=[0, 3])
        merge_build_stats!(a, b)
        @test a.rule_pass == [15, 30]
        @test a.rule_fail == [1, 5]
    end

    @testset "merge_build_stats! mct counters" begin
        a = BuildStats(mct_lookups=100, mct_cache_hits=80)
        b = BuildStats(mct_lookups=50, mct_cache_hits=40)
        merge_build_stats!(a, b)
        @test a.mct_lookups == 150
        @test a.mct_cache_hits == 120
    end

    @testset "mct_avg_time weighted merge" begin
        a = BuildStats(
            mct_lookups = 100, mct_suppressions = 10, mct_avg_time = 60.0,
            rule_pass = Int64[0], rule_fail = Int64[0], mct_time_hist = zeros(Int64, 48),
        )
        b = BuildStats(
            mct_lookups = 50, mct_suppressions = 5, mct_avg_time = 90.0,
            rule_pass = Int64[0], rule_fail = Int64[0], mct_time_hist = zeros(Int64, 48),
        )
        merge_build_stats!(a, b)
        # Weighted: (60.0 * 90 + 90.0 * 45) / 135 = (5400 + 4050) / 135 = 70.0
        @test a.mct_avg_time ≈ 70.0
    end
end

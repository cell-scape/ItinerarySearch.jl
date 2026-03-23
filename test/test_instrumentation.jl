using Test
using ItinerarySearch
using DuckDB, DBInterface
using Dates
using InlineStrings

@testset "Tier 1 Instrumentation" begin

    @testset "Build stats populated" begin
        store = DuckDBStore()
        try
            # Insert two legs that create a connection opportunity at LHR
            DBInterface.execute(store.db, """
            INSERT INTO legs VALUES (
                1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
                'ORD', 'LHR', 540, 1320, 535, 1325,
                -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
                '2026-06-15', '2026-06-15', 127,
                'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
            )
            """)
            DBInterface.execute(store.db, """
            INSERT INTO legs VALUES (
                2, 1, 'UA', 5678, ' ', 1, ' ', 1, 'J',
                'LHR', 'CDG', 1500, 1620, 1495, 1625,
                0, 60, 0, 0, '2', '1', '320', 'N', 'UA',
                '2026-06-15', '2026-06-15', 127,
                'I', 'I', '', ' ', 'JCDZPY', 213.0, false
            )
            """)
            DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)")
            DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)")
            DBInterface.execute(store.db, "INSERT INTO stations VALUES ('CDG','FR','','PAR','EUR',49.0097,2.5479,60)")
            post_ingest_sql!(store)

            config = SearchConfig()
            graph = build_graph!(store, config, Date(2026, 6, 15))

            bs = graph.build_stats
            @test bs.total_stations > 0
            @test bs.total_legs > 0
            @test bs.total_connections >= 0
            @test bs.total_pairs_evaluated >= 0
            @test bs.build_time_ns > UInt64(0)

            # Rule vectors should be pre-sized
            @test length(bs.rule_pass) > 0
            @test length(bs.rule_fail) > 0

            # MCT lookups should have happened if pairs were evaluated
            if bs.total_pairs_evaluated > 0
                @test bs.mct_lookups > 0
                # At least one cascade source should be non-zero
                @test (bs.mct_exceptions + bs.mct_standards + bs.mct_defaults + bs.mct_suppressions) > 0
            end

            # Geo stats populated
            @test length(graph.geo_stats.by_country) > 0
            @test length(graph.geo_stats.by_region) > 0
            @test haskey(graph.geo_stats.by_country, InlineString3("US"))
        finally
            close(store)
        end
    end

    @testset "Search stats populated" begin
        store = DuckDBStore()
        try
            DBInterface.execute(store.db, """
            INSERT INTO legs VALUES (
                1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
                'ORD', 'LHR', 540, 1320, 535, 1325,
                -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
                '2026-06-15', '2026-06-15', 127,
                'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
            )
            """)
            DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)")
            DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)")
            post_ingest_sql!(store)

            config = SearchConfig()
            graph = build_graph!(store, config, Date(2026, 6, 15))

            ctx = RuntimeContext(
                config = config,
                constraints = SearchConstraints(),
                itn_rules = build_itn_rules(config),
            )

            search_itineraries(graph.stations, StationCode("ORD"), StationCode("LHR"),
                               Date(2026, 6, 15), ctx)

            ss = ctx.search_stats
            @test ss.queries >= Int32(1)
            @test ss.search_time_ns > UInt64(0)

            # If we found paths, histograms should have entries
            if ss.paths_found > 0
                @test sum(ss.elapsed_time_hist) > 0
                @test sum(ss.total_distance_hist) > 0
                @test sum(ss.paths_by_stops) > 0
            end
        finally
            close(store)
        end
    end

    @testset "MCTSelectionRow gating" begin
        # At :basic, mct_selections should remain empty on ctx
        ctx_basic = RuntimeContext(
            config = SearchConfig(metrics_level = :basic),
            constraints = SearchConstraints(),
        )
        @test ctx_basic.mct_selections isa Vector{MCTSelectionRow}
        @test isempty(ctx_basic.mct_selections)

        # At :full, the vector is ready to receive rows
        ctx_full = RuntimeContext(
            config = SearchConfig(metrics_level = :full),
            constraints = SearchConstraints(),
        )
        @test ctx_full.mct_selections isa Vector{MCTSelectionRow}
        @test isempty(ctx_full.mct_selections)  # empty until build_connections! runs
    end

end

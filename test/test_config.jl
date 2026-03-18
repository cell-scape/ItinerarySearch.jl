using Test
using ItinerarySearch
using ItinerarySearch: _default_path

@testset "SearchConfig" begin
    @testset "Default construction" begin
        cfg = SearchConfig()
        @test cfg.backend == "duckdb"
        @test cfg.db_path == ":memory:"
        @test cfg.max_stops == 2
        @test cfg.max_connection_minutes == 480
        @test cfg.max_elapsed_minutes == 1440
        @test cfg.circuity_factor == 2.5
        @test cfg.circuity_extra_miles == 500.0
        @test cfg.scope == SCOPE_ALL
        @test cfg.interline == INTERLINE_CODESHARE
        @test cfg.max_days == 1
        @test cfg.trailing_days == 0
    end

    @testset "Immutability" begin
        cfg = SearchConfig()
        @test !ismutable(cfg)
    end

    @testset "Custom construction" begin
        cfg = SearchConfig(max_stops=3, scope=SCOPE_INTL)
        @test cfg.max_stops == 3
        @test cfg.scope == SCOPE_INTL
        @test cfg.backend == "duckdb"
    end

    @testset "JSON loading" begin
        json_path = joinpath(@__DIR__, "test_config.json")
        open(json_path, "w") do io
            write(io, """
            {
              "store": { "backend": "duckdb", "path": "/tmp/test.db" },
              "search": { "max_stops": 3, "scope": "intl" }
            }
            """)
        end
        cfg = load_config(json_path)
        @test cfg.backend == "duckdb"
        @test cfg.db_path == "/tmp/test.db"
        @test cfg.max_stops == 3
        @test cfg.scope == SCOPE_INTL
        rm(json_path; force=true)
    end

    @testset "Default paths point to demo data" begin
        cfg = SearchConfig()
        @test endswith(cfg.ssim_path, "uaoa_ssim.new.dat")
        @test endswith(cfg.mct_path, "MCTIMFILUA.DAT")
    end
end

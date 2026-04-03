using Test
using ItinerarySearch
using ItinerarySearch: _default_path, AirlineCode

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

@testset "load_constraints" begin
    @testset "Full constraints section" begin
        json = """
        {
            "constraints": {
                "max_stops": 1,
                "deny_carriers": ["XX", "YY"],
                "allow_service_types": ["J"],
                "max_circuity": 3.0,
                "domestic_circuity_extra_miles": 600
            }
        }
        """
        path = tempname() * ".json"
        write(path, json)
        sc = load_constraints(path)
        @test sc.defaults.max_stops == Int16(1)
        @test sc.defaults.deny_carriers == Set([AirlineCode("XX"), AirlineCode("YY")])
        @test sc.defaults.allow_service_types == Set(['J'])
        @test sc.defaults.max_circuity == 3.0
        @test sc.defaults.domestic_circuity_extra_miles == 600.0
        rm(path)
    end

    @testset "Missing constraints key returns defaults" begin
        json = """{"search": {"max_stops": 2}}"""
        path = tempname() * ".json"
        write(path, json)
        sc = load_constraints(path)
        d = sc.defaults
        # All scalar fields should be at their ParameterSet defaults
        @test d.max_stops == Int16(2)
        @test d.max_connection_time == Int16(480)
        @test d.max_elapsed == Int32(1440)
        @test isempty(d.deny_carriers)
        @test isempty(d.allow_service_types)
        rm(path)
    end

    @testset "Empty constraints object returns defaults" begin
        json = """{"constraints": {}}"""
        path = tempname() * ".json"
        write(path, json)
        sc = load_constraints(path)
        d = sc.defaults
        @test d.max_stops == Int16(2)
        @test d.max_connection_time == Int16(480)
        @test isempty(d.deny_carriers)
        rm(path)
    end

    @testset "Numeric range fields" begin
        json = """
        {
            "constraints": {
                "min_connection_time": 30,
                "max_connection_time": 360,
                "min_stops": 0,
                "max_stops": 2,
                "min_elapsed": 60,
                "max_elapsed": 720,
                "min_total_distance": 100,
                "max_total_distance": 5000,
                "min_circuity": 0.5,
                "max_circuity": 2.5,
                "circuity_factor": 1.8,
                "international_circuity_extra_miles": 800
            }
        }
        """
        path = tempname() * ".json"
        write(path, json)
        sc = load_constraints(path)
        p = sc.defaults
        @test p.min_connection_time == Int16(30)
        @test p.max_connection_time == Int16(360)
        @test p.min_stops == Int16(0)
        @test p.max_stops == Int16(2)
        @test p.min_elapsed == Int32(60)
        @test p.max_elapsed == Int32(720)
        @test p.min_total_distance == Float32(100.0)
        @test p.max_total_distance == Float32(5000.0)
        @test p.min_circuity == 0.5
        @test p.max_circuity == 2.5
        @test p.circuity_factor == 1.8
        @test p.international_circuity_extra_miles == 800.0
        rm(path)
    end

    @testset "Categorical deny/allow sets" begin
        json = """
        {
            "constraints": {
                "deny_countries": ["RU", "BY"],
                "allow_carriers": ["UA", "AA"],
                "deny_stations": ["SVO", "DME"],
                "deny_service_types": ["C"],
                "allow_body_types": ["W"]
            }
        }
        """
        path = tempname() * ".json"
        write(path, json)
        sc = load_constraints(path)
        p = sc.defaults
        using InlineStrings
        @test p.deny_countries == Set([InlineString3("RU"), InlineString3("BY")])
        @test p.allow_carriers == Set([AirlineCode("UA"), AirlineCode("AA")])
        @test p.deny_stations == Set([InlineString3("SVO"), InlineString3("DME")])
        @test p.deny_service_types == Set(['C'])
        @test p.allow_body_types == Set(['W'])
        rm(path)
    end
end

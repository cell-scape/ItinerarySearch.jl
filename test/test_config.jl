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

    # ── Dict constructor ─────────────────────────────────────────────────────
    @testset "SearchConfig(dict) — Symbol keys, canonical enum values" begin
        cfg = SearchConfig(Dict(:max_stops => 3, :scope => SCOPE_INTL))
        @test cfg.max_stops == 3
        @test cfg.scope == SCOPE_INTL
        @test cfg.interline == INTERLINE_CODESHARE  # default preserved
    end

    @testset "SearchConfig(dict) — String keys" begin
        cfg = SearchConfig(Dict("max_stops" => 4, "interline" => "all"))
        @test cfg.max_stops == 4
        @test cfg.interline == INTERLINE_ALL
    end

    @testset "SearchConfig(dict) — enum fields accept strings" begin
        cfg = SearchConfig(Dict(
            :scope => "intl",
            :interline => "online",
        ))
        @test cfg.scope == SCOPE_INTL
        @test cfg.interline == INTERLINE_ONLINE
    end

    @testset "SearchConfig(dict) — Symbol-typed fields accept strings" begin
        cfg = SearchConfig(Dict(
            :log_level => "debug",
            :distance_formula => "vincenty",
            :mct_codeshare_mode => "marketing",
        ))
        @test cfg.log_level === :debug
        @test cfg.distance_formula === :vincenty
        @test cfg.mct_codeshare_mode === :marketing
    end

    @testset "SearchConfig(dict) — output_formats accepts Vector{String}" begin
        cfg = SearchConfig(Dict(:output_formats => ["json", "csv"]))
        @test cfg.output_formats == [:json, :csv]
    end

    @testset "SearchConfig(dict) — nested mct_audit AbstractDict" begin
        cfg = SearchConfig(Dict(
            :mct_audit => Dict(:enabled => true, :detail => "detailed",
                               :max_candidates => 5),
        ))
        @test cfg.mct_audit.enabled == true
        @test cfg.mct_audit.detail === :detailed
        @test cfg.mct_audit.max_candidates == 5
    end

    @testset "SearchConfig(dict) — unknown key errors" begin
        @test_throws ArgumentError SearchConfig(Dict(:not_a_field => 1))
        @test_throws ArgumentError SearchConfig(Dict("nonsense" => "value"))
    end

    @testset "SearchConfig(dict) — empty dict yields defaults" begin
        cfg = SearchConfig(Dict{Symbol,Any}())
        default = SearchConfig()
        @test cfg.max_stops == default.max_stops
        @test cfg.scope == default.scope
        @test cfg.interline == default.interline
        @test cfg.log_level === default.log_level
    end
end

@testset "load_config — extended SearchConfig parsers" begin
    @testset "search.distance_formula parses to Symbol" begin
        path = tempname() * ".json"
        write(path, """{"search": {"distance_formula": "vincenty"}}""")
        cfg = load_config(path)
        @test cfg.distance_formula === :vincenty
        rm(path)
    end

    @testset "search.maft/interline_dcnx/crs_cnx enabled flags" begin
        path = tempname() * ".json"
        write(path, """
        {"search": {"maft_enabled": false, "interline_dcnx_enabled": false,
                    "crs_cnx_enabled": false}}
        """)
        cfg = load_config(path)
        @test cfg.maft_enabled == false
        @test cfg.interline_dcnx_enabled == false
        @test cfg.crs_cnx_enabled == false
        rm(path)
    end

    @testset "mct_behaviour section" begin
        path = tempname() * ".json"
        write(path, """
        {"mct_behaviour": {
            "mct_cache_enabled": false,
            "mct_serial_ascending": true,
            "mct_codeshare_mode": "marketing",
            "mct_schengen_mode": "eur_only",
            "mct_suppressions_enabled": false
        }}
        """)
        cfg = load_config(path)
        @test cfg.mct_cache_enabled == false
        @test cfg.mct_serial_ascending == true
        @test cfg.mct_codeshare_mode === :marketing
        @test cfg.mct_schengen_mode === :eur_only
        @test cfg.mct_suppressions_enabled == false
        rm(path)
    end

    @testset "mct_behaviour overrides search.mct_cache_enabled" begin
        # When both locations set the same key, `mct_behaviour` wins (canonical home).
        path = tempname() * ".json"
        write(path, """
        {"search": {"mct_cache_enabled": true},
         "mct_behaviour": {"mct_cache_enabled": false}}
        """)
        cfg = load_config(path)
        @test cfg.mct_cache_enabled == false
        rm(path)
    end

    @testset "exhaustive config/defaults.json parses cleanly" begin
        # config/defaults.json is an exhaustive exemplar — it lists every
        # SearchConfig field.  Most values mirror the compiled-in defaults,
        # but a handful (schedule window) are tuned to be useful demo
        # settings rather than strict defaults.  The test's job is to
        # verify the file is parseable and every field comes through; it
        # does NOT require the file to equal SearchConfig() exactly.
        default_cfg = SearchConfig()
        file_cfg = load_config(joinpath(@__DIR__, "..", "config", "defaults.json"))

        # Fields that should mirror struct defaults (the "not-tuned" fields).
        @test file_cfg.max_stops == default_cfg.max_stops
        @test file_cfg.max_connection_minutes == default_cfg.max_connection_minutes
        @test file_cfg.max_elapsed_minutes == default_cfg.max_elapsed_minutes
        @test file_cfg.circuity_factor == default_cfg.circuity_factor
        @test file_cfg.circuity_extra_miles == default_cfg.circuity_extra_miles
        @test file_cfg.scope == default_cfg.scope
        @test file_cfg.interline == default_cfg.interline
        @test file_cfg.allow_roundtrips == default_cfg.allow_roundtrips
        @test file_cfg.distance_formula === default_cfg.distance_formula
        @test file_cfg.maft_enabled == default_cfg.maft_enabled
        @test file_cfg.interline_dcnx_enabled == default_cfg.interline_dcnx_enabled
        @test file_cfg.crs_cnx_enabled == default_cfg.crs_cnx_enabled
        @test file_cfg.mct_cache_enabled == default_cfg.mct_cache_enabled
        @test file_cfg.mct_serial_ascending == default_cfg.mct_serial_ascending
        @test file_cfg.mct_codeshare_mode === default_cfg.mct_codeshare_mode
        @test file_cfg.mct_schengen_mode === default_cfg.mct_schengen_mode
        @test file_cfg.mct_suppressions_enabled == default_cfg.mct_suppressions_enabled
        @test file_cfg.metrics_level === default_cfg.metrics_level
        @test file_cfg.event_log_enabled == default_cfg.event_log_enabled
        @test file_cfg.log_level === default_cfg.log_level
        @test file_cfg.output_formats == default_cfg.output_formats

        # Fields the exemplar intentionally tunes — verify they parse to
        # sensible values (positive integers in the expected ranges),
        # not that they equal the compiled-in defaults.
        @test file_cfg.leading_days >= 0 && file_cfg.leading_days <= 7
        @test file_cfg.trailing_days >= 0 && file_cfg.trailing_days <= 7
        @test file_cfg.max_days >= 1 && file_cfg.max_days <= 14
    end
end

@testset "MCTAuditConfig(dict)" begin
    @testset "Symbol keys, canonical values" begin
        a = MCTAuditConfig(Dict(:enabled => true, :detail => :detailed))
        @test a.enabled == true
        @test a.detail === :detailed
    end

    @testset "String keys, detail as string" begin
        a = MCTAuditConfig(Dict("enabled" => true, "detail" => "summary",
                                "max_candidates" => 20))
        @test a.enabled == true
        @test a.detail === :summary
        @test a.max_candidates == 20
    end

    @testset "unknown key errors" begin
        @test_throws ArgumentError MCTAuditConfig(Dict(:bogus => 1))
    end
end

@testset "circuity_check_scope" begin
    @test SearchConfig().circuity_check_scope == :both
    @test SearchConfig(circuity_check_scope=:connection).circuity_check_scope == :connection
    @test SearchConfig(circuity_check_scope=:itinerary).circuity_check_scope == :itinerary
    # Invalid values go through the JSON parser (see _parse_circuity_check_scope test);
    # direct construction is permissive, matching other Symbol-valued SearchConfig fields.
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

    @testset "circuity_tiers JSON round-trip" begin
        raw = """
        {
          "constraints": {
            "defaults": {
              "circuity_tiers": [
                {"max_distance": 200, "factor": 2.3},
                {"max_distance": 900, "factor": 1.8},
                {"max_distance": null, "factor": 1.2}
              ]
            }
          }
        }
        """
        path = tempname() * ".json"
        write(path, raw)
        try
            sc = load_constraints(path)
            @test length(sc.defaults.circuity_tiers) == 3
            @test sc.defaults.circuity_tiers[1] == CircuityTier(200.0, 2.3)
            @test sc.defaults.circuity_tiers[end] == CircuityTier(Inf, 1.2)
        finally
            rm(path; force=true)
        end
    end
end

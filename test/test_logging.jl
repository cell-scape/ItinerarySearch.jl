using Test
using ItinerarySearch
using JSON3
using Logging
using LoggingExtras

@testset "Structured Logging" begin

    @testset "_resolve_log_level from config" begin
        config = SearchConfig(log_level = :debug)
        level = ItinerarySearch._resolve_log_level(config)
        @test level == Logging.Debug

        config2 = SearchConfig(log_level = :warn)
        @test ItinerarySearch._resolve_log_level(config2) == Logging.Warn

        config3 = SearchConfig()
        @test ItinerarySearch._resolve_log_level(config3) == Logging.Info
    end

    @testset "_resolve_log_level env var override" begin
        old = get(ENV, "ITINERARY_SEARCH_LOG_LEVEL", nothing)
        try
            ENV["ITINERARY_SEARCH_LOG_LEVEL"] = "debug"
            config = SearchConfig(log_level = :error)
            @test ItinerarySearch._resolve_log_level(config) == Logging.Debug
        finally
            if old === nothing
                delete!(ENV, "ITINERARY_SEARCH_LOG_LEVEL")
            else
                ENV["ITINERARY_SEARCH_LOG_LEVEL"] = old
            end
        end
    end

    @testset "_dynatrace_json_formatter output" begin
        buf = IOBuffer()
        args = (
            level = Logging.Info,
            message = "Test message",
            _module = ItinerarySearch,
            file = "test.jl",
            line = 42,
            kwargs = pairs((count = 10, name = "test")),
        )
        ItinerarySearch._dynatrace_json_formatter(buf, args)
        line = String(take!(buf))
        parsed = JSON3.read(strip(line))

        @test parsed[:severity] == "INFO"
        @test parsed[:content] == "Test message"
        @test parsed[Symbol("service.name")] == "ItinerarySearch"
        @test haskey(parsed, :timestamp)
        @test haskey(parsed, :attributes)
        @test parsed[:attributes][:count] == 10
        @test parsed[:attributes][:module] == "ItinerarySearch"
        @test parsed[:attributes][:file] == "test.jl"
        @test parsed[:attributes][:line] == 42
    end

    @testset "setup_logger returns TeeLogger with JSON file" begin
        path = tempname() * ".log"
        try
            config = SearchConfig(log_json_path = path, log_level = :info)
            logger = setup_logger(config)
            @test logger isa TeeLogger
            ItinerarySearch._close_logger(logger)
            @test isfile(path)
        finally
            rm(path; force = true)
        end
    end

    @testset "setup_logger console-only when no json path" begin
        config = SearchConfig()
        logger = setup_logger(config)
        @test logger isa MinLevelLogger
    end

    @testset "JSON file receives log events" begin
        path = tempname() * ".log"
        try
            config = SearchConfig(log_json_path = path, log_level = :info)
            logger = setup_logger(config)
            prev = global_logger(logger)
            try
                @info "Test log message" key1 = 42 key2 = "value"
            finally
                ItinerarySearch._close_logger(logger)
                global_logger(prev)
            end

            @test isfile(path)
            content = readline(path)
            parsed = JSON3.read(content)
            @test parsed[:severity] == "INFO"
            @test parsed[:content] == "Test log message"
            @test parsed[:attributes][:key1] == 42
        finally
            rm(path; force = true)
        end
    end

    @testset "Debug messages filtered at info level" begin
        path = tempname() * ".log"
        try
            config = SearchConfig(log_json_path = path, log_level = :info)
            logger = setup_logger(config)
            prev = global_logger(logger)
            try
                @debug "Should not appear"
                @info "Should appear"
            finally
                ItinerarySearch._close_logger(logger)
                global_logger(prev)
            end

            lines = readlines(path)
            @test length(lines) == 1
            @test JSON3.read(lines[1])[:severity] == "INFO"
        finally
            rm(path; force = true)
        end
    end

    @testset "Debug messages appear at debug level" begin
        path = tempname() * ".log"
        try
            config = SearchConfig(log_json_path = path, log_level = :debug)
            logger = setup_logger(config)
            prev = global_logger(logger)
            try
                @debug "Debug message"
                @info "Info message"
            finally
                ItinerarySearch._close_logger(logger)
                global_logger(prev)
            end

            lines = readlines(path)
            @test length(lines) == 2
            @test JSON3.read(lines[1])[:severity] == "DEBUG"
            @test JSON3.read(lines[2])[:severity] == "INFO"
        finally
            rm(path; force = true)
        end
    end

    @testset "_close_logger does not close stdout" begin
        config = SearchConfig(log_stdout_json = true, log_level = :info)
        logger = setup_logger(config)
        ItinerarySearch._close_logger(logger)
        @test isopen(stdout)
    end

    @testset "Integration: build_graph! with JSON logging" begin
        using DuckDB, DBInterface
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
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)",
            )
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)",
            )
            post_ingest_sql!(store)

            path = tempname() * ".log"
            config = SearchConfig(log_json_path = path, log_level = :info)
            graph = build_graph!(store, config, Date(2026, 6, 15))

            @test isfile(path)
            lines = readlines(path)
            @test length(lines) >= 5

            for line in lines
                parsed = JSON3.read(line)
                @test haskey(parsed, :timestamp)
                @test haskey(parsed, :severity)
                @test haskey(parsed, :content)
                @test parsed[Symbol("service.name")] == "ItinerarySearch"
            end

            rm(path; force = true)
        finally
            close(store)
        end
    end

    @testset "build_graph! default config (no JSON output)" begin
        using DuckDB, DBInterface
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
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)",
            )
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)",
            )
            post_ingest_sql!(store)

            config = SearchConfig()
            graph = build_graph!(store, config, Date(2026, 6, 15))
            @test graph.build_stats.total_stations > 0
        finally
            close(store)
        end
    end
end

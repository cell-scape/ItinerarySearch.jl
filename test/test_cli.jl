# test/test_cli.jl — Tests for the CLI module (ItinerarySearch.CLI)

using Test
using ItinerarySearch
using ItinerarySearch.CLI: _build_parser, _apply_overrides, _apply_constraint_overrides
using ItinerarySearch.CLI: _write_output, _cmd_info, _cmd_ingest, _cmd_search
using ArgParse: parse_args
using JSON3
using Dates

# Helper: redirect stdout to a temp file, run thunk, return captured string.
# redirect_stdout only accepts IO objects backed by a file descriptor (not IOBuffer).
function _capture_stdout(thunk::Function)::String
    tmp = tempname()
    ret = nothing
    try
        open(tmp, "w") do f
            ret = redirect_stdout(thunk, f)
        end
    finally
        # nothing — rm handled by caller or at end of function
    end
    out = isfile(tmp) ? read(tmp, String) : ""
    rm(tmp; force=true)
    return out
end

# Helper: redirect stderr to a temp file, run thunk, return (return_value, captured_string).
function _capture_stderr(thunk::Function)
    tmp = tempname()
    ret = nothing
    try
        open(tmp, "w") do f
            ret = redirect_stderr(thunk, f)
        end
    finally
    end
    out = isfile(tmp) ? read(tmp, String) : ""
    rm(tmp; force=true)
    return (ret, out)
end

@testset "CLI" begin

    # ── Argument parsing ───────────────────────────────────────────────────────

    @testset "Argument parsing" begin
        parser = _build_parser()

        @testset "search command" begin
            args = parse_args(["search", "ORD", "LHR", "2026-06-15"], parser)
            @test args["%COMMAND%"] == "search"
            sub = args["search"]
            @test sub["origin"] == "ORD"
            @test sub["dest"] == "LHR"
            @test sub["dates"] == ["2026-06-15"]
            @test sub["cross"] == false
        end

        @testset "search command: multiple dates" begin
            args = parse_args(["search", "ORD,MDW", "LHR,LGW", "2026-06-15", "2026-06-16"], parser)
            sub = args["search"]
            @test sub["origin"] == "ORD,MDW"
            @test sub["dest"] == "LHR,LGW"
            @test sub["dates"] == ["2026-06-15", "2026-06-16"]
        end

        @testset "search command: --cross flag" begin
            args = parse_args(["search", "ORD", "LHR", "2026-06-15", "--cross"], parser)
            @test args["search"]["cross"] == true
        end

        @testset "build command" begin
            args = parse_args(["build", "--date", "2026-06-15"], parser)
            @test args["%COMMAND%"] == "build"
            @test args["build"]["date"] == "2026-06-15"
        end

        @testset "ingest command" begin
            args = parse_args(["ingest"], parser)
            @test args["%COMMAND%"] == "ingest"
        end

        @testset "info command" begin
            args = parse_args(["info"], parser)
            @test args["%COMMAND%"] == "info"
        end

        @testset "trip command" begin
            args = parse_args(["trip", "ORD", "LHR", "2026-06-15"], parser)
            @test args["%COMMAND%"] == "trip"
            sub = args["trip"]
            @test sub["legs"] == ["ORD", "LHR", "2026-06-15"]
            @test sub["min-stay"] == 0
            @test sub["max-trips"] == 1000
            @test sub["max-per-leg"] == 100
        end

        @testset "global flags defaults" begin
            args = parse_args(["info"], parser)
            @test args["config"] === nothing
            @test args["log-level"] === nothing
            @test args["log-json"] === nothing
            @test args["quiet"] == false
            @test args["compact"] == false
            @test args["output"] === nothing
            @test args["leading-days"] === nothing
            @test args["trailing-days"] === nothing
            @test args["max-stops"] === nothing
            @test args["max-elapsed"] === nothing
            @test args["max-connection"] === nothing
            @test args["scope"] === nothing
            @test args["interline"] === nothing
            @test args["allow-roundtrips"] == false
            @test args["no-mct-cache"] == false
            @test args["newssim"] === nothing
            @test args["delimiter"] === nothing
        end

        @testset "--newssim flag recognized" begin
            args = parse_args(["--newssim", "/tmp/test.csv", "info"], parser)
            @test args["newssim"] == "/tmp/test.csv"
            @test args["delimiter"] === nothing
        end

        @testset "--newssim with --delimiter" begin
            args = parse_args(["--newssim", "/tmp/test.csv", "--delimiter", "|", "info"], parser)
            @test args["newssim"] == "/tmp/test.csv"
            @test args["delimiter"] == "|"
        end

        @testset "--delimiter alone (without --newssim)" begin
            args = parse_args(["--delimiter", "\\t", "info"], parser)
            @test args["newssim"] === nothing
            @test args["delimiter"] == "\\t"
        end

        @testset "--newssim with search command" begin
            args = parse_args(["--newssim", "/tmp/test.csv", "search", "ORD", "LHR", "2026-06-15"], parser)
            @test args["newssim"] == "/tmp/test.csv"
            @test args["%COMMAND%"] == "search"
        end

        @testset "--newssim with build command" begin
            args = parse_args(["--newssim", "/tmp/test.csv", "build", "--date", "2026-06-15"], parser)
            @test args["newssim"] == "/tmp/test.csv"
            @test args["%COMMAND%"] == "build"
        end

        @testset "global flags: overrides parsed" begin
            args = parse_args([
                "--leading-days", "5",
                "--trailing-days", "3",
                "--scope", "intl",
                "--interline", "online",
                "--no-mct-cache",
                "--allow-roundtrips",
                "--compact",
                "info",
            ], parser)
            @test args["leading-days"] == 5
            @test args["trailing-days"] == 3
            @test args["scope"] == "intl"
            @test args["interline"] == "online"
            @test args["no-mct-cache"] == true
            @test args["allow-roundtrips"] == true
            @test args["compact"] == true
        end
    end

    # ── _apply_overrides ──────────────────────────────────────────────────────

    @testset "_apply_overrides" begin
        parser = _build_parser()

        @testset "defaults preserved when flags absent" begin
            args = parse_args(["info"], parser)
            base = SearchConfig()
            cfg = _apply_overrides(base, args)
            @test cfg.leading_days == base.leading_days
            @test cfg.trailing_days == base.trailing_days
            @test cfg.scope == base.scope
            @test cfg.interline == base.interline
            @test cfg.allow_roundtrips == base.allow_roundtrips
            @test cfg.mct_cache_enabled == base.mct_cache_enabled
        end

        @testset "--leading-days 5" begin
            args = parse_args(["--leading-days", "5", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.leading_days == 5
        end

        @testset "--trailing-days 3" begin
            args = parse_args(["--trailing-days", "3", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.trailing_days == 3
        end

        @testset "--scope intl" begin
            args = parse_args(["--scope", "intl", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.scope == SCOPE_INTL
        end

        @testset "--scope dom" begin
            args = parse_args(["--scope", "dom", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.scope == SCOPE_DOM
        end

        @testset "--scope all" begin
            args = parse_args(["--scope", "all", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.scope == SCOPE_ALL
        end

        @testset "--interline online" begin
            args = parse_args(["--interline", "online", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.interline == INTERLINE_ONLINE
        end

        @testset "--interline all" begin
            args = parse_args(["--interline", "all", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.interline == INTERLINE_ALL
        end

        @testset "--no-mct-cache" begin
            args = parse_args(["--no-mct-cache", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.mct_cache_enabled == false
        end

        @testset "--allow-roundtrips" begin
            args = parse_args(["--allow-roundtrips", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.allow_roundtrips == true
        end

        @testset "--log-level debug" begin
            args = parse_args(["--log-level", "debug", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.log_level == :debug
        end

        @testset "--log-level invalid is ignored" begin
            args = parse_args(["--log-level", "verbose", "info"], parser)
            base = SearchConfig()
            cfg = _apply_overrides(base, args)
            # Invalid log level is silently skipped — config unchanged
            @test cfg.log_level == base.log_level
        end

        @testset "--log-json path" begin
            args = parse_args(["--log-json", "/tmp/test.jsonl", "info"], parser)
            cfg = _apply_overrides(SearchConfig(), args)
            @test cfg.log_json_path == "/tmp/test.jsonl"
        end
    end

    # ── _apply_constraint_overrides ───────────────────────────────────────────

    @testset "_apply_constraint_overrides" begin
        parser = _build_parser()

        @testset "no overrides: returns same defaults" begin
            args = parse_args(["info"], parser)
            base = SearchConstraints()
            result = _apply_constraint_overrides(base, args)
            @test result.defaults.max_stops == base.defaults.max_stops
            @test result.defaults.max_mct_override == base.defaults.max_mct_override
            @test result.defaults.max_elapsed == base.defaults.max_elapsed
        end

        @testset "--max-stops 3" begin
            args = parse_args(["--max-stops", "3", "info"], parser)
            result = _apply_constraint_overrides(SearchConstraints(), args)
            @test result.defaults.max_stops == Int16(3)
        end

        @testset "--max-stops 0 (nonstop only)" begin
            args = parse_args(["--max-stops", "0", "info"], parser)
            result = _apply_constraint_overrides(SearchConstraints(), args)
            @test result.defaults.max_stops == Int16(0)
        end

        @testset "--max-connection 600" begin
            args = parse_args(["--max-connection", "600", "info"], parser)
            result = _apply_constraint_overrides(SearchConstraints(), args)
            @test result.defaults.max_mct_override == Minutes(600)
        end

        @testset "--max-elapsed 720" begin
            args = parse_args(["--max-elapsed", "720", "info"], parser)
            result = _apply_constraint_overrides(SearchConstraints(), args)
            @test result.defaults.max_elapsed == Int32(720)
        end

        @testset "multiple overrides combined" begin
            args = parse_args([
                "--max-stops", "1",
                "--max-connection", "300",
                "--max-elapsed", "480",
                "info",
            ], parser)
            result = _apply_constraint_overrides(SearchConstraints(), args)
            @test result.defaults.max_stops == Int16(1)
            @test result.defaults.max_mct_override == Minutes(300)
            @test result.defaults.max_elapsed == Int32(480)
        end

        @testset "closed_stations preserved through override" begin
            base = SearchConstraints(
                closed_stations = Set([StationCode("ORD")]),
            )
            args = parse_args(["--max-stops", "1", "info"], parser)
            result = _apply_constraint_overrides(base, args)
            @test StationCode("ORD") in result.closed_stations
        end
    end

    # ── _write_output ─────────────────────────────────────────────────────────

    @testset "_write_output" begin
        parser = _build_parser()

        @testset "writes to stdout when --output not set" begin
            args = parse_args(["info"], parser)
            out = _capture_stdout() do
                _write_output("hello output", args)
            end
            @test contains(out, "hello output")
        end

        @testset "writes to file when --output is set" begin
            tmp = tempname()
            try
                args = parse_args(["--output", tmp, "info"], parser)
                _write_output("{\"key\":\"value\"}", args)
                @test isfile(tmp)
                content = read(tmp, String)
                @test contains(content, "{\"key\":\"value\"}")
            finally
                rm(tmp; force=true)
            end
        end

        @testset "file output does not write to stdout" begin
            tmp = tempname()
            try
                args = parse_args(["--output", tmp, "info"], parser)
                out = _capture_stdout() do
                    _write_output("secret content", args)
                end
                @test !contains(out, "secret content")
            finally
                rm(tmp; force=true)
            end
        end
    end

    # ── Integration: _cmd_info ────────────────────────────────────────────────

    @testset "Integration: _cmd_info" begin
        parser = _build_parser()
        args = parse_args(["info"], parser)
        config = SearchConfig()

        out = _capture_stdout() do
            _cmd_info(config, args["info"], args)
        end

        @test !isempty(out)
        parsed = JSON3.read(out)
        @test haskey(parsed, :table_stats)
        @test haskey(parsed, :config)
        # Empty store: no legs
        @test parsed[:table_stats][:legs] == 0
        # Config section contains known fields
        @test haskey(parsed[:config], :backend)
        @test parsed[:config][:backend] == "duckdb"
        @test haskey(parsed[:config], :mct_cache_enabled)
    end

    # ── Integration: _cmd_ingest ──────────────────────────────────────────────

    @testset "Integration: _cmd_ingest with test data" begin
        ssim_path = tempname()
        airports_path = tempname()
        mct_path = tempname()

        write(ssim_path, make_test_ssim())
        write(airports_path, make_test_airports())
        write(mct_path, make_test_mct())

        config = SearchConfig(
            ssim_path = ssim_path,
            mct_path = mct_path,
            airports_path = airports_path,
            regions_path = "/dev/null",
            aircrafts_path = "/dev/null",
            oa_control_path = "/dev/null",
        )

        parser = _build_parser()
        args = parse_args(["ingest"], parser)

        out = ""
        exit_code = 0
        out = _capture_stdout() do
            exit_code = _cmd_ingest(config, args["ingest"], args)
        end

        @test exit_code == 0
        @test !isempty(out)

        parsed = JSON3.read(out)
        @test haskey(parsed, :legs)
        @test parsed[:legs] > 0
        @test haskey(parsed, :stations)
        @test parsed[:stations] > 0

        rm(ssim_path; force=true)
        rm(airports_path; force=true)
        rm(mct_path; force=true)
    end

    # ── Integration: _cmd_search ──────────────────────────────────────────────

    @testset "Integration: _cmd_search with test data" begin
        ssim_path = tempname()
        airports_path = tempname()
        mct_path = tempname()

        write(ssim_path, make_test_ssim())
        write(airports_path, make_test_airports())
        write(mct_path, make_test_mct())

        config = SearchConfig(
            ssim_path = ssim_path,
            mct_path = mct_path,
            airports_path = airports_path,
            regions_path = "/dev/null",
            aircrafts_path = "/dev/null",
            oa_control_path = "/dev/null",
        )
        constraints = SearchConstraints()

        parser = _build_parser()
        args = parse_args(["search", "ORD", "LHR", "2026-06-15"], parser)

        exit_code = 0
        out = _capture_stdout() do
            exit_code = _cmd_search(config, constraints, args["search"], args)
        end

        @test exit_code == 0
        @test !isempty(out)

        # JSON structure is date → origin → dest → [...itineraries], or {} if no results.
        # Both are valid: the search pipeline ran without error.
        parsed = JSON3.read(out)
        @test parsed isa JSON3.Object

        # If the test SSIM data yields itineraries, verify the nested structure.
        if length(parsed) >= 1
            date_entry = first(values(parsed))
            @test date_entry isa JSON3.Object
        end

        rm(ssim_path; force=true)
        rm(airports_path; force=true)
        rm(mct_path; force=true)
    end

    # ── main: exit codes and error handling ───────────────────────────────────
    #
    # Note: ArgParse calls exit() directly (not throw) for missing commands or
    # required positional args, so those cases cannot be tested in-process.
    # We test the in-process-safe cases: valid commands (exit 0) and config errors
    # (exit 1, which is caught and returned by the main function's catch block).

    @testset "main: error handling" begin

        @testset "info: valid command → exit 0" begin
            code = 0
            out = _capture_stdout() do
                code = ItinerarySearch.CLI.main(["info"])
            end
            @test code == 0
            parsed = JSON3.read(out)
            @test haskey(parsed, :table_stats)
        end

        @testset "--quiet flag: exit 0" begin
            # With --quiet the global logger is set to Error-level only.
            # The exit code should still be 0 for a valid command.
            code = 0
            out = _capture_stdout() do
                code = ItinerarySearch.CLI.main(["--quiet", "info"])
            end
            @test code == 0
            @test !isempty(out)
        end

        @testset "--output writes to file for info command" begin
            tmp = tempname()
            try
                code = ItinerarySearch.CLI.main(["--output", tmp, "info"])
                @test code == 0
                @test isfile(tmp)
                parsed = JSON3.read(read(tmp, String))
                @test haskey(parsed, :table_stats)
            finally
                rm(tmp; force=true)
            end
        end

        @testset "nonexistent --config file → exit 1" begin
            # Config error is caught in main's try block and returns 1.
            (code, _) = _capture_stderr() do
                ItinerarySearch.CLI.main(["--config", "/nonexistent/path/config.json", "info"])
            end
            @test code == 1
        end

        @testset "parser requires origin/dest/dates for search" begin
            # Verify the search sub-arg table has required positionals.
            # We inspect the parser structure rather than invoking parse_args,
            # because ArgParse calls exit() directly for missing required args.
            parser = _build_parser()
            search_table = parser["search"]
            arg_names = [a.dest_name for a in search_table.args_table.fields]
            @test "origin" in arg_names
            @test "dest" in arg_names
            @test "dates" in arg_names
        end
    end

end

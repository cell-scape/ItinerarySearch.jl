# test/test_cli_parallel_flag.jl — Tests for the --no-parallel CLI flag
#
# Verifies that:
#   - The parser accepts --no-parallel as a valid global flag
#   - The flag defaults to false when absent
#   - When present, _apply_overrides sets parallel_markets = false
#   - The default config leaves parallel_markets = true (unchanged)

using Test
using ItinerarySearch
using ItinerarySearch.CLI: _build_parser, _apply_overrides
using ArgParse: parse_args

@testset "CLI --no-parallel flag" begin

    @testset "parser recognizes --no-parallel" begin
        parser = _build_parser()
        args = parse_args(["--no-parallel", "info"], parser)
        # ArgParse stores the key with hyphens preserved
        @test haskey(args, "no-parallel")
        @test args["no-parallel"] === true
    end

    @testset "flag defaults to false when absent" begin
        parser = _build_parser()
        args = parse_args(["info"], parser)
        @test haskey(args, "no-parallel")
        @test args["no-parallel"] === false
    end

    @testset "default SearchConfig has parallel_markets = true" begin
        parser = _build_parser()
        args = parse_args(["info"], parser)
        cfg = _apply_overrides(SearchConfig(), args)
        @test cfg.parallel_markets === true
    end

    @testset "--no-parallel sets parallel_markets = false" begin
        parser = _build_parser()
        args = parse_args(["--no-parallel", "info"], parser)
        cfg = _apply_overrides(SearchConfig(), args)
        @test cfg.parallel_markets === false
    end

    @testset "--no-parallel does not affect mct_cache_enabled" begin
        parser = _build_parser()
        args = parse_args(["--no-parallel", "info"], parser)
        cfg = _apply_overrides(SearchConfig(), args)
        @test cfg.mct_cache_enabled === true
    end

    @testset "--no-mct-cache does not affect parallel_markets" begin
        parser = _build_parser()
        args = parse_args(["--no-mct-cache", "info"], parser)
        cfg = _apply_overrides(SearchConfig(), args)
        @test cfg.parallel_markets === true
    end

    @testset "--no-parallel combined with --no-mct-cache" begin
        parser = _build_parser()
        args = parse_args(["--no-parallel", "--no-mct-cache", "info"], parser)
        cfg = _apply_overrides(SearchConfig(), args)
        @test cfg.parallel_markets === false
        @test cfg.mct_cache_enabled === false
    end

    @testset "base config parallel_markets=false is preserved when flag absent" begin
        # Verify _apply_overrides does not reset a false value coming from a config file
        parser = _build_parser()
        args = parse_args(["info"], parser)
        base = SearchConfig(parallel_markets = false)
        cfg = _apply_overrides(base, args)
        @test cfg.parallel_markets === false
    end

end

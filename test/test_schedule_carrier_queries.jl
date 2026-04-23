include("_test_setup.jl")

using ItinerarySearch: query_direct_markets_by_carriers, query_codeshare_partners

@testset "query_direct_markets_by_carriers" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)

        @testset "no carrier filter returns all distinct direct markets" begin
            all_markets = query_direct_markets_by_carriers(store, target, nothing)
            @test !isempty(all_markets)
            @test all_markets isa Vector{Tuple{String,String}}
            # Deduplicated: each (origin, dest) pair appears at most once
            @test length(all_markets) == length(Set(all_markets))
        end

        @testset "UA filter returns a subset of all markets" begin
            ua_markets = query_direct_markets_by_carriers(store, target, ["UA"])
            all_markets = query_direct_markets_by_carriers(store, target, nothing)
            @test length(ua_markets) <= length(all_markets)
            @test Set(ua_markets) ⊆ Set(all_markets)
        end

        @testset "empty carrier list returns empty (all filters reject)" begin
            result = query_direct_markets_by_carriers(store, target, String[])
            @test isempty(result)
        end

        @testset "matches marketing OR operating carrier" begin
            # UA is both a marketing and operating carrier in the demo set.
            ua_mkt = query_direct_markets_by_carriers(store, target, ["UA"])
            @test !isempty(ua_mkt)
        end
    finally
        close(store)
    end
end

@testset "query_codeshare_partners" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)

        @testset "UA partners on demo data" begin
            partners = query_codeshare_partners(store, target, ["UA"])
            @test partners isa Vector{String}
            # Partners are distinct carriers (never UA itself)
            @test !("UA" in partners)
        end

        @testset "empty carrier list returns empty partner list" begin
            partners = query_codeshare_partners(store, target, String[])
            @test isempty(partners)
        end
    finally
        close(store)
    end
end

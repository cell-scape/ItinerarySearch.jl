include("_test_setup.jl")

@testset "MarketUniverse" begin
    u = MarketUniverse([("ORD", "LHR", Date(2026, 2, 25))])
    @test length(u.tuples) == 1
    @test u.tuples[1] == ("ORD", "LHR", Date(2026, 2, 25))
end

@testset "_universe_from_carriers_direct" begin
    newssim_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
    target = Date(2026, 2, 25)
    store = DuckDBStore()
    try
        ingest_newssim!(store, newssim_path)

        @testset "no carrier filter returns all direct markets" begin
            u = ItinerarySearch._universe_from_carriers_direct(store, target, nothing, false)
            @test u isa MarketUniverse
            @test !isempty(u.tuples)
            @test all(t -> t[3] == target, u.tuples)
        end

        @testset "UA filter with include_codeshare=false returns only UA-direct markets" begin
            u = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], false)
            @test !isempty(u.tuples)
            u_all = ItinerarySearch._universe_from_carriers_direct(store, target, nothing, false)
            @test length(u.tuples) <= length(u_all.tuples)
            @test Set(u.tuples) ⊆ Set(u_all.tuples)
        end

        @testset "UA with include_codeshare=true >= UA without" begin
            u_no_cs = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], false)
            u_cs = ItinerarySearch._universe_from_carriers_direct(store, target, ["UA"], true)
            @test length(u_cs.tuples) >= length(u_no_cs.tuples)
            @test Set(u_no_cs.tuples) ⊆ Set(u_cs.tuples)
        end

        @testset "deduplication — no duplicate tuples" begin
            u = ItinerarySearch._universe_from_carriers_direct(store, target, nothing, false)
            @test length(u.tuples) == length(Set(u.tuples))
        end
    finally
        close(store)
    end
end

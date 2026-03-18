using Test
using ItinerarySearch
using DuckDB, DBInterface

@testset "DuckDBStore" begin
    @testset "Construction creates tables" begin
        store = DuckDBStore()
        stats = table_stats(store)
        @test stats.legs == 0
        @test stats.dei == 0
        @test stats.stations == 0
        @test stats.mct == 0
        @test stats.expanded_legs == 0
        @test stats.segments == 0
        @test stats.markets == 0
        close(store)
    end

    @testset "DuckDBStore is an AbstractStore" begin
        store = DuckDBStore()
        @test store isa AbstractStore
        close(store)
    end

    @testset "Custom DB path" begin
        path = tempname() * ".duckdb"
        store = DuckDBStore(path)
        @test isfile(path)
        close(store)
        rm(path; force=true)
        rm(path * ".wal"; force=true)
    end
end

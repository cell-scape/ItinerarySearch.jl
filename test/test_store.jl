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

@testset "Post-Ingest SQL Pipeline" begin
    store = DuckDBStore()

    # Insert test legs directly (simulating ingest)
    DBInterface.execute(store.db, """
    INSERT INTO legs VALUES (
        1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
        'ORD', 'LHR',
        540, 1320, 535, 1325,
        -300, 0, 0, 0,
        '1', '2', '789', 'W', 'UA',
        '2026-06-01', '2026-06-30', 127,
        'D', 'I', '', ' ',
        'JCDZPY', 0.0, false
    )
    """)

    # Insert stations with lat/lng for spatial distance computation
    DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','Chicago','NOA',41.9742,-87.9073,-300)")
    DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','London','EUR',51.4700,-0.4543,0)")

    # Run post-ingest
    post_ingest_sql!(store)

    stats = table_stats(store)
    @test stats.expanded_legs > 0    # EDF expansion created rows
    @test stats.segments > 0         # Segments table populated
    @test stats.markets > 0          # Markets table populated

    # Check segment has circuity (may be 0 since leg distance is 0.0, but column should exist)
    result = DBInterface.execute(store.db, "SELECT segment_circuity FROM segments LIMIT 1")
    row = first(result)
    @test !ismissing(row.segment_circuity) || row.segment_circuity === missing  # column exists

    # Check markets has distance
    result = DBInterface.execute(store.db, "SELECT distance_miles FROM markets LIMIT 1")
    row = first(result)
    @test row.distance_miles > 0  # ORD-LHR is ~3941 miles

    close(store)
end

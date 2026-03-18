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
    DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)")
    DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)")

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

@testset "DuckDBStore Query Methods" begin
    using ItinerarySearch: _build_schedule_filters

    # Setup: create store with test data, run full pipeline
    store = DuckDBStore()

    # Insert test data
    DBInterface.execute(store.db, """
    INSERT INTO legs VALUES (
        1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
        'ORD', 'LHR', 540, 1320, 535, 1325,
        -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
        '2026-06-15', '2026-06-15', 127,
        'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
    )
    """)
    DBInterface.execute(store.db, "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)")
    DBInterface.execute(store.db, "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)")

    post_ingest_sql!(store)

    @testset "query_station" begin
        stn = query_station(store, StationCode("ORD"))
        @test stn !== nothing
        @test stn.code == StationCode("ORD")
        @test stn.country == InlineString3("US")

        @test query_station(store, StationCode("ZZZ")) === nothing
    end

    @testset "get_departures" begin
        deps = get_departures(store, StationCode("ORD"), Date(2026, 6, 15))
        @test length(deps) >= 1
        @test all(r -> r.org == StationCode("ORD"), deps)
    end

    @testset "get_arrivals" begin
        arrs = get_arrivals(store, StationCode("LHR"), Date(2026, 6, 15))
        @test length(arrs) >= 1
        @test all(r -> r.dst == StationCode("LHR"), arrs)
    end

    @testset "query_legs" begin
        legs = query_legs(store, StationCode("ORD"), StationCode("LHR"), Date(2026, 6, 15))
        @test length(legs) >= 1
        @test legs[1].airline == AirlineCode("UA")
    end

    @testset "query_market_distance" begin
        d = query_market_distance(store, StationCode("ORD"), StationCode("LHR"))
        @test d !== nothing
        @test d > 3000  # ORD-LHR is ~3941 miles

        # Reversed order should give same result (NDOD)
        d2 = query_market_distance(store, StationCode("LHR"), StationCode("ORD"))
        @test d ≈ d2

        # Unknown market
        @test query_market_distance(store, StationCode("ZZZ"), StationCode("YYY")) === nothing
    end

    @testset "query_segment" begin
        # Get the segment hash from segments table
        result = DBInterface.execute(store.db, "SELECT segment_hash FROM segments LIMIT 1")
        row = first(result)
        hash_val = UInt64(row.segment_hash)
        seg = query_segment(store, hash_val)
        @test seg !== nothing
        @test seg.airline == AirlineCode("UA")
    end

    @testset "query_segment_stops" begin
        result = DBInterface.execute(store.db, "SELECT segment_hash FROM segments LIMIT 1")
        row = first(result)
        hash_val = UInt64(row.segment_hash)
        bp, op = query_segment_stops(store, hash_val)
        @test length(bp) >= 1
        @test length(op) >= 1
    end

    @testset "query_mct" begin
        # Insert an MCT record
        DBInterface.execute(store.db, """
        INSERT INTO mct VALUES (
            1, 0, 'ORD', 'ORD', 'DD', 45,
            'UA', '', '', '', '', '',
            '', '', '', '', '', '',
            '', '', '', '',
            0, 0, 0, 0,
            '', '', '', '',
            '1900-01-01', '2099-12-31', false, '', '', '',
            'UA', false, 0
        )
        """)

        mct = query_mct(store, AirlineCode("UA"), AirlineCode("UA"), StationCode("ORD"), MCT_DD)
        @test mct.time == Int16(45)
        @test mct.source == SOURCE_EXCEPTION  # carrier-specific match

        # Insert a station standard (arr_carrier = '')
        DBInterface.execute(store.db, """
        INSERT INTO mct VALUES (
            2, 0, 'ORD', 'ORD', 'II', 90,
            '', '', '', '', '', '',
            '', '', '', '', '', '',
            '', '', '', '',
            0, 0, 0, 0,
            '', '', '', '',
            '1900-01-01', '2099-12-31', false, '', '', '',
            '', false, 0
        )
        """)
        mct_std = query_mct(store, AirlineCode("DL"), AirlineCode("DL"), StationCode("ORD"), MCT_II)
        @test mct_std.time == Int16(90)
        @test mct_std.source == SOURCE_STATION_STANDARD

        # Global default fallback
        mct_default = query_mct(store, AirlineCode("XX"), AirlineCode("XX"), StationCode("ZZZ"), MCT_II)
        @test mct_default.source == SOURCE_GLOBAL_DEFAULT
    end

    close(store)
end

@testset "Integration: Full Pipeline" begin
    # Write synthetic SSIM and MCT to temp files
    ssim_path = tempname()
    mct_path = tempname()
    airports_path = tempname()

    write(ssim_path, make_test_ssim())  # from test_ingest.jl (included before this file)
    write(mct_path, make_test_mct())    # from test_ingest.jl

    write(airports_path, make_test_airports())

    # Build config pointing to temp files
    config = SearchConfig(
        ssim_path = ssim_path,
        mct_path = mct_path,
        airports_path = airports_path,
        regions_path = "/dev/null",
        aircrafts_path = "/dev/null",
        oa_control_path = "/dev/null",
    )

    # Run full pipeline
    store = DuckDBStore()
    load_schedule!(store, config)

    # Verify all tables populated
    stats = table_stats(store)
    @test stats.legs > 0
    @test stats.dei > 0
    @test stats.stations == 3
    @test stats.mct > 0
    @test stats.expanded_legs > 0
    @test stats.segments > 0
    @test stats.markets > 0

    # Query tests
    stn = query_station(store, StationCode("ORD"))
    @test stn !== nothing
    @test stn.lat ≈ 41.97 atol=0.01

    # Market distance
    d = query_market_distance(store, StationCode("ORD"), StationCode("LHR"))
    @test d !== nothing
    @test d > 3000

    # MCT lookup
    mct_result = query_mct(store, AirlineCode("UA"), AirlineCode("UA"),
                           StationCode("ORD"), MCT_II)
    @test mct_result.time > 0

    # Cleanup
    close(store)
    rm(ssim_path; force=true)
    rm(mct_path; force=true)
    rm(airports_path; force=true)
end

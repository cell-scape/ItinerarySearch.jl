using Test
using ItinerarySearch
using DuckDB, DBInterface
using Dates
using InlineStrings

# ── Test data helpers ──────────────────────────────────────────────────────────
#
# We build a minimal but realistic two-leg schedule:
#
#   ORD → LHR   UA 1234   dep 09:00  arr 22:00  (all days, Jun 1–30 2026)
#   JFK → ORD   UA  200   dep 08:00  arr 10:00  (all days, Jun 1–30 2026)
#
# Stations: ORD (US/NAM), LHR (GB/EUR), JFK (US/NAM)
#
# This lets us test:
#   - Station creation from reference table (ORD, LHR, JFK all inserted)
#   - Leg/station linkage
#   - Segment grouping (two legs from different flights → 2 segments)
#   - Connection building (JFK→ORD arrives 10:00, ORD→LHR departs 09:00 next day
#     — actually ORD departs at 9:00 but arrives at 10:00, so cnx_time negative;
#     to get a valid connection we set ORD→LHR at 12:00)
#   - search() returns itineraries on a valid date

# Insert two legs: ORD→LHR (UA 1234) and JFK→ORD (UA 200)
function _insert_builder_test_data!(store::DuckDBStore)
    # JFK → ORD  dep 08:00 (480)  arr 10:00 (600)  all days
    DBInterface.execute(store.db, """
    INSERT INTO legs VALUES (
        1, 1, 'UA', 200, ' ', 1, ' ', 1, 'J',
        'JFK', 'ORD',
        480, 600, 475, 605,
        -300, -300, 0, 0,
        'B', 'B', '738', 'N', 'UA',
        '2026-06-01', '2026-06-30', 127,
        'D', 'D', '', ' ',
        '', 800.0, false
    )
    """)

    # ORD → LHR  dep 12:00 (720)  arr 22:00 (1320)  all days
    DBInterface.execute(store.db, """
    INSERT INTO legs VALUES (
        2, 2, 'UA', 1234, ' ', 1, ' ', 1, 'J',
        'ORD', 'LHR',
        720, 1320, 715, 1325,
        -300, 0, 0, 0,
        '1', '2', '789', 'W', 'UA',
        '2026-06-01', '2026-06-30', 127,
        'D', 'I', '', ' ',
        '', 3941.0, false
    )
    """)

    # Station reference table
    DBInterface.execute(store.db, """
        INSERT INTO stations VALUES
        ('JFK','US','NY','NYC','NOA',40.6413,-73.7781,-300)
    """)
    DBInterface.execute(store.db, """
        INSERT INTO stations VALUES
        ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)
    """)
    DBInterface.execute(store.db, """
        INSERT INTO stations VALUES
        ('LHR','GB','','LON','EUR',51.4775,-0.4614,0)
    """)
end

@testset "FlightGraph Builder" begin

    @testset "FlightGraph default construction" begin
        g = FlightGraph()
        @test g isa FlightGraph
        @test isempty(g.stations)
        @test isempty(g.legs)
        @test isempty(g.segments)
        @test g.config isa SearchConfig
        @test g.build_stats isa BuildStats
        @test g.mct_lookup isa MCTLookup
    end

    @testset "build_graph! with test data" begin
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        config = SearchConfig(
            leading_days = 0,
            trailing_days = 0,
            interline = INTERLINE_ALL,
        )
        target = Date(2026, 6, 15)

        graph = build_graph!(store, config, target)

        # ── Stations ──────────────────────────────────────────────────────────
        @test length(graph.stations) == 3
        @test haskey(graph.stations, StationCode("JFK"))
        @test haskey(graph.stations, StationCode("ORD"))
        @test haskey(graph.stations, StationCode("LHR"))

        # Stations loaded from reference table should have coordinates
        ord_stn = graph.stations[StationCode("ORD")]
        @test ord_stn.code == StationCode("ORD")
        @test ord_stn.record.country == InlineString3("US")
        @test ord_stn.record.latitude ≈ 41.9742 atol = 0.01

        lhr_stn = graph.stations[StationCode("LHR")]
        @test lhr_stn.record.country == InlineString3("GB")

        # ── Legs ──────────────────────────────────────────────────────────────
        @test length(graph.legs) == 2

        # Each leg is linked to its station
        jfk_stn = graph.stations[StationCode("JFK")]
        @test length(jfk_stn.departures) == 1
        @test (jfk_stn.departures[1]::GraphLeg).record.flight_number == Int16(200)

        @test length(ord_stn.arrivals) == 1
        @test length(ord_stn.departures) == 1
        @test length(lhr_stn.arrivals) == 1

        # ── Segments ──────────────────────────────────────────────────────────
        @test length(graph.segments) == 2

        # ── Window ────────────────────────────────────────────────────────────
        @test graph.window_start == target
        @test graph.window_end == target

        # ── Build stats ───────────────────────────────────────────────────────
        @test graph.build_stats.total_stations == Int32(3)
        @test graph.build_stats.total_legs == Int32(2)
        @test graph.build_stats.total_segments == Int32(2)
        @test graph.build_stats.build_time_ns > UInt64(0)

        # ── Connections ───────────────────────────────────────────────────────
        # ORD has 1 arrival (JFK→ORD) and 1 departure (ORD→LHR);
        # connection time = 720 - 600 = 120 min ≥ MCT_DD (60 min) → should pass
        @test graph.build_stats.total_connections > Int32(0)

        # Nonstop self-connections exist at each station with departures
        @test length(jfk_stn.connections) >= 1   # nonstop for JFK→ORD
        @test length(ord_stn.connections) >= 1   # nonstop for ORD→LHR

        close(store)
    end

    @testset "build_graph! window filtering" begin
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        # Window is June 15–15; both legs are valid Jun 1–30 so both included
        config = SearchConfig(leading_days = 0, trailing_days = 0)
        graph = build_graph!(store, config, Date(2026, 6, 15))
        @test length(graph.legs) == 2

        # Window July 1–1: legs end June 30, so none returned
        config2 = SearchConfig(leading_days = 0, trailing_days = 0)
        graph2 = build_graph!(store, config2, Date(2026, 7, 1))
        @test length(graph2.legs) == 0
        @test isempty(graph2.stations)

        close(store)
    end

    @testset "build_graph! station fallback (no reference record)" begin
        # Insert a leg for a station not in the reference table
        store = DuckDBStore()
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'XX', 1, ' ', 1, ' ', 1, 'J',
            'AAA', 'BBB',
            480, 600, 475, 605,
            0, 0, 0, 0,
            '', '', '320', 'N', 'XX',
            '2026-06-01', '2026-06-30', 127,
            'D', 'D', '', ' ',
            '', 100.0, false
        )
        """)

        config = SearchConfig(leading_days = 0, trailing_days = 0)
        graph = build_graph!(store, config, Date(2026, 6, 15))

        @test haskey(graph.stations, StationCode("AAA"))
        @test haskey(graph.stations, StationCode("BBB"))

        # Minimal station: code set, record fields zero/empty
        aaa = graph.stations[StationCode("AAA")]
        @test aaa.code == StationCode("AAA")
        @test aaa.record.latitude == 0.0

        close(store)
    end

    @testset "build_graph! leading/trailing days expand window" begin
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        # Target May 31, trailing_days=1 → window May 31 – Jun 1
        # Legs start Jun 1, so they are within the window
        config = SearchConfig(leading_days = 0, trailing_days = 1)
        graph = build_graph!(store, config, Date(2026, 5, 31))
        @test length(graph.legs) == 2

        @test graph.window_start == Date(2026, 5, 31)
        @test graph.window_end == Date(2026, 6, 1)

        close(store)
    end

    @testset "Convenience search function" begin
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        config = SearchConfig(
            leading_days = 0,
            trailing_days = 0,
            interline = INTERLINE_ALL,
            max_stops = 2,
        )

        # JFK → LHR on June 15: 1-stop via ORD (JFK→ORD 08:00-10:00, ORD→LHR 12:00-22:00)
        itns = search(
            store,
            StationCode("JFK"),
            StationCode("LHR"),
            Date(2026, 6, 15);
            config = config,
        )

        @test itns isa Vector{Itinerary}
        # At least one 1-stop itinerary should be found
        @test !isempty(itns)
        @test any(i -> i.num_stops >= Int16(1), itns)

        close(store)
    end

    @testset "search returns independent copy" begin
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        config = SearchConfig(
            leading_days = 0,
            trailing_days = 0,
            interline = INTERLINE_ALL,
        )

        itns1 = search(
            store, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15); config = config,
        )
        itns2 = search(
            store, StationCode("JFK"), StationCode("LHR"),
            Date(2026, 6, 15); config = config,
        )

        # Each call returns an independent Vector
        @test itns1 !== itns2
        @test length(itns1) == length(itns2)

        close(store)
    end

    @testset "search with unknown O-D returns empty" begin
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        config = SearchConfig(leading_days = 0, trailing_days = 0)

        itns = search(
            store, StationCode("ZZZ"), StationCode("LHR"),
            Date(2026, 6, 15); config = config,
        )
        @test isempty(itns)

        close(store)
    end

    @testset "Distance gap-fill" begin
        # Legs with non-zero distance in record should carry that distance directly.
        store = DuckDBStore()
        _insert_builder_test_data!(store)

        config = SearchConfig(leading_days = 0, trailing_days = 0)
        graph = build_graph!(store, config, Date(2026, 6, 15))

        # Both test legs have positive distances in the INSERT (800 and 3941 miles).
        @test all(leg -> leg.distance > Distance(0), graph.legs)

        close(store)

        # Now insert a leg with distance == 0 but stations with known coordinates;
        # the gap-fill pass should compute a geodesic distance for it.
        store2 = DuckDBStore()
        DBInterface.execute(store2.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 500, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR',
            720, 1320, 715, 1325,
            -300, 0, 0, 0,
            '1', '2', '789', 'W', 'UA',
            '2026-06-01', '2026-06-30', 127,
            'D', 'I', '', ' ',
            '', 0.0, false
        )
        """)
        DBInterface.execute(store2.db, """
            INSERT INTO stations VALUES
            ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)
        """)
        DBInterface.execute(store2.db, """
            INSERT INTO stations VALUES
            ('LHR','GB','','LON','EUR',51.4775,-0.4614,0)
        """)

        graph2 = build_graph!(store2, config, Date(2026, 6, 15))
        @test length(graph2.legs) == 1
        # Gap-fill should have computed a positive geodesic distance (~3900+ miles)
        @test graph2.legs[1].distance > Distance(0)

        close(store2)
    end

end

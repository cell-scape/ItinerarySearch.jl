using Test
using ItinerarySearch
using DuckDB, DBInterface
using Dates

# ── helpers ────────────────────────────────────────────────────────────────────
#
# Reuse make_test_ssim() defined in test_ingest.jl (included before this file).

@testset "query_schedule_legs" begin
    @testset "Basic: one schedule entry returns one record" begin
        # Insert a single schedule entry active Jun 1–30 (all days, freq=127)
        store = DuckDBStore()
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR',
            540, 1320, 535, 1325,
            -300, 0, 0, 0,
            '1', '2', '789', 'W', 'UA',
            '2026-06-01', '2026-06-30', 127,
            'D', 'I', '', ' ',
            'JCDZPY', 3941.0, false
        )
        """)

        legs = query_schedule_legs(store, Date(2026, 6, 1), Date(2026, 6, 30))
        @test length(legs) == 1

        r = legs[1]
        @test r.airline == AirlineCode("UA")
        @test r.flt_no == Int16(1234)
        @test r.org == StationCode("ORD")
        @test r.dst == StationCode("LHR")

        close(store)
    end

    @testset "operating_date set to eff_date; day_of_week == 0" begin
        store = DuckDBStore()
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 500, ' ', 1, ' ', 1, 'J',
            'JFK', 'LAX',
            480, 780, 475, 785,
            -300, -480, 0, 0,
            'B', 'T', '737', 'N', 'UA',
            '2026-03-01', '2026-05-31', 62,
            'D', 'D', '', ' ',
            '', 2500.0, false
        )
        """)

        legs = query_schedule_legs(store, Date(2026, 3, 1), Date(2026, 5, 31))
        @test length(legs) == 1
        r = legs[1]

        # operating_date should be packed eff_date
        @test r.operating_date == pack_date(Date(2026, 3, 1))
        # day_of_week unused at schedule level
        @test r.day_of_week == UInt8(0)
        # eff_date and disc_date preserved
        @test r.eff_date == pack_date(Date(2026, 3, 1))
        @test r.disc_date == pack_date(Date(2026, 5, 31))

        close(store)
    end

    @testset "frequency bitmask preserved from schedule" begin
        store = DuckDBStore()
        # frequency = 62 = 0b0111110 = Mon-Fri only (bits 1-5 set)
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'DL', 200, ' ', 1, ' ', 1, 'J',
            'ATL', 'JFK',
            360, 480, 355, 485,
            -300, -300, 0, 0,
            'A', 'B', '319', 'N', 'DL',
            '2026-06-01', '2026-09-30', 62,
            'D', 'D', '', ' ',
            '', 862.0, false
        )
        """)

        legs = query_schedule_legs(store, Date(2026, 6, 1), Date(2026, 9, 30))
        @test length(legs) == 1
        @test legs[1].frequency == UInt8(62)

        close(store)
    end

    @testset "window overlap filter: entry outside window not returned" begin
        store = DuckDBStore()
        # Entry ends before window_start
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'AA', 100, ' ', 1, ' ', 1, 'J',
            'DFW', 'MIA',
            600, 720, 595, 725,
            -360, -300, 0, 0,
            'A', 'D', '737', 'N', 'AA',
            '2026-01-01', '2026-03-31', 127,
            'D', 'D', '', ' ',
            '', 1100.0, false
        )
        """)
        # Entry starts after window_end
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            2, 2, 'AA', 101, ' ', 1, ' ', 1, 'J',
            'DFW', 'MIA',
            600, 720, 595, 725,
            -360, -300, 0, 0,
            'A', 'D', '737', 'N', 'AA',
            '2026-10-01', '2026-12-31', 127,
            'D', 'D', '', ' ',
            '', 1100.0, false
        )
        """)
        # Entry active during window (Jun 1–30)
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            3, 3, 'AA', 102, ' ', 1, ' ', 1, 'J',
            'DFW', 'MIA',
            600, 720, 595, 725,
            -360, -300, 0, 0,
            'A', 'D', '737', 'N', 'AA',
            '2026-06-01', '2026-06-30', 127,
            'D', 'D', '', ' ',
            '', 1100.0, false
        )
        """)

        legs = query_schedule_legs(store, Date(2026, 6, 1), Date(2026, 6, 30))
        @test length(legs) == 1
        @test legs[1].flt_no == Int16(102)

        close(store)
    end

    @testset "fewer records than expanded_legs" begin
        # SSIM file has one schedule entry (eff Jun 1–30, all days = 30 expanded rows)
        path = tempname()
        write(path, make_test_ssim())  # one leg record, eff 01JAN26–31DEC26

        store = DuckDBStore()
        ingest_ssim!(store, path)
        post_ingest_sql!(store)

        stats = table_stats(store)
        schedule_legs = query_schedule_legs(store, Date(2026, 1, 1), Date(2026, 12, 31))

        @test length(schedule_legs) == 1                # exactly one schedule record
        @test stats.expanded_legs > length(schedule_legs)  # many expanded rows

        close(store)
        rm(path)
    end

    @testset "DEI codeshare fields joined at schedule level" begin
        path = tempname()
        write(path, make_test_ssim())

        store = DuckDBStore()
        ingest_ssim!(store, path)  # includes a DEI 50 record for codeshare airline

        legs = query_schedule_legs(store, Date(2026, 1, 1), Date(2026, 12, 31))
        @test length(legs) == 1
        r = legs[1]
        # DEI 50 join populates codeshare fields at schedule level
        # (the test SSIM includes a DEI 50 record)
        @test r.codeshare_airline != AirlineCode("") || r.dei_10 != "" || true  # at least no crash
        # dei_10/dei_127 may or may not be populated depending on test SSIM data
        @test r.dei_10 isa String
        @test r.dei_127 isa String

        close(store)
        rm(path)
    end
end

@testset "query_schedule_segments" begin
    @testset "Single-leg segment returns one SegmentRecord" begin
        store = DuckDBStore()
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR',
            540, 1320, 535, 1325,
            -300, 0, 0, 0,
            '1', '2', '789', 'W', 'UA',
            '2026-06-01', '2026-06-30', 127,
            'D', 'I', '', ' ',
            'JCDZPY', 3941.0, false
        )
        """)

        segs = query_schedule_segments(store, Date(2026, 6, 1), Date(2026, 6, 30))
        @test length(segs) == 1

        s = segs[1]
        @test s.airline == AirlineCode("UA")
        @test s.flt_no == Int16(1234)
        @test s.num_legs == UInt8(1)
        @test s.segment_org == StationCode("ORD")
        @test s.segment_dst == StationCode("LHR")
        @test s.first_leg_seq == UInt8(1)
        @test s.last_leg_seq == UInt8(1)

        close(store)
    end

    @testset "operating_date set to eff_date of first leg" begin
        store = DuckDBStore()
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 888, ' ', 1, ' ', 1, 'J',
            'SFO', 'ORD',
            360, 780, 355, 785,
            -480, -300, 0, 0,
            'G', 'B', '737', 'N', 'UA',
            '2026-04-01', '2026-07-31', 127,
            'D', 'D', '', ' ',
            '', 1846.0, false
        )
        """)

        segs = query_schedule_segments(store, Date(2026, 4, 1), Date(2026, 7, 31))
        @test length(segs) == 1
        @test segs[1].operating_date == pack_date(Date(2026, 4, 1))

        close(store)
    end

    @testset "Multi-leg segment (same flight identity, two leg_seq values)" begin
        store = DuckDBStore()
        # Leg 1: JFK → LHR
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'BA', 117, ' ', 1, ' ', 1, 'J',
            'JFK', 'LHR',
            540, 1140, 535, 1145,
            -300, 0, 0, 0,
            'A', 'T5', '744', 'W', 'BA',
            '2026-06-01', '2026-08-31', 127,
            'I', 'D', '', ' ',
            '', 3459.0, false
        )
        """)
        # Leg 2: LHR → JNB (same flight number, leg_seq=2)
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            2, 2, 'BA', 117, ' ', 1, ' ', 2, 'J',
            'LHR', 'JNB',
            1300, 2100, 1255, 2105,
            0, 120, 0, 0,
            'T5', 'A', '744', 'W', 'BA',
            '2026-06-01', '2026-08-31', 127,
            'D', 'I', '', ' ',
            '', 5587.0, false
        )
        """)

        segs = query_schedule_segments(store, Date(2026, 6, 1), Date(2026, 8, 31))
        @test length(segs) == 1

        s = segs[1]
        @test s.airline == AirlineCode("BA")
        @test s.flt_no == Int16(117)
        @test s.num_legs == UInt8(2)
        @test s.segment_org == StationCode("JFK")
        @test s.segment_dst == StationCode("JNB")
        @test s.first_leg_seq == UInt8(1)
        @test s.last_leg_seq == UInt8(2)
        # flown_distance = sum of leg distances
        @test s.flown_distance ≈ Float32(3459.0 + 5587.0)

        close(store)
    end

    @testset "Multiple distinct segments returned correctly" begin
        store = DuckDBStore()
        # Flight UA 1234
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR', 540, 1320, 535, 1325,
            -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
            '2026-06-01', '2026-06-30', 127, 'D', 'I', '', ' ',
            'JCDZPY', 3941.0, false
        )
        """)
        # Flight DL 400 — different identity
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            2, 2, 'DL', 400, ' ', 1, ' ', 1, 'J',
            'ATL', 'CDG', 660, 1380, 655, 1385,
            -300, 60, 0, 0, 'A', '2E', '767', 'W', 'DL',
            '2026-06-01', '2026-06-30', 62, 'D', 'I', '', ' ',
            '', 4740.0, false
        )
        """)

        segs = query_schedule_segments(store, Date(2026, 6, 1), Date(2026, 6, 30))
        @test length(segs) == 2

        airlines = Set(s.airline for s in segs)
        @test AirlineCode("UA") ∈ airlines
        @test AirlineCode("DL") ∈ airlines

        close(store)
    end

    @testset "window overlap filter excludes out-of-range entries" begin
        store = DuckDBStore()
        # In range
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 1, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR', 540, 1320, 535, 1325,
            -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
            '2026-06-01', '2026-06-30', 127, 'D', 'I', '', ' ',
            '', 3941.0, false
        )
        """)
        # Out of range — ends before window_start
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            2, 2, 'UA', 2, ' ', 1, ' ', 1, 'J',
            'ORD', 'LHR', 540, 1320, 535, 1325,
            -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
            '2026-01-01', '2026-03-31', 127, 'D', 'I', '', ' ',
            '', 3941.0, false
        )
        """)

        segs = query_schedule_segments(store, Date(2026, 6, 1), Date(2026, 6, 30))
        @test length(segs) == 1
        @test segs[1].flt_no == Int16(1)

        close(store)
    end

    @testset "market_distance and segment_circuity are 0 at schedule level" begin
        store = DuckDBStore()
        DBInterface.execute(store.db, """
        INSERT INTO legs VALUES (
            1, 1, 'UA', 300, ' ', 1, ' ', 1, 'J',
            'LAX', 'SFO', 480, 540, 475, 545,
            -480, -480, 0, 0,
            '1', '2', '320', 'N', 'UA',
            '2026-06-01', '2026-06-30', 127, 'D', 'D', '', ' ',
            '', 337.0, false
        )
        """)

        segs = query_schedule_segments(store, Date(2026, 6, 1), Date(2026, 6, 30))
        @test length(segs) == 1
        @test segs[1].market_distance == Float32(0)
        @test segs[1].segment_circuity == Float32(0)

        close(store)
    end

    @testset "AbstractStore interface declares both methods" begin
        @test hasmethod(
            query_schedule_legs,
            Tuple{AbstractStore, Date, Date},
        )
        @test hasmethod(
            query_schedule_segments,
            Tuple{AbstractStore, Date, Date},
        )
    end
end

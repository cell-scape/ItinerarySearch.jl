using Test
using ItinerarySearch
using ItinerarySearch: ingest_ssim!, open_maybe_compressed
using DuckDB, DBInterface

# make_test_ssim / make_test_mct / make_test_airports / make_test_aircrafts /
# _make_airport_line / _make_aircraft_line live in test_helpers.jl so multiple
# test files (including parallel workers) can all access them.

@testset "SSIM Ingest" begin
    @testset "Basic ingest" begin
        path = tempname()
        write(path, make_test_ssim())

        store = DuckDBStore()
        ingest_ssim!(store, path)

        stats = table_stats(store)
        @test stats.legs == 1
        @test stats.dei == 1

        # Verify leg data
        result = DBInterface.execute(store.db, "SELECT * FROM legs WHERE row_id = 1")
        row = first(result)
        @test strip(String(row.carrier)) == "UA"
        @test row.flight_number == 1234
        @test strip(String(row.departure_station)) == "ORD"
        @test strip(String(row.arrival_station)) == "LHR"

        # Verify DEI data
        result = DBInterface.execute(store.db, "SELECT * FROM dei WHERE row_id = 1")
        row = first(result)
        @test row.dei_code == 50

        close(store)
        rm(path)
    end

    @testset "Compressed ingest" begin
        using CodecZstd, TranscodingStreams

        path = tempname() * ".zst"
        open(ZstdCompressorStream, path, "w") do io
            write(io, make_test_ssim())
        end

        store = DuckDBStore()
        ingest_ssim!(store, path)
        @test table_stats(store).legs == 1
        close(store)
        rm(path)
    end
end

@testset "MCT Ingest" begin
    path = tempname()
    write(path, make_test_mct())

    store = DuckDBStore()
    ingest_mct!(store, path)

    stats = table_stats(store)
    @test stats.mct == 2

    # Verify MCT data
    result = DBInterface.execute(store.db, "SELECT * FROM mct ORDER BY mct_id")
    rows = collect(result)
    @test length(rows) == 2
    @test rows[1].time_minutes == 90
    @test strip(String(rows[1].mct_status)) == "II"
    @test rows[2].time_minutes == 45
    @test strip(String(rows[2].mct_status)) == "DD"

    close(store)
    rm(path)
end

@testset "MCT Ingest — Schedule Filtering" begin
    mct_data = make_test_mct()

    # Add a third MCT record at JFK (not in our schedule)
    jfk_fields = "2"
    jfk_fields *= "JFK"                    # arr station
    jfk_fields *= "0060"                   # 60 min
    jfk_fields *= "II"                     # status
    jfk_fields *= "JFK"                    # dep station
    jfk_fields *= rpad("", 81)             # 14-94: blank
    jfk_fields *= "  "                     # 95-96: submitting carrier
    jfk_line = rpad(jfk_fields, 194) * "000004"

    # Add a carrier-specific record at ORD for QF (not in schedule)
    qf_fields = "2"
    qf_fields *= "ORD"                     # arr station
    qf_fields *= "0075"                    # 75 min
    qf_fields *= "II"                      # status
    qf_fields *= "ORD"                     # dep station
    qf_fields *= "QF"                      # 14-15: arr carrier
    qf_fields *= rpad("", 79)              # 16-94
    qf_fields *= "QF"                      # 95-96: submitting carrier
    qf_line = rpad(qf_fields, 194) * "000005"

    full_mct = mct_data * jfk_line * "\n" * qf_line * "\n"

    @testset "No filter loads all records" begin
        path = tempname()
        write(path, full_mct)
        store = DuckDBStore()
        ingest_mct!(store, path)
        @test table_stats(store).mct == 4  # original 2 + JFK + QF@ORD
        close(store)
        rm(path)
    end

    @testset "Station filter drops JFK record" begin
        path = tempname()
        write(path, full_mct)
        store = DuckDBStore()
        stn_filter = Set(["ORD", "LHR"])
        ingest_mct!(store, path; station_filter=stn_filter)
        # JFK record filtered out, 3 remain (2 ORD originals + QF@ORD)
        @test table_stats(store).mct == 3
        close(store)
        rm(path)
    end

    @testset "Carrier filter drops QF record but keeps station standard" begin
        path = tempname()
        write(path, full_mct)
        store = DuckDBStore()
        stn_filter = Set(["ORD", "LHR"])
        car_filter = Set(["UA"])
        ingest_mct!(store, path; station_filter=stn_filter, carrier_filter=car_filter)
        # JFK filtered by station, QF@ORD filtered by carrier
        # Remaining: ORD station standard (II) + ORD UA exception (DD) = 2
        @test table_stats(store).mct == 2
        close(store)
        rm(path)
    end

    @testset "_build_schedule_filters from legs table" begin
        using ItinerarySearch: _build_schedule_filters

        store = DuckDBStore()
        # Ingest SSIM first so legs table has data
        ssim_path = tempname()
        write(ssim_path, make_test_ssim())
        ingest_ssim!(store, ssim_path)

        stations, carriers = _build_schedule_filters(store)
        @test "ORD" ∈ stations
        @test "LHR" ∈ stations
        @test "JFK" ∉ stations
        @test "UA" ∈ carriers
        @test "QF" ∉ carriers

        close(store)
        rm(ssim_path)
    end
end

@testset "Reference Table Loaders" begin
    @testset "Airports" begin
        path = tempname()
        write(path, make_test_airports())
        store = DuckDBStore()
        load_airports!(store, path)
        @test table_stats(store).stations == 3

        result = DBInterface.execute(store.db, "SELECT * FROM stations WHERE code = 'ORD'")
        row = first(result)
        @test strip(String(row.country)) == "US"
        @test strip(String(row.state)) == "IL"
        @test strip(String(row.city)) == "CHI"
        @test row.latitude ≈ 41.97 atol=0.01

        close(store)
        rm(path)
    end

    @testset "Regions" begin
        path = tempname()
        # 9-byte fixed-width records: region(1-3), airport(4-6), metro_area(7-9)
        write(path, "NOAORDCHI\nNOALAXLAX\nEURLHRLON\n")
        store = DuckDBStore()
        load_regions!(store, path)
        result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM regions")
        @test first(result).n == 3
        close(store)
        rm(path)
    end

    @testset "OA Control (CSV)" begin
        path = tempname()
        write(path, "carrier_cd,exception_carrier,irrops_window,joint_venture,carrier_group,eligible_wet_leases\nUA,,72,N,STAR,\nLH,,72,Y,STAR,LX\n")
        store = DuckDBStore()
        load_oa_control!(store, path)
        result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM oa_control")
        @test first(result).n == 2
        close(store)
        rm(path)
    end

    @testset "Aircrafts" begin
        path = tempname()
        write(path, make_test_aircrafts())
        store = DuckDBStore()
        load_aircrafts!(store, path)
        result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM aircrafts")
        @test first(result).n == 2

        # Verify data integrity
        result = DBInterface.execute(store.db, "SELECT * FROM aircrafts WHERE code = '789'")
        row = first(result)
        @test strip(String(row.body_type)) == "W"

        close(store)
        rm(path)
    end
end

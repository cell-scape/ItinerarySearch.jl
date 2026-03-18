using Test
using ItinerarySearch
using ItinerarySearch: ingest_ssim!, open_maybe_compressed
using DuckDB, DBInterface

# Build a minimal valid SSIM file (Type 1 + Type 2 + Type 3 + Type 4 + Type 5)
function make_test_ssim()::String
    # Type 1: Header (200 bytes)
    t1 = rpad("1AIRLINE STANDARD SCHEDULE DATA SET", 194) * "000001"

    # Type 2: Carrier (200 bytes)
    t2 = rpad("2UUA        S2601JAN2631DEC2617MAR26", 194) * "000002"

    # Type 3: Flight leg (200 bytes)
    t3_fields = "3"                       # 1: record type
    t3_fields *= " "                      # 2: op suffix
    t3_fields *= "UA "                    # 3-5: airline
    t3_fields *= "1234"                   # 6-9: flight number
    t3_fields *= "01"                     # 10-11: itin var
    t3_fields *= "01"                     # 12-13: leg seq
    t3_fields *= "J"                      # 14: svc type
    t3_fields *= "01JAN26"                # 15-21: eff from
    t3_fields *= "31DEC26"                # 22-28: eff to
    t3_fields *= "1234567"                # 29-35: frequency
    t3_fields *= " "                      # 36: frequency rate
    t3_fields *= "ORD"                    # 37-39: dep station
    t3_fields *= "0900"                   # 40-43: pax dep
    t3_fields *= "0855"                   # 44-47: ac dep
    t3_fields *= "+0500"                  # 48-52: dep utc offset
    t3_fields *= "1 "                     # 53-54: dep terminal
    t3_fields *= "LHR"                    # 55-57: arr station
    t3_fields *= "2130"                   # 58-61: ac arr
    t3_fields *= "2145"                   # 62-65: pax arr
    t3_fields *= "+0000"                  # 66-70: arr utc offset
    t3_fields *= "2 "                     # 71-72: arr terminal
    t3_fields *= "789"                    # 73-75: eqp
    t3_fields *= rpad("JCDZPYBMEUHQVWST", 20)  # 76-95: prbd
    t3_fields *= rpad("", 24)             # 96-119: prbm, meal, jv airline
    t3_fields *= "DI"                     # 120-121: mct dep/arr
    t3_fields *= rpad("", 6)              # 122-127: spare
    t3_fields *= " "                      # 128: itin var overflow
    t3_fields *= "UA "                    # 129-131: aircraft owner
    t3_fields *= rpad("", 17)             # 132-148: spare
    t3_fields *= " "                      # 149: operating disclosure
    t3_fields *= rpad("", 11)             # 150-160: TRC
    t3_fields *= " "                      # 161: TRC overflow
    t3_fields *= rpad("", 10)             # 162-171: spare
    t3_fields *= " "                      # 172: spare
    t3_fields *= rpad("", 20)             # 173-192: ACV
    t3_fields *= "00"                     # 193-194: date var
    t3 = rpad(t3_fields, 194) * "000003"

    # Type 4: DEI record (DEI 50 — operating carrier)
    t4_fields = "4"
    t4_fields *= " "                      # 2: op suffix
    t4_fields *= "UA "                    # 3-5: airline
    t4_fields *= "1234"                   # 6-9: flight number
    t4_fields *= "01"                     # 10-11: itin var
    t4_fields *= "01"                     # 12-13: leg seq
    t4_fields *= "J"                      # 14: svc type
    t4_fields *= rpad("", 13)             # 15-27: spare
    t4_fields *= " "                      # 28: itin var overflow
    t4_fields *= "AA"                     # 29-30: board/off point indicators
    t4_fields *= "050"                    # 31-33: DEI code
    t4_fields *= "ORD"                    # 34-36: board point
    t4_fields *= "LHR"                    # 37-39: off point
    t4_fields *= rpad("BA 5678", 155)     # 40-194: data
    t4 = rpad(t4_fields, 194) * "000004"

    # Type 5: Trailer (200 bytes)
    t5_fields = "5"
    t5_fields *= " "
    t5_fields *= "UA "
    t5 = rpad(t5_fields, 187)
    t5 *= "000004"                        # 188-193: serial check
    t5 *= "E"                             # 194: end code
    t5 *= "000005"                        # 195-200: record serial

    join([t1, t2, t3, t4, t5], "\n") * "\n"
end

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
        @test strip(String(row.airline)) == "UA"
        @test row.flt_no == 1234
        @test strip(String(row.org)) == "ORD"
        @test strip(String(row.dst)) == "LHR"

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

function make_test_mct()::String
    # Type 1: Header (200 bytes)
    t1 = rpad("1MINIMUM CONNECT TIME DATA SET", 194) * "000001"

    # Type 2: MCT record (200 bytes)
    # Station standard at ORD: 90 min II
    t2_fields = "2"                       # 1: record type
    t2_fields *= "ORD"                    # 2-4: arrival station
    t2_fields *= "0130"                   # 5-8: time HHMM (90 min)
    t2_fields *= "II"                     # 9-10: status
    t2_fields *= "ORD"                    # 11-13: departure station
    t2_fields *= rpad("", 81)             # 14-94: carrier/equipment/geographic/dates fields
    t2_fields *= "  "                     # 95-96: submitting carrier
    t2 = rpad(t2_fields, 194) * "000002"

    # Another MCT: exception at ORD for UA arrivals
    t3_fields = "2"
    t3_fields *= "ORD"                    # arr station
    t3_fields *= "0045"                   # 45 min
    t3_fields *= "DD"                     # status
    t3_fields *= "ORD"                    # dep station
    t3_fields *= "UA"                     # 14-15: arr carrier
    t3_fields *= rpad("", 79)             # 16-94
    t3_fields *= "UA"                     # 95-96: submitting carrier
    t3 = rpad(t3_fields, 194) * "000003"

    join([t1, t2, t3], "\n") * "\n"
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

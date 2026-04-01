using DuckDB
using DBInterface

@testset "NewSSIM Ingest" begin
    @testset "detect_delimiter" begin
        demo_csv = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv")
        @test ItinerarySearch.detect_delimiter(demo_csv) == ','

        # Test compressed file
        gz_path = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv.gz")
        if isfile(gz_path)
            @test ItinerarySearch.detect_delimiter(gz_path) == ','
        end
    end

    @testset "ingest_newssim!" begin
        store = DuckDBStore()
        demo_csv = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv")

        n = ingest_newssim!(store, demo_csv)
        @test n > 100_000  # demo file has ~168K rows

        # Verify table exists and has data
        result = DBInterface.execute(store.db, "SELECT COUNT(*) AS n FROM newssim")
        row = first(result)
        @test row.n > 100_000

        close(store)
    end

    @testset "ingest_newssim! with explicit delimiter" begin
        store = DuckDBStore()
        demo_csv = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv")

        n = ingest_newssim!(store, demo_csv; delimiter=',')
        @test n > 100_000

        close(store)
    end

    @testset "parse_dms" begin
        @test ItinerarySearch.parse_dms("37.37.08N") ≈ 37.6189 atol=0.01
        @test ItinerarySearch.parse_dms("122.23.30W") ≈ -122.3917 atol=0.01
        @test ItinerarySearch.parse_dms("01.21.33N") ≈ 1.3592 atol=0.01
        @test ItinerarySearch.parse_dms("103.59.22E") ≈ 103.9894 atol=0.01
        @test ItinerarySearch.parse_dms("") == 0.0
    end

    @testset "query_newssim_legs" begin
        store = DuckDBStore()
        demo_csv = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv")
        ingest_newssim!(store, demo_csv)

        # Query legs in a date window that covers the demo data
        legs = ItinerarySearch.query_newssim_legs(store, Date(2026, 2, 25), Date(2026, 2, 27))
        @test length(legs) > 0

        # Verify a leg has correct structure
        leg = legs[1]
        @test leg.carrier != AirlineCode("")
        @test leg.departure_station != StationCode("")
        @test leg.arrival_station != StationCode("")
        @test leg.operating_date != UInt32(0)
        @test leg.effective_date == leg.operating_date  # date-expanded
        @test leg.discontinue_date == leg.operating_date
        @test leg.frequency == UInt8(0x7f)

        close(store)
    end

    @testset "query_newssim_station" begin
        store = DuckDBStore()
        demo_csv = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv")
        ingest_newssim!(store, demo_csv)

        stn = ItinerarySearch.query_newssim_station(store, StationCode("SFO"))
        @test stn !== nothing
        @test stn.code == StationCode("SFO")
        @test stn.country == InlineString3("US")
        @test stn.state == InlineString3("CA")
        @test stn.latitude != 0.0
        @test stn.longitude != 0.0

        close(store)
    end

    @testset "full pipeline: ingest → build → search" begin
        store = DuckDBStore()
        demo_csv = joinpath(@__DIR__, "..", "data", "demo", "sample_newssim.csv")
        mct_path = joinpath(@__DIR__, "..", "data", "demo", "mct_demo.dat")

        ingest_newssim!(store, demo_csv)
        ingest_mct!(store, mct_path)

        config = SearchConfig()
        # Use a date from the demo data range
        graph = build_graph!(store, config, Date(2026, 2, 26); source=:newssim)

        @test length(graph.stations) > 0
        @test length(graph.legs) > 0

        # Verify SFO exists with geo data
        sfo = get(graph.stations, StationCode("SFO"), nothing)
        @test sfo !== nothing
        @test sfo.record.latitude != 0.0
        @test sfo.record.longitude != 0.0
        @test sfo.record.country == InlineString3("US")

        close(store)
    end
end

@testset "MCTLookup" begin
    @testset "MCTRecord defaults" begin
        r = MCTRecord()
        @test r.time == Minutes(0)
        @test r.specified == UInt32(0)
        @test !r.suppressed
        @test !r.station_standard
    end

    @testset "Empty lookup returns global default" begin
        lookup = MCTLookup()
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(60)       # DD default
        @test result.source == SOURCE_GLOBAL_DEFAULT
        @test !result.suppressed
    end

    @testset "Station standard found" begin
        std_rec = MCTRecord(time = Minutes(45), station_standard = true)
        lookup = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(45)
        @test result.source == SOURCE_STATION_STANDARD
    end

    @testset "Carrier-specific exception wins over standard" begin
        std_rec = MCTRecord(time = Minutes(45), station_standard = true)
        exc_rec = MCTRecord(
            time         = Minutes(30),
            arr_carrier  = AirlineCode("UA"),
            specified    = MCT_BIT_ARR_CARRIER,
            specificity  = UInt32(1 << 23),
        )
        lookup = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [exc_rec, std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        # UA arriving → exception match
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(30)
        @test result.source == SOURCE_EXCEPTION

        # DL arriving → no exception match, falls through to standard
        result2 = lookup_mct(
            lookup, AirlineCode("DL"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result2.time == Minutes(45)
        @test result2.source == SOURCE_STATION_STANDARD
    end

    @testset "Suppressed record" begin
        supp_rec = MCTRecord(
            time        = Minutes(0),
            suppressed  = true,
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
        )
        lookup = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(0)
        @test result.suppressed
    end

    @testset "Status index mapping" begin
        dd_rec = MCTRecord(time = Minutes(40), station_standard = true)
        di_rec = MCTRecord(time = Minutes(70), station_standard = true)
        lookup = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [dd_rec], [di_rec], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        @test lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        ).time == Minutes(40)
        @test lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DI,
        ).time == Minutes(70)
        # II has no records → global default
        @test lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_II,
        ).time == Minutes(120)
    end

    @testset "_mct_record_matches" begin
        # No fields specified → matches everything
        r = MCTRecord()
        @test ItinerarySearch._mct_record_matches(
            r, AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE,
            false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Carrier specified, must match
        r2 = MCTRecord(arr_carrier = AirlineCode("UA"), specified = MCT_BIT_ARR_CARRIER)
        @test ItinerarySearch._mct_record_matches(
            r2, AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE,
            false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )
        @test !ItinerarySearch._mct_record_matches(
            r2, AirlineCode("DL"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE,
            false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )
    end

    @testset "MCT_BIT_* constants" begin
        # All bit constants are distinct powers of 2
        bits = [
            MCT_BIT_ARR_CARRIER, MCT_BIT_DEP_CARRIER,
            MCT_BIT_ARR_TERM, MCT_BIT_DEP_TERM,
            MCT_BIT_PRV_STN, MCT_BIT_NXT_STN,
            MCT_BIT_PRV_COUNTRY, MCT_BIT_NXT_COUNTRY,
            MCT_BIT_PRV_REGION, MCT_BIT_NXT_REGION,
            MCT_BIT_DEP_BODY, MCT_BIT_ARR_BODY,
        ]
        @test length(unique(bits)) == 12
        for b in bits
            @test count_ones(b) == 1   # each is a single-bit mask
        end
    end

    @testset "_compute_specificity" begin
        # Fully generic record (no bits set) → 0
        r0 = MCTRecord()
        @test ItinerarySearch._compute_specificity(r0) == UInt32(0)

        # Carrier-specific record outranks terminal-only record
        r_carrier = MCTRecord(
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
        )
        r_term = MCTRecord(
            arr_term  = InlineString3("1"),
            specified = MCT_BIT_ARR_TERM,
        )
        @test ItinerarySearch._compute_specificity(r_carrier) >
              ItinerarySearch._compute_specificity(r_term)
    end

    @testset "Suppression does not affect non-matching carrier" begin
        # Suppression record is specific to UA; AA should fall through to global default
        supp_rec = MCTRecord(
            time        = Minutes(0),
            suppressed  = true,
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
        )
        lookup = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(
            lookup, AirlineCode("AA"), AirlineCode("DL"),
            StationCode("ORD"), MCT_DD,
        )
        @test !result.suppressed
        @test result.source == SOURCE_GLOBAL_DEFAULT
        @test result.time == Minutes(60)
    end

    @testset "Global defaults per status" begin
        lookup = MCTLookup()
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), MCT_DD).time == Minutes(60)
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), MCT_DI).time == Minutes(90)
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), MCT_ID).time == Minutes(90)
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), MCT_II).time == Minutes(120)
    end

    @testset "mct_id propagation" begin
        # Global default (no station records) → mct_id == 0
        lookup_empty = MCTLookup()
        result_global = lookup_mct(
            lookup_empty, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result_global.mct_id == Int32(0)
        @test result_global.source == SOURCE_GLOBAL_DEFAULT

        # Station has records but none match → falls through to global default → mct_id == 0
        exc_rec_dl = MCTRecord(
            time        = Minutes(30),
            arr_carrier = AirlineCode("DL"),
            specified   = MCT_BIT_ARR_CARRIER,
            specificity = UInt32(1 << 23),
            mct_id      = Int32(42),
        )
        lookup_no_match = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [exc_rec_dl], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_fallback = lookup_mct(
            lookup_no_match, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result_fallback.mct_id == Int32(0)
        @test result_fallback.source == SOURCE_GLOBAL_DEFAULT

        # Exception match → mct_id propagated from MCTRecord
        exc_rec_ua = MCTRecord(
            time        = Minutes(25),
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
            specificity = UInt32(1 << 23),
            mct_id      = Int32(99),
        )
        lookup_hit = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [exc_rec_ua], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_exc = lookup_mct(
            lookup_hit, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result_exc.mct_id == Int32(99)
        @test result_exc.source == SOURCE_EXCEPTION

        # Station standard match → mct_id propagated
        std_rec = MCTRecord(
            time             = Minutes(45),
            station_standard = true,
            mct_id           = Int32(7),
        )
        lookup_std = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_std = lookup_mct(
            lookup_std, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result_std.mct_id == Int32(7)
        @test result_std.source == SOURCE_STATION_STANDARD

        # Suppression match → mct_id propagated
        supp_rec = MCTRecord(
            time        = Minutes(0),
            suppressed  = true,
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
            mct_id      = Int32(55),
        )
        lookup_supp = MCTLookup(
            stations = Dict(
                StationCode("ORD") => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_supp = lookup_mct(
            lookup_supp, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), MCT_DD,
        )
        @test result_supp.mct_id == Int32(55)
        @test result_supp.suppressed
    end

    @testset "materialize_mct_lookup filters by min_mct_override" begin
        using DuckDB, DBInterface
        store = DuckDBStore()
        try
            # Insert three MCT rows:
            #   id=1 — 30 min (below 60-min threshold, should be excluded)
            #   id=2 — 90 min (above threshold, should be included)
            #   id=3 — suppression record at 0 min (must always be included)
            DBInterface.execute(store.db, """
                INSERT INTO mct (mct_id, arr_stn, dep_stn, mct_status, time_minutes, suppress, station_standard)
                VALUES
                    (1, 'ORD', 'ORD', 'DD', 30,  false, false),
                    (2, 'ORD', 'ORD', 'DD', 90,  false, false),
                    (3, 'ORD', 'ORD', 'DD',  0,  true,  false)
            """)

            constraints = SearchConstraints(
                defaults = ParameterSet(min_mct_override = Minutes(60)),
            )
            lookup = materialize_mct_lookup(
                store, Set([StationCode("ORD")]); constraints = constraints,
            )

            @test haskey(lookup.stations, StationCode("ORD"))
            dd_records = lookup.stations[StationCode("ORD")][1]   # status index 1 = DD

            # 30-min record must be absent
            times = [r.time for r in dd_records]
            @test Minutes(30) ∉ times

            # 90-min record must be present
            @test Minutes(90) ∈ times

            # Suppression record (time=0, suppressed=true) must be present
            suppressed_present = any(r -> r.suppressed, dd_records)
            @test suppressed_present
        finally
            close(store)
        end
    end
end

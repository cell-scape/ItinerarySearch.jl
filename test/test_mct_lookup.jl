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
        )

        # Carrier specified, must match
        r2 = MCTRecord(arr_carrier = AirlineCode("UA"), specified = MCT_BIT_ARR_CARRIER)
        @test ItinerarySearch._mct_record_matches(
            r2, AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
        )
        @test !ItinerarySearch._mct_record_matches(
            r2, AirlineCode("DL"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
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
end

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
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(30)       # DD default
        @test result.source == SOURCE_GLOBAL_DEFAULT
        @test !result.suppressed
    end

    @testset "Station standard found" begin
        std_rec = MCTRecord(time = Minutes(45), station_standard = true)
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [exc_rec, std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        # UA arriving → exception match
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(30)
        @test result.source == SOURCE_EXCEPTION

        # DL arriving → no exception match, falls through to standard
        result2 = lookup_mct(
            lookup, AirlineCode("DL"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
        )
        @test result.time == Minutes(0)
        @test result.suppressed
    end

    @testset "Status index mapping" begin
        dd_rec = MCTRecord(time = Minutes(40), station_standard = true)
        di_rec = MCTRecord(time = Minutes(70), station_standard = true)
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [dd_rec], [di_rec], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        @test lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
        ).time == Minutes(40)
        @test lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DI,
        ).time == Minutes(70)
        # II has no records → global default
        @test lookup_mct(
            lookup, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_II,
        ).time == Minutes(90)
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(
            lookup, AirlineCode("AA"), AirlineCode("DL"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
        )
        @test !result.suppressed
        @test result.source == SOURCE_GLOBAL_DEFAULT
        @test result.time == Minutes(30)
    end

    @testset "Global defaults per status" begin
        lookup = MCTLookup()
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), StationCode("ORD"), MCT_DD).time == Minutes(30)
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), StationCode("ORD"), MCT_DI).time == Minutes(60)
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), StationCode("ORD"), MCT_ID).time == Minutes(90)
        @test lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                         StationCode("ORD"), StationCode("ORD"), MCT_II).time == Minutes(90)
    end

    @testset "mct_id propagation" begin
        # Global default (no station records) → mct_id == 0
        lookup_empty = MCTLookup()
        result_global = lookup_mct(
            lookup_empty, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [exc_rec_dl], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_fallback = lookup_mct(
            lookup_no_match, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [exc_rec_ua], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_exc = lookup_mct(
            lookup_hit, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_std = lookup_mct(
            lookup_std, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
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
                (StationCode("ORD"), StationCode("ORD")) => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_supp = lookup_mct(
            lookup_supp, AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD,
        )
        @test result_supp.mct_id == Int32(55)
        @test result_supp.suppressed
    end

    @testset "Codeshare matching" begin
        # Record that requires arriving flight to be a codeshare (arr_cs_ind = 'Y')
        # with operating carrier AC
        cs_rec = MCTRecord(
            time             = Minutes(20),
            arr_cs_ind       = 'Y',
            arr_cs_op_carrier = AirlineCode("AC"),
            specified        = MCT_BIT_ARR_CS_IND | MCT_BIT_ARR_CS_OP,
            specificity      = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_ARR_CS_IND | MCT_BIT_ARR_CS_OP),
            ),
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("YYZ"), StationCode("YYZ")) => (
                    [cs_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # _mct_record_matches directly — codeshare flight with matching op carrier
        @test ItinerarySearch._mct_record_matches(
            cs_rec,
            AirlineCode("AC"), AirlineCode("WJ"),  # arr/dep carrier
            ' ', ' ',
            NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            AirlineCode("AC"), NO_AIRLINE,          # arr_op_carrier = AC
            true, false,                            # arr_is_codeshare = true
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Non-codeshare flight — must NOT match (cs_ind='Y' requires codeshare)
        @test !ItinerarySearch._mct_record_matches(
            cs_rec,
            AirlineCode("AC"), AirlineCode("WJ"),
            ' ', ' ',
            NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE,
            false, false,                           # arr_is_codeshare = false
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Wrong operating carrier — must NOT match
        @test !ItinerarySearch._mct_record_matches(
            cs_rec,
            AirlineCode("AC"), AirlineCode("WJ"),
            ' ', ' ',
            NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            AirlineCode("UA"), NO_AIRLINE,          # wrong op carrier
            true, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # lookup_mct — codeshare match hits the record
        result_cs = lookup_mct(
            lookup,
            AirlineCode("AC"), AirlineCode("WJ"),
            StationCode("YYZ"), StationCode("YYZ"), MCT_DD;
            arr_is_codeshare = true,
            arr_op_carrier   = AirlineCode("AC"),
        )
        @test result_cs.time == Minutes(20)
        @test result_cs.source == SOURCE_EXCEPTION

        # lookup_mct — non-codeshare flight falls through to global default
        result_non_cs = lookup_mct(
            lookup,
            AirlineCode("AC"), AirlineCode("WJ"),
            StationCode("YYZ"), StationCode("YYZ"), MCT_DD;
            arr_is_codeshare = false,
        )
        @test result_non_cs.source == SOURCE_GLOBAL_DEFAULT
        @test result_non_cs.time == Minutes(30)
    end

    @testset "Flight number range matching" begin
        # Record covers arriving flight numbers 1000–1999
        rng_rec = MCTRecord(
            time              = Minutes(35),
            arr_flt_rng_start = FlightNumber(1000),
            arr_flt_rng_end   = FlightNumber(1999),
            specified         = MCT_BIT_ARR_FLT_RNG,
            specificity       = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_ARR_FLT_RNG),
            ),
        )
        # Record covers departing flight numbers 200–299
        dep_rng_rec = MCTRecord(
            time              = Minutes(28),
            dep_flt_rng_start = FlightNumber(200),
            dep_flt_rng_end   = FlightNumber(299),
            specified         = MCT_BIT_DEP_FLT_RNG,
            specificity       = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_DEP_FLT_RNG),
            ),
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("LAX"), StationCode("LAX")) => (
                    [dep_rng_rec, rng_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # Arriving flight 1500 — within range [1000,1999] → match
        @test ItinerarySearch._mct_record_matches(
            rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(1500), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Arriving flight 999 — below range → no match
        @test !ItinerarySearch._mct_record_matches(
            rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(999), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Arriving flight 2000 — above range → no match
        @test !ItinerarySearch._mct_record_matches(
            rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(2000), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Boundary: arriving flight 1000 — at start of range → match
        @test ItinerarySearch._mct_record_matches(
            rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(1000), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Boundary: arriving flight 1999 — at end of range → match
        @test ItinerarySearch._mct_record_matches(
            rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(1999), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Departing side: dep flight 250 — within dep range [200,299] → match
        @test ItinerarySearch._mct_record_matches(
            dep_rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(250),
            InlineString3(""), InlineString3(""),
        )

        # Departing side: dep flight 100 — outside dep range → no match
        @test !ItinerarySearch._mct_record_matches(
            dep_rng_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(100),
            InlineString3(""), InlineString3(""),
        )

        # Through lookup_mct: arr flt 1500 hits arr range record
        result_in = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("LAX"), StationCode("LAX"), MCT_DD;
            arr_flt_no = FlightNumber(1500),
        )
        @test result_in.time == Minutes(35)
        @test result_in.source == SOURCE_EXCEPTION

        # Through lookup_mct: arr flt 500 misses arr range, misses dep range → global default
        result_out = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("LAX"), StationCode("LAX"), MCT_DD;
            arr_flt_no = FlightNumber(500),
            dep_flt_no = FlightNumber(500),
        )
        @test result_out.source == SOURCE_GLOBAL_DEFAULT
    end

    @testset "Aircraft type matching" begin
        acft_rec = MCTRecord(
            time          = Minutes(55),
            arr_acft_type = InlineString7("789"),
            specified     = MCT_BIT_ARR_ACFT_TYPE,
            specificity   = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_ARR_ACFT_TYPE),
            ),
        )
        dep_acft_rec = MCTRecord(
            time          = Minutes(40),
            dep_acft_type = InlineString7("77W"),
            specified     = MCT_BIT_DEP_ACFT_TYPE,
            specificity   = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_DEP_ACFT_TYPE),
            ),
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("SFO"), StationCode("SFO")) => (
                    [dep_acft_rec, acft_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # Arriving 789 → match
        @test ItinerarySearch._mct_record_matches(
            acft_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7("789"), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Arriving 738 → no match
        @test !ItinerarySearch._mct_record_matches(
            acft_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7("738"), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Departing 77W → match
        @test ItinerarySearch._mct_record_matches(
            dep_acft_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7("77W"),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3(""),
        )

        # Through lookup_mct: 789 arriving hits record
        result_match = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("SFO"), StationCode("SFO"), MCT_DD;
            arr_acft_type = InlineString7("789"),
        )
        @test result_match.time == Minutes(55)
        @test result_match.source == SOURCE_EXCEPTION

        # Through lookup_mct: 320 arriving misses → global default
        result_miss = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("SFO"), StationCode("SFO"), MCT_DD;
            arr_acft_type = InlineString7("320"),
        )
        @test result_miss.source == SOURCE_GLOBAL_DEFAULT
    end

    @testset "State geography matching" begin
        state_rec = MCTRecord(
            time      = Minutes(50),
            prv_state = InlineString3("IL"),
            specified = MCT_BIT_PRV_STATE,
            specificity = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_PRV_STATE),
            ),
        )
        nxt_state_rec = MCTRecord(
            time      = Minutes(45),
            nxt_state = InlineString3("CA"),
            specified = MCT_BIT_NXT_STATE,
            specificity = ItinerarySearch._compute_specificity(
                MCTRecord(specified = MCT_BIT_NXT_STATE),
            ),
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [nxt_state_rec, state_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # prv_state = IL → match
        @test ItinerarySearch._mct_record_matches(
            state_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3("IL"), InlineString3(""),
        )

        # prv_state = CA → no match (record requires IL)
        @test !ItinerarySearch._mct_record_matches(
            state_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3("CA"), InlineString3(""),
        )

        # nxt_state = CA → match on the nxt_state record
        @test ItinerarySearch._mct_record_matches(
            nxt_state_rec,
            AirlineCode("UA"), AirlineCode("AA"),
            ' ', ' ', NO_STATION, NO_STATION,
            InlineString3(""), InlineString3(""),
            InlineString3(""), InlineString3(""),
            NO_AIRLINE, NO_AIRLINE, false, false,
            InlineString7(""), InlineString7(""),
            FlightNumber(0), FlightNumber(0),
            InlineString3(""), InlineString3("CA"),
        )

        # Through lookup_mct: prv_state = IL hits state_rec (NXT_STATE has higher specificity but doesn't match IL)
        result_il = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD;
            prv_state = InlineString3("IL"),
        )
        @test result_il.time == Minutes(50)
        @test result_il.source == SOURCE_EXCEPTION

        # Through lookup_mct: prv_state = TX → global default
        result_tx = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD;
            prv_state = InlineString3("TX"),
            nxt_state = InlineString3("TX"),
        )
        @test result_tx.source == SOURCE_GLOBAL_DEFAULT
    end

    @testset "Date validity filtering" begin
        # Record valid only for June 2026
        dated_rec = MCTRecord(
            time       = Minutes(33),
            eff_date   = pack_date(Date(2026, 6, 1)),
            dis_date   = pack_date(Date(2026, 6, 30)),
            specified  = UInt32(0),    # wildcard — matches everything IF in date window
            specificity = UInt32(1 << 7),  # date bit contributes to specificity
        )
        # Undated fallback record
        undated_rec = MCTRecord(
            time        = Minutes(60),
            station_standard = true,
        )
        lookup2 = MCTLookup(
            stations = Dict(
                (StationCode("DFW"), StationCode("DFW")) => (
                    [dated_rec, undated_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # target_date within window (June 15 2026) → dated record matches
        result_in = lookup_mct(
            lookup2,
            AirlineCode("AA"), AirlineCode("UA"),
            StationCode("DFW"), StationCode("DFW"), MCT_DD;
            target_date = pack_date(Date(2026, 6, 15)),
        )
        @test result_in.time == Minutes(33)
        @test result_in.source == SOURCE_EXCEPTION

        # target_date before window (May 31 2026) → dated record skipped; undated matches
        result_before = lookup_mct(
            lookup2,
            AirlineCode("AA"), AirlineCode("UA"),
            StationCode("DFW"), StationCode("DFW"), MCT_DD;
            target_date = pack_date(Date(2026, 5, 31)),
        )
        @test result_before.time == Minutes(60)

        # target_date after window (July 1 2026) → dated record skipped; undated matches
        result_after = lookup_mct(
            lookup2,
            AirlineCode("AA"), AirlineCode("UA"),
            StationCode("DFW"), StationCode("DFW"), MCT_DD;
            target_date = pack_date(Date(2026, 7, 1)),
        )
        @test result_after.time == Minutes(60)

        # target_date = 0 (no date filtering) → dated record DOES match (wildcard date)
        result_nodate = lookup_mct(
            lookup2,
            AirlineCode("AA"), AirlineCode("UA"),
            StationCode("DFW"), StationCode("DFW"), MCT_DD;
            target_date = UInt32(0),
        )
        @test result_nodate.time == Minutes(33)
        @test result_nodate.source == SOURCE_EXCEPTION

        # Undated record (effective_date = 0) always matches regardless of target_date
        always_rec = MCTRecord(
            time      = Minutes(77),
            eff_date  = UInt32(0),
            dis_date  = UInt32(0),
            specified = UInt32(0),
        )
        lookup3 = MCTLookup(
            stations = Dict(
                (StationCode("BOS"), StationCode("BOS")) => (
                    [always_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result_always = lookup_mct(
            lookup3,
            AirlineCode("B6"), AirlineCode("DL"),
            StationCode("BOS"), StationCode("BOS"), MCT_DD;
            target_date = pack_date(Date(2025, 1, 1)),
        )
        @test result_always.time == Minutes(77)
        @test result_always.source == SOURCE_EXCEPTION
    end

    @testset "Suppression geography" begin
        # Suppression geography is scoped to the CONNECTION STATION, not prv/nxt.
        # supp_region=EUR means suppress at stations in EUR region.

        # Suppression record scoped to EUR region
        supp_eur = MCTRecord(
            time        = Minutes(0),
            suppressed  = true,
            supp_region = InlineString3("EUR"),
            specified   = UInt32(0),   # field matching is wildcard; geography scope narrows it
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("LHR"), StationCode("LHR")) => (
                    [supp_eur], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # Connection station in EUR → suppressed
        result_eur = lookup_mct(
            lookup,
            AirlineCode("BA"), AirlineCode("LH"),
            StationCode("LHR"), StationCode("LHR"), MCT_DD;
            cnx_region = InlineString3("EUR"),
        )
        @test result_eur.suppressed
        @test result_eur.source == SOURCE_EXCEPTION

        # Connection station NOT in EUR → NOT suppressed → global default
        result_noa = lookup_mct(
            lookup,
            AirlineCode("BA"), AirlineCode("LH"),
            StationCode("LHR"), StationCode("LHR"), MCT_DD;
            cnx_region = InlineString3("NOA"),
        )
        @test !result_noa.suppressed
        @test result_noa.source == SOURCE_GLOBAL_DEFAULT

        # Suppression scoped to country US
        supp_us = MCTRecord(
            time         = Minutes(0),
            suppressed   = true,
            supp_country = InlineString3("US"),
            specified    = UInt32(0),
        )
        lookup_ctry = MCTLookup(
            stations = Dict(
                (StationCode("JFK"), StationCode("JFK")) => (
                    [supp_us], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        # Connection station in US → suppressed
        result_us = lookup_mct(
            lookup_ctry,
            AirlineCode("UA"), AirlineCode("DL"),
            StationCode("JFK"), StationCode("JFK"), MCT_DD;
            cnx_country = InlineString3("US"),
        )
        @test result_us.suppressed

        # Connection station in CA → not suppressed
        result_ca = lookup_mct(
            lookup_ctry,
            AirlineCode("UA"), AirlineCode("DL"),
            StationCode("JFK"), StationCode("JFK"), MCT_DD;
            cnx_country = InlineString3("CA"),
        )
        @test !result_ca.suppressed
        @test result_ca.source == SOURCE_GLOBAL_DEFAULT

        # Suppression scoped to state TX
        supp_tx = MCTRecord(
            time       = Minutes(0),
            suppressed = true,
            supp_state = InlineString3("TX"),
            specified  = UInt32(0),
        )
        lookup_state = MCTLookup(
            stations = Dict(
                (StationCode("DFW"), StationCode("DFW")) => (
                    [supp_tx], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        # Connection station in TX → suppressed
        result_tx = lookup_mct(
            lookup_state,
            AirlineCode("AA"), AirlineCode("WN"),
            StationCode("DFW"), StationCode("DFW"), MCT_DD;
            cnx_state = InlineString3("TX"),
        )
        @test result_tx.suppressed

        # Connection station in IL → not suppressed
        result_il = lookup_mct(
            lookup_state,
            AirlineCode("AA"), AirlineCode("WN"),
            StationCode("DFW"), StationCode("DFW"), MCT_DD;
            cnx_state = InlineString3("IL"),
        )
        @test !result_il.suppressed
    end

    @testset "Inter-station MCT" begin
        # Record keyed to (JFK, EWR) — inter-station
        inter_rec = MCTRecord(
            time             = Minutes(120),
            station_standard = true,
        )
        std_rec = MCTRecord(
            time             = Minutes(45),
            station_standard = true,
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("JFK"), StationCode("EWR")) => (
                    [inter_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
                (StationCode("JFK"), StationCode("JFK")) => (
                    [std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # Inter-station lookup JFK→EWR hits the inter-station record
        result_inter = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("JFK"), StationCode("EWR"), MCT_DD,
        )
        @test result_inter.time == Minutes(120)
        @test result_inter.source == SOURCE_STATION_STANDARD

        # Intra-station JFK→JFK hits the JFK standard record
        result_intra = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("JFK"), StationCode("JFK"), MCT_DD,
        )
        @test result_intra.time == Minutes(45)
        @test result_intra.source == SOURCE_STATION_STANDARD

        # Unknown inter-station pair (no key in dict) → inter_station_default = 240 min
        result_unknown = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("LGA"), StationCode("EWR"), MCT_DD,
        )
        @test result_unknown.time == Minutes(240)
        @test result_unknown.source == SOURCE_GLOBAL_DEFAULT

        # Unknown intra-station pair → normal global default (30 min for DD)
        result_unknown_intra = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("BOS"), StationCode("BOS"), MCT_DD,
        )
        @test result_unknown_intra.time == Minutes(30)
        @test result_unknown_intra.source == SOURCE_GLOBAL_DEFAULT

        # Verify inter_station_default field
        custom_lookup = MCTLookup(inter_station_default = Minutes(180))
        result_custom = lookup_mct(
            custom_lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("JFK"), StationCode("EWR"), MCT_DD,
        )
        @test result_custom.time == Minutes(180)
        @test result_custom.source == SOURCE_GLOBAL_DEFAULT
    end

    @testset "Specificity ordering — most specific wins" begin
        # Three records, all matching, with different specificity levels:
        # 1. Carrier + flight range (high specificity)
        # 2. Carrier only (medium specificity)
        # 3. Generic wildcard (low specificity)
        carrier_and_range = MCTRecord(
            time              = Minutes(10),
            arr_carrier       = AirlineCode("UA"),
            arr_flt_rng_start = FlightNumber(100),
            arr_flt_rng_end   = FlightNumber(200),
            specified         = MCT_BIT_ARR_CARRIER | MCT_BIT_ARR_FLT_RNG,
        )
        carrier_only = MCTRecord(
            time        = Minutes(25),
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
        )
        wildcard = MCTRecord(
            time      = Minutes(60),
            specified = UInt32(0),
        )

        # Recompute specificity so they sort correctly
        spec_cr = ItinerarySearch._compute_specificity(carrier_and_range)
        spec_co = ItinerarySearch._compute_specificity(carrier_only)
        spec_wc = ItinerarySearch._compute_specificity(wildcard)

        @test spec_cr > spec_co
        @test spec_co > spec_wc

        rec_high = MCTRecord(
            time              = Minutes(10),
            arr_carrier       = AirlineCode("UA"),
            arr_flt_rng_start = FlightNumber(100),
            arr_flt_rng_end   = FlightNumber(200),
            specified         = MCT_BIT_ARR_CARRIER | MCT_BIT_ARR_FLT_RNG,
            specificity       = spec_cr,
        )
        rec_mid = MCTRecord(
            time        = Minutes(25),
            arr_carrier = AirlineCode("UA"),
            specified   = MCT_BIT_ARR_CARRIER,
            specificity = spec_co,
        )
        rec_low = MCTRecord(
            time        = Minutes(60),
            specified   = UInt32(0),
            specificity = spec_wc,
        )

        # Records pre-sorted descending by specificity (as materialize_mct_lookup does)
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [rec_high, rec_mid, rec_low], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )

        # UA flt 150 matches rec_high (carrier + range) → should get 10 min
        result_high = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD;
            arr_flt_no = FlightNumber(150),
        )
        @test result_high.time == Minutes(10)

        # UA flt 500 (out of range) only matches rec_mid (carrier only) → 25 min
        result_mid = lookup_mct(
            lookup,
            AirlineCode("UA"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD;
            arr_flt_no = FlightNumber(500),
        )
        @test result_mid.time == Minutes(25)

        # DL flt 500 matches only rec_low (wildcard) → 60 min
        result_low = lookup_mct(
            lookup,
            AirlineCode("DL"), AirlineCode("AA"),
            StationCode("ORD"), StationCode("ORD"), MCT_DD;
            arr_flt_no = FlightNumber(500),
        )
        @test result_low.time == Minutes(60)
    end

    @testset "MCT_BIT_* constants completeness" begin
        all_bits = [
            MCT_BIT_ARR_CARRIER,
            MCT_BIT_DEP_CARRIER,
            MCT_BIT_ARR_TERM,
            MCT_BIT_DEP_TERM,
            MCT_BIT_PRV_STN,
            MCT_BIT_NXT_STN,
            MCT_BIT_PRV_COUNTRY,
            MCT_BIT_NXT_COUNTRY,
            MCT_BIT_PRV_REGION,
            MCT_BIT_NXT_REGION,
            MCT_BIT_DEP_BODY,
            MCT_BIT_ARR_BODY,
            MCT_BIT_ARR_CS_IND,
            MCT_BIT_ARR_CS_OP,
            MCT_BIT_DEP_CS_IND,
            MCT_BIT_DEP_CS_OP,
            MCT_BIT_ARR_ACFT_TYPE,
            MCT_BIT_DEP_ACFT_TYPE,
            MCT_BIT_ARR_FLT_RNG,
            MCT_BIT_DEP_FLT_RNG,
            MCT_BIT_PRV_STATE,
            MCT_BIT_NXT_STATE,
        ]
        # All 22 constants are present and distinct
        @test length(all_bits) == 22
        @test length(unique(all_bits)) == 22
        # Each is a single-bit mask (power of 2)
        for b in all_bits
            @test count_ones(b) == 1
        end
        # All fit within UInt32
        for b in all_bits
            @test b isa UInt32
            @test b <= typemax(UInt32)
        end
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

            @test haskey(lookup.stations, (StationCode("ORD"), StationCode("ORD")))
            dd_records = lookup.stations[(StationCode("ORD"), StationCode("ORD"))][1]   # status index 1 = DD

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

    @testset "matched_fields propagation" begin
        # Exception match propagates specified bitmask
        exc_rec = MCTRecord(
            time = Minutes(30),
            arr_carrier = AirlineCode("UA"),
            specified = MCT_BIT_ARR_CARRIER,
            specificity = UInt32(1 << 25),
        )
        lookup = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [exc_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result = lookup_mct(lookup, AirlineCode("UA"), AirlineCode("AA"),
                            StationCode("ORD"), StationCode("ORD"), MCT_DD)
        @test result.matched_fields == MCT_BIT_ARR_CARRIER
        @test result.source == SOURCE_EXCEPTION

        # Station standard propagates specified (usually 0 for standards)
        std_rec = MCTRecord(time = Minutes(45), station_standard = true)
        lookup2 = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [std_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result2 = lookup_mct(lookup2, AirlineCode("DL"), AirlineCode("AA"),
                             StationCode("ORD"), StationCode("ORD"), MCT_DD)
        @test result2.matched_fields == UInt32(0)
        @test result2.source == SOURCE_STATION_STANDARD

        # Global default has matched_fields = 0
        result3 = lookup_mct(MCTLookup(), AirlineCode("UA"), AirlineCode("AA"),
                             StationCode("ZZZ"), StationCode("ZZZ"), MCT_DD)
        @test result3.matched_fields == UInt32(0)
        @test result3.source == SOURCE_GLOBAL_DEFAULT

        # Suppression propagates specified
        supp_rec = MCTRecord(
            time = Minutes(0),
            suppressed = true,
            arr_carrier = AirlineCode("UA"),
            specified = MCT_BIT_ARR_CARRIER,
        )
        lookup3 = MCTLookup(
            stations = Dict(
                (StationCode("ORD"), StationCode("ORD")) => (
                    [supp_rec], MCTRecord[], MCTRecord[], MCTRecord[],
                ),
            ),
        )
        result4 = lookup_mct(lookup3, AirlineCode("UA"), AirlineCode("AA"),
                             StationCode("ORD"), StationCode("ORD"), MCT_DD)
        @test result4.matched_fields == MCT_BIT_ARR_CARRIER
        @test result4.suppressed
    end
end

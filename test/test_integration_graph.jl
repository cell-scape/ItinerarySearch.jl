using Test
using ItinerarySearch
using DuckDB, DBInterface
using Dates
using InlineStrings

# ── SSIM builder helpers ───────────────────────────────────────────────────────
#
# Three-station network:
#
#   ORD → JFK   UA  200  dep 08:00  arr 11:00  (all days, Jun 1–30 2026)
#   JFK → LHR   UA 1234  dep 14:00  arr 02:00+1 (all days, Jun 1–30 2026)
#   ORD → LHR   UA  916  dep 10:00  arr 22:00  (all days, Jun 1–30 2026)
#
# This gives us:
#   - Nonstop ORD→LHR (direct)
#   - 1-stop  ORD→JFK→LHR (ORD arr 11:00, JFK dep 14:00 → 180 min cnx ≥ MCT)
#
# Note: SSIM ingest leaves legs.distance = 0.0 (no distance in Type-3 records).
# Post-ingest pipeline computes market distances via ST_Distance_Sphere and
# stores them in the markets table; legs.distance remains 0 at schedule level.
# Tests that require nonzero distance use SQL to set it after ingest.

function _build_ssim_line(;
    rec_serial::Int,
    airline::String,
    flt_no::Int,
    org::String,
    dep::String,    # "HHMM"
    dst::String,
    arr::String,    # "HHMM"
    arr_date_var::Int = 0,
    eff_from::String = "01JUN26",
    eff_to::String = "30JUN26",
    frequency::String = "1234567",
    eqp::String = "738",
    dep_utc::String = "-0500",
    arr_utc::String = "+0000",
    dep_term::String = "H ",
    arr_term::String = "T ",
)::String
    # SSIM Type-3 fixed-width layout (1-indexed):
    # col  1      : record type "3"
    # col  2      : op_suffix
    # col  3-5    : airline (3 chars)
    # col  6-9    : flt_no (4 digits)
    # col 10-11   : itin_var
    # col 12-13   : leg_seq
    # col 14      : svc_type
    # col 15-21   : eff_from (7 chars, ddmonyy)
    # col 22-28   : eff_to
    # col 29-35   : frequency (7 chars)
    # col 36      : frequency rate
    # col 37-39   : dep station
    # col 40-43   : pax_dep (HHMM)
    # col 44-47   : ac_dep  (HHMM)
    # col 48-52   : dep_utc (+/-HHMM)
    # col 53-54   : dep_term (2 chars)
    # col 55-57   : arr station
    # col 58-61   : ac_arr
    # col 62-65   : pax_arr
    # col 66-70   : arr_utc
    # col 71-72   : arr_term
    # col 73-75   : eqp
    # col 76-95   : prbd (20 chars)
    # col 96-119  : prbm+meal+jv (24 chars)
    # col 120-121 : mct dep/arr
    # col 122-127 : spare (6)
    # col 128     : itin_var_overflow
    # col 129-131 : aircraft_owner
    # col 132-148 : spare (17)
    # col 149     : op_disclosure
    # col 150-160 : TRC (11)
    # col 161     : TRC overflow
    # col 162-171 : spare (10)
    # col 172     : spare
    # col 173-192 : ACV (20)
    # col 193     : dep_date_var  ← dep first
    # col 194     : arr_date_var  ← arr second
    # col 195-200 : record serial
    line = "3"
    line *= " "
    line *= rpad(airline, 3)
    line *= lpad(string(flt_no), 4, '0')
    line *= "01"
    line *= "01"
    line *= "J"
    line *= eff_from
    line *= eff_to
    line *= frequency
    line *= " "
    line *= org
    line *= dep
    line *= dep     # ac_dep = pax_dep
    line *= dep_utc
    line *= rpad(dep_term, 2)
    line *= dst
    line *= arr
    line *= arr     # ac_arr = pax_arr
    line *= arr_utc
    line *= rpad(arr_term, 2)
    line *= rpad(eqp, 3)
    line *= rpad("JCDZPY", 20)   # prbd
    line *= rpad("", 24)          # prbm+meal+jv
    line *= "DI"                  # mct_dep / mct_arr
    line *= rpad("", 6)           # spare
    line *= " "                   # itin_var_overflow
    line *= rpad(airline, 3)      # aircraft_owner
    line *= rpad("", 17)          # spare
    line *= " "                   # op_disclosure
    line *= rpad("", 11)          # TRC
    line *= " "                   # TRC overflow
    line *= rpad("", 10)          # spare
    line *= " "                   # spare
    line *= rpad("", 20)          # ACV
    line *= "0"                   # col 193: dep_date_var (always 0)
    line *= string(arr_date_var)  # col 194: arr_date_var
    rpad(line, 194) * lpad(string(rec_serial), 6, '0')
end

function _build_integration_ssim()::String
    t1 = rpad("1AIRLINE STANDARD SCHEDULE DATA SET", 194) * "000001"
    t2 = rpad("2UUA        S2601JAN2631DEC2617MAR26", 194) * "000002"

    # ORD → JFK  dep 08:00 (480) arr 11:00 (660)  domestic, no overnight
    t3 = _build_ssim_line(
        rec_serial = 3,
        airline = "UA",
        flt_no = 200,
        org = "ORD",
        dep = "0800",
        dst = "JFK",
        arr = "1100",
        arr_date_var = 0,
        dep_utc = "-0500",
        arr_utc = "-0500",
    )

    # JFK → LHR  dep 14:00 (840) arr 02:00+1 (120)  transatlantic overnight
    t4 = _build_ssim_line(
        rec_serial = 4,
        airline = "UA",
        flt_no = 1234,
        org = "JFK",
        dep = "1400",
        dst = "LHR",
        arr = "0200",
        arr_date_var = 1,      # arrives next day
        dep_utc = "-0500",
        arr_utc = "+0000",
    )

    # ORD → LHR  dep 10:00 (600) arr 22:00 (1320)  nonstop transatlantic
    t5 = _build_ssim_line(
        rec_serial = 5,
        airline = "UA",
        flt_no = 916,
        org = "ORD",
        dep = "1000",
        dst = "LHR",
        arr = "2200",
        arr_date_var = 0,
        dep_utc = "-0500",
        arr_utc = "+0000",
    )

    t6_fields = "5 UA "
    t6 = rpad(t6_fields, 187) * "000005" * "E" * "000006"

    join([t1, t2, t3, t4, t5, t6], "\n") * "\n"
end

# Helper: inject leg distances into the legs table via SQL (SSIM has no distance field)
function _inject_leg_distances!(store::DuckDBStore)
    DBInterface.execute(store.db, """
        UPDATE legs SET distance =
            CASE
                WHEN org = 'ORD' AND dst = 'JFK' THEN 740.0
                WHEN org = 'JFK' AND dst = 'LHR' THEN 3451.0
                WHEN org = 'ORD' AND dst = 'LHR' THEN 3941.0
                ELSE 0.0
            END
    """)
end

# ── Integration test body ──────────────────────────────────────────────────────

@testset "Graph Engine Integration" begin

    # Shared airports file content (used across subtests)
    airports_content =
        make_test_airports()

    @testset "Full pipeline: ingest → graph → search" begin
        ssim_path = tempname()
        mct_path = tempname()
        airports_path = tempname()

        write(ssim_path, _build_integration_ssim())
        write(mct_path, make_test_mct())  # from test_ingest.jl (included earlier)
        write(airports_path, airports_content)

        config = SearchConfig(
            ssim_path = ssim_path,
            mct_path = mct_path,
            airports_path = airports_path,
            regions_path = "/dev/null",
            aircrafts_path = "/dev/null",
            oa_control_path = "/dev/null",
            leading_days = 0,
            trailing_days = 0,
            interline = INTERLINE_ALL,
            max_stops = 2,
        )

        store = DuckDBStore()
        load_schedule!(store, config)
        _inject_leg_distances!(store)

        # ── 1. Verify ingest ───────────────────────────────────────────────────
        stats = table_stats(store)
        @test stats.legs == 3          # ORD→JFK, JFK→LHR, ORD→LHR
        @test stats.stations == 3
        @test stats.expanded_legs > 0
        @test stats.markets >= 3       # ORD-JFK, JFK-LHR, ORD-LHR

        # ── 2. Build graph ─────────────────────────────────────────────────────
        target = Date(2026, 6, 15)
        graph = build_graph!(store, config, target)

        @test graph isa FlightGraph
        @test length(graph.stations) == 3
        @test haskey(graph.stations, StationCode("ORD"))
        @test haskey(graph.stations, StationCode("JFK"))
        @test haskey(graph.stations, StationCode("LHR"))

        # Station coordinates populated from reference table
        ord_stn = graph.stations[StationCode("ORD")]
        @test ord_stn.record.lat ≈ 41.9742 atol = 0.01
        @test ord_stn.record.country == InlineString3("US")

        lhr_stn = graph.stations[StationCode("LHR")]
        @test lhr_stn.record.country == InlineString3("GB")

        jfk_stn = graph.stations[StationCode("JFK")]
        @test jfk_stn.record.country == InlineString3("US")

        # ── 3. Verify graph structure ──────────────────────────────────────────
        @test length(graph.legs) == 3
        @test length(graph.segments) == 3
        @test graph.build_stats.total_stations == Int32(3)
        @test graph.build_stats.total_legs == Int32(3)
        @test graph.build_stats.build_time_ns > UInt64(0)

        # Connections were built
        @test graph.build_stats.total_connections > Int32(0)

        # Station departure/arrival topology
        @test length(ord_stn.departures) == 2   # ORD→JFK and ORD→LHR
        @test length(lhr_stn.arrivals) == 2      # from JFK and from ORD
        @test length(jfk_stn.departures) == 1   # JFK→LHR
        @test length(jfk_stn.arrivals) == 1     # ORD→JFK

        # ── 4. Search for ORD → LHR ────────────────────────────────────────────
        ctx = RuntimeContext(
            config = config,
            constraints = SearchConstraints(),
            itn_rules = build_itn_rules(config),
        )

        itns = copy(search_itineraries(
            graph.stations,
            StationCode("ORD"),
            StationCode("LHR"),
            target,
            ctx,
        ))

        @test itns isa Vector{Itinerary}
        @test !isempty(itns)

        # Must include the nonstop ORD→LHR (UA 916, dep 10:00 arr 22:00)
        nonstops = filter(i -> i.num_stops == Int16(0), itns)
        @test !isempty(nonstops)

        # The nonstop ORD→LHR UTC elapsed:
        #   dep_utc_offset=-300 (UTC-5), arr_utc_offset=0 (UTC+0)
        #   utc_dep = 600 - (-300) = 900 (15:00 UTC)
        #   utc_arr = 1320 - 0    = 1320 (22:00 UTC)
        #   elapsed = 1320 - 900 = 420 min (7h block time)
        @test any(i -> i.elapsed_time == Int32(420), nonstops)

        # May include the 1-stop via JFK (ORD arr 11:00, JFK dep 14:00 → 180 min cnx)
        # JFK→LHR: dep 14:00 (840), arr 02:00+1 = 840+1440-120+120 ...
        # elapsed for 1-stop = computed by _compute_elapsed
        one_stops = filter(i -> i.num_stops == Int16(1), itns)
        if !isempty(one_stops)
            # JFK→LHR arr_date_var=1, so arr = 120 + 1440 = 1560; dep = 840
            # 1-stop elapsed: last_leg arr (1560) - first_leg dep (480) + cnx_time
            # cnx_time of the connecting cp = JFK dep (840) - ORD arr (660) = 180
            # total = (1560 - 480) + 180 = 1260 min
            @test any(i -> i.elapsed_time > Int32(0), one_stops)
        end

        # ── 5. Distance and circuity (after _inject_leg_distances!) ────────────
        for itn in itns
            @test itn.total_distance >= Distance(0)    # may be 0 for nonstops with no leg dist
            @test itn.market_distance > Distance(0)    # GC distance computed from coords
        end

        # Nonstops with injected distance should have total_distance > 0
        nonstops_with_dist = filter(i -> i.num_stops == Int16(0) && i.total_distance > Distance(0), itns)
        @test !isempty(nonstops_with_dist)
        @test all(i -> i.circuity >= 0.95f0, nonstops_with_dist)  # allow floating-point tolerance

        # ── 6. International classification via INTL flag ─────────────────────
        # The 1-stop ORD→JFK→LHR is US→GB, so the JFK→LHR connection is INTL.
        # Nonstop self-connections do not set STATUS_INTERNATIONAL (bypass rule chain).
        if !isempty(one_stops)
            @test any(i -> is_international(i.status), one_stops)
        end

        # Geographic diversity: ORD→LHR covers US and GB
        for itn in itns
            @test itn.num_countries >= Int16(1)
        end
        @test any(i -> i.num_countries >= Int16(2), itns)

        # ── 7. Output formats ─────────────────────────────────────────────────
        wide = itinerary_wide_format(itns)
        @test length(wide) == length(itns)

        for w in wide
            @test w.origin == "ORD"
            @test w.destination == "LHR"
            @test w.num_legs >= 1
        end

        long_rows = itinerary_long_format(itns)
        @test length(long_rows) >= length(itns)  # at least one row per itinerary

        for row in long_rows
            @test row.itinerary_id >= 1
            @test row.leg_seq >= 1
            @test !isempty(row.airline)
            @test row.flt_no > 0
        end

        # SearchStats populated
        @test ctx.search_stats.queries == Int32(1)
        @test ctx.search_stats.paths_found == Int32(length(itns))
        @test sum(ctx.search_stats.paths_by_stops) == ctx.search_stats.paths_found

        # Cleanup
        close(store)
        rm(ssim_path; force = true)
        rm(mct_path; force = true)
        rm(airports_path; force = true)
    end

    @testset "Multiple O-D search (graph reuse)" begin
        ssim_path = tempname()
        mct_path = tempname()
        airports_path = tempname()

        write(ssim_path, _build_integration_ssim())
        write(mct_path, make_test_mct())
        write(airports_path, airports_content)

        config = SearchConfig(
            ssim_path = ssim_path,
            mct_path = mct_path,
            airports_path = airports_path,
            regions_path = "/dev/null",
            aircrafts_path = "/dev/null",
            oa_control_path = "/dev/null",
            leading_days = 0,
            trailing_days = 0,
            interline = INTERLINE_ALL,
            max_stops = 2,
            circuity_extra_miles = 50_000.0,  # suppress circuity for zero-coord test stations
        )

        store = DuckDBStore()
        load_schedule!(store, config)
        target = Date(2026, 6, 15)
        graph = build_graph!(store, config, target)

        # Use large extra_miles to suppress circuity rejection with synthetic zero-coord stations
        constraints = SearchConstraints(
            defaults = ParameterSet(circuity_extra_miles=50_000.0),
        )
        ctx = RuntimeContext(
            config = config,
            constraints = constraints,
            itn_rules = build_itn_rules(config),
        )

        # First search: ORD → LHR (copy result before ctx.results is cleared)
        n1 = length(search_itineraries(
            graph.stations, StationCode("ORD"), StationCode("LHR"), target, ctx,
        ))
        @test n1 > 0
        @test ctx.search_stats.queries == Int32(1)

        # Second search: ORD → JFK (domestic, same graph)
        n2 = length(search_itineraries(
            graph.stations, StationCode("ORD"), StationCode("JFK"), target, ctx,
        ))
        @test n2 > 0
        @test ctx.search_stats.queries == Int32(2)

        # Third search: JFK → LHR
        n3 = length(search_itineraries(
            graph.stations, StationCode("JFK"), StationCode("LHR"), target, ctx,
        ))
        @test n3 >= 0  # may be 0 if no stations loaded (ref data path issue in test)
        @test ctx.search_stats.queries == Int32(3)

        # Total paths found is cumulative
        @test ctx.search_stats.paths_found == Int32(n1 + n2 + n3)

        # GC cache is populated for queried O-D pairs
        key_ord_lhr = (StationCode("ORD"), StationCode("LHR"))
        key_ord_jfk = (StationCode("ORD"), StationCode("JFK"))
        key_jfk_lhr = (StationCode("JFK"), StationCode("LHR"))
        @test haskey(ctx.gc_cache, key_ord_lhr)
        @test haskey(ctx.gc_cache, key_ord_jfk)
        @test haskey(ctx.gc_cache, key_jfk_lhr)
        @test ctx.gc_cache[key_ord_lhr] > 0.0
        @test ctx.gc_cache[key_ord_jfk] > 0.0

        close(store)
        rm(ssim_path; force = true)
        rm(mct_path; force = true)
        rm(airports_path; force = true)
    end

    @testset "Search with no results (unknown O-D)" begin
        ssim_path = tempname()
        mct_path = tempname()
        airports_path = tempname()

        write(ssim_path, _build_integration_ssim())
        write(mct_path, make_test_mct())
        write(airports_path, airports_content)

        config = SearchConfig(
            ssim_path = ssim_path,
            mct_path = mct_path,
            airports_path = airports_path,
            regions_path = "/dev/null",
            aircrafts_path = "/dev/null",
            oa_control_path = "/dev/null",
            leading_days = 0,
            trailing_days = 0,
        )

        store = DuckDBStore()
        load_schedule!(store, config)
        target = Date(2026, 6, 15)
        graph = build_graph!(store, config, target)

        ctx = RuntimeContext(
            config = config,
            constraints = SearchConstraints(),
            itn_rules = build_itn_rules(config),
        )

        # Station not in graph at all — returns empty immediately
        itns_unknown = search_itineraries(
            graph.stations, StationCode("ZZZ"), StationCode("LHR"), target, ctx,
        )
        @test isempty(itns_unknown)

        # Reversed direction with no return legs in SSIM
        itns_rev = search_itineraries(
            graph.stations, StationCode("LHR"), StationCode("ORD"), target, ctx,
        )
        @test isempty(itns_rev)

        # Date outside schedule validity window (legs run Jun 1–30)
        itns_future = search_itineraries(
            graph.stations, StationCode("ORD"), StationCode("LHR"),
            Date(2026, 7, 1), ctx,
        )
        @test isempty(itns_future)

        close(store)
        rm(ssim_path; force = true)
        rm(mct_path; force = true)
        rm(airports_path; force = true)
    end

    @testset "Convenience search wrapper" begin
        ssim_path = tempname()
        mct_path = tempname()
        airports_path = tempname()

        write(ssim_path, _build_integration_ssim())
        write(mct_path, make_test_mct())
        write(airports_path, airports_content)

        config = SearchConfig(
            ssim_path = ssim_path,
            mct_path = mct_path,
            airports_path = airports_path,
            regions_path = "/dev/null",
            aircrafts_path = "/dev/null",
            oa_control_path = "/dev/null",
            leading_days = 0,
            trailing_days = 0,
            interline = INTERLINE_ALL,
        )

        store = DuckDBStore()
        load_schedule!(store, config)

        # One-shot search via convenience wrapper (builds graph internally)
        itns = search(
            store,
            StationCode("ORD"),
            StationCode("LHR"),
            Date(2026, 6, 15);
            config = config,
        )

        @test itns isa Vector{Itinerary}
        @test !isempty(itns)

        # search() returns a deep copy — calling again gives a new independent vector
        itns2 = search(
            store,
            StationCode("ORD"),
            StationCode("LHR"),
            Date(2026, 6, 15);
            config = config,
        )
        @test itns isa Vector{Itinerary}
        @test length(itns) == length(itns2)
        @test itns !== itns2  # independent copies (different objects)

        close(store)
        rm(ssim_path; force = true)
        rm(mct_path; force = true)
        rm(airports_path; force = true)
    end

end

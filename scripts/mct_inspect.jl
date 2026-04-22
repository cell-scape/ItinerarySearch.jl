#!/usr/bin/env julia
# scripts/mct_inspect.jl — Interactive MCT audit inspector
#
# Lightweight mode (MCT file + misconnect CSV only, no schedule needed):
#   julia --project=. scripts/mct_inspect.jl data/input/UA_Misconnect_Report.csv
#   julia --project=. scripts/mct_inspect.jl data/input/UAOA_Misconnect_Report.csv
#   julia --project=. scripts/mct_inspect.jl data/input/OA_Misconnect_Report.csv
#
# Custom MCT file:
#   julia --project=. scripts/mct_inspect.jl data/input/UA_Misconnect_Report.csv --mct path/to/mct.dat
#
# Replay mode (write comparison CSV instead of interactive):
#   julia --project=. scripts/mct_inspect.jl data/input/UA_Misconnect_Report.csv --replay
#   julia --project=. scripts/mct_inspect.jl data/input/UA_Misconnect_Report.csv --replay --detailed
#
# Search mode (run a search and inspect each connection in each result):
#   julia --project=. scripts/mct_inspect.jl --search ORD LHR 2026-03-20
#   2-stop itineraries contribute both connections in sequence — step through
#   them with the same n/p/c/b commands as misconnect rows.

using ItinerarySearch
using Dates
import ItinerarySearch: materialize_mct_lookup, DuckDBStore, ingest_mct!,
    load_airports!, load_aircrafts!, load_regions!, StationRecord

# ── Parse arguments ──────────────────────────────────────────────────────────

function parse_args()
    misconnect_path = ""
    mct_path = ""
    airports_path = ""
    aircrafts_path = ""
    regions_path = ""
    replay = false
    detailed = false
    search_mode = false
    search_org = ""
    search_dst = ""
    search_date = ""

    i = 1
    while i <= length(ARGS)
        arg = ARGS[i]
        if arg == "--mct" && i < length(ARGS)
            mct_path = ARGS[i + 1]
            i += 2
        elseif arg == "--airports" && i < length(ARGS)
            airports_path = ARGS[i + 1]
            i += 2
        elseif arg == "--aircrafts" && i < length(ARGS)
            aircrafts_path = ARGS[i + 1]
            i += 2
        elseif arg == "--regions" && i < length(ARGS)
            regions_path = ARGS[i + 1]
            i += 2
        elseif arg == "--replay"
            replay = true
            i += 1
        elseif arg == "--detailed"
            detailed = true
            i += 1
        elseif arg == "--search" && i + 3 <= length(ARGS)
            search_mode = true
            search_org = ARGS[i + 1]
            search_dst = ARGS[i + 2]
            search_date = ARGS[i + 3]
            i += 4
        elseif !startswith(arg, "-") && isempty(misconnect_path)
            misconnect_path = arg
            i += 1
        else
            i += 1
        end
    end

    # Defaults
    dir = pkgdir(ItinerarySearch)
    _default(filename) = dir !== nothing ?
        joinpath(dir, "data", "input", filename) :
        joinpath("data", "input", filename)

    isempty(mct_path) && (mct_path = _default("MCTIMFILUA.DAT"))
    isempty(airports_path) && (airports_path = _default("mdstua.txt"))
    isempty(aircrafts_path) && (aircrafts_path = _default("aircraft.txt"))
    isempty(regions_path) && (regions_path = _default("REGIMFILUA.DAT"))

    return (; misconnect_path, mct_path, airports_path, aircrafts_path,
            regions_path, replay, detailed,
            search_mode, search_org, search_dst, search_date)
end

# ── Main ─────────────────────────────────────────────────────────────────────

function main()
    args = parse_args()

    if isempty(args.misconnect_path) && !args.search_mode
        println("Usage: julia --project=. scripts/mct_inspect.jl <misconnect.csv> [--mct mct.dat] [--replay] [--detailed]")
        println("       julia --project=. scripts/mct_inspect.jl --search ORG DST YYYY-MM-DD [--mct mct.dat]")
        println()
        println("Examples:")
        println("  julia --project=. scripts/mct_inspect.jl data/input/UA_Misconnect_Report.csv")
        println("  julia --project=. scripts/mct_inspect.jl data/input/UAOA_Misconnect_Report.csv --replay")
        println("  julia --project=. scripts/mct_inspect.jl --search ORD LHR 2026-03-20")
        exit(1)
    end

    if !args.search_mode && !isfile(args.misconnect_path)
        println("Error: misconnect file not found: $(args.misconnect_path)")
        exit(1)
    end

    println("MCT Audit Inspector")
    println("="^50)
    if args.search_mode
        println("  Search:     $(args.search_org) → $(args.search_dst) on $(args.search_date)")
    else
        println("  Misconnect: $(args.misconnect_path)")
    end
    println("  MCT file:   $(args.mct_path)")
    println("  Airports:   $(args.airports_path)")
    println("  Regions:    $(args.regions_path)")
    println("  Aircrafts:  $(args.aircrafts_path)")
    println()

    # Load MCT lookup
    println("Loading data...")
    store = DuckDBStore()
    ingest_mct!(store, args.mct_path)

    # Load airports for region resolution
    airports = Dict{StationCode,StationRecord}()
    if isfile(args.airports_path)
        load_airports!(store, args.airports_path)
        result = DBInterface.execute(store.db,
            "SELECT code, country, state, city, region, latitude, longitude, utc_offset FROM stations")
        for r in result
            code = StationCode(string(r.code))
            airports[code] = StationRecord(
                code = code,
                country = InlineStrings.InlineString3(string(something(r.country, ""))),
                state = InlineStrings.InlineString3(string(something(r.state, ""))),
                city = InlineStrings.InlineString3(string(something(r.city, ""))),
                region = InlineStrings.InlineString3(string(something(r.region, ""))),
                latitude = Float64(something(r.latitude, 0.0)),
                longitude = Float64(something(r.longitude, 0.0)),
                utc_offset = Int16(something(r.utc_offset, 0)),
            )
        end
        println("  Loaded $(length(airports)) airports")

        # Load regions and join onto airports.
        # For SCH/EUR: prefer SCH on the StationRecord (matches default sch_then_eur mode),
        # but track all regions so the inspector can do Schengen fallback.
        station_regions = Dict{StationCode,Set{InlineStrings.InlineString3}}()
        if isfile(args.regions_path)
            load_regions!(store, args.regions_path)
            result = DBInterface.execute(store.db,
                "SELECT airport, region FROM regions")
            for r in result
                code = StationCode(strip(string(r.airport)))
                rgn = InlineStrings.InlineString3(strip(string(something(r.region, ""))))
                isempty(rgn) && continue
                push!(get!(station_regions, code, Set{InlineStrings.InlineString3}()), rgn)
            end
            # Set the primary region on StationRecord: prefer SCH over EUR
            region_count = 0
            _SCH = InlineStrings.InlineString3("SCH")
            _EUR = InlineStrings.InlineString3("EUR")
            for (code, rgns) in station_regions
                haskey(airports, code) || continue
                # Pick primary: SCH if available, else first non-empty
                primary = _SCH in rgns ? _SCH :
                          _EUR in rgns ? _EUR : first(rgns)
                old = airports[code]
                airports[code] = StationRecord(
                    code=old.code, country=old.country, state=old.state,
                    city=old.city, region=primary, latitude=old.latitude,
                    longitude=old.longitude, utc_offset=old.utc_offset)
                region_count += 1
            end
            println("  Joined $(region_count) regions onto airports (SCH preferred)")
        end
    end

    # Load aircrafts for body type resolution
    acft_body = Dict{String,Char}()
    if isfile(args.aircrafts_path)
        load_aircrafts!(store, args.aircrafts_path)
        result = DBInterface.execute(store.db, "SELECT code, body_type FROM aircrafts")
        for r in result
            code = strip(string(r.code))
            bt = strip(string(something(r.body_type, "")))
            isempty(code) && continue
            acft_body[code] = isempty(bt) ? 'N' : (bt[1] == 'W' ? 'W' : 'N')
        end
        println("  Loaded $(length(acft_body)) aircraft body types")
    end

    lookup = materialize_mct_lookup(store)
    close(store)
    println("  MCT lookup ready")
    println()

    if args.replay && !args.search_mode
        # Replay mode — write comparison CSV/JSONL
        detail = args.detailed ? :detailed : :summary
        ext = args.detailed ? ".jsonl" : ".csv"
        base = replace(basename(args.misconnect_path), ".csv" => "")
        outpath = "data/output/$(base)_replay$(ext)"
        mkpath(dirname(outpath))
        io = open(outpath, "w")
        replay_misconnects(args.misconnect_path, lookup;
            output_io=io, detail=detail, airports=airports, acft_body=acft_body)
        close(io)
        println("Replay written to: $outpath")
    elseif args.search_mode
        # Search mode — load schedule, run a search, hand the resulting
        # itineraries to the inspector.  Each itinerary contributes one row
        # per connection (a 2-stop yields two rows in sequence), so the user
        # can step through them with the same n/p/c/b commands.
        println("\nLoading schedule (this may take a moment)...")
        cfg = SearchConfig()
        sched_store = DuckDBStore()
        load_schedule!(sched_store, cfg)
        target = Date(args.search_date)
        println("Building graph for $(target)...")
        graph = build_graph!(sched_store, cfg, target)
        ctx = RuntimeContext(
            config = cfg,
            constraints = SearchConstraints(),
            itn_rules = build_itn_rules(cfg),
        )
        println("Searching $(args.search_org) → $(args.search_dst)...")
        itineraries = search_itineraries(graph.stations,
            StationCode(args.search_org), StationCode(args.search_dst),
            target, ctx)
        n_with_cnx = count(i -> length(i.connections) > 1, itineraries)
        println("Found $(length(itineraries)) itineraries " *
                "($(n_with_cnx) with at least one connection)\n")
        if n_with_cnx == 0
            println("No connecting itineraries to inspect (all results are nonstops).")
            close(sched_store)
            return
        end
        mct_inspect(lookup;
            itineraries = collect(itineraries),
            airports = airports,
            station_regions = station_regions,
            acft_body = acft_body)
        close(sched_store)
    else
        # Interactive mode (misconnect CSV)
        mct_inspect(lookup;
            misconnect=args.misconnect_path,
            airports=airports,
            station_regions=station_regions,
            acft_body=acft_body)
    end
end

using DBInterface, InlineStrings
main()

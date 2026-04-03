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

using ItinerarySearch
import ItinerarySearch: materialize_mct_lookup, DuckDBStore, ingest_mct!,
    load_airports!, StationRecord

# ── Parse arguments ──────────────────────────────────────────────────────────

function parse_args()
    misconnect_path = ""
    mct_path = ""
    airports_path = ""
    replay = false
    detailed = false

    i = 1
    while i <= length(ARGS)
        arg = ARGS[i]
        if arg == "--mct" && i < length(ARGS)
            mct_path = ARGS[i + 1]
            i += 2
        elseif arg == "--airports" && i < length(ARGS)
            airports_path = ARGS[i + 1]
            i += 2
        elseif arg == "--replay"
            replay = true
            i += 1
        elseif arg == "--detailed"
            detailed = true
            i += 1
        elseif !startswith(arg, "-") && isempty(misconnect_path)
            misconnect_path = arg
            i += 1
        else
            i += 1
        end
    end

    # Defaults
    if isempty(mct_path)
        dir = pkgdir(ItinerarySearch)
        mct_path = dir !== nothing ?
            joinpath(dir, "data", "input", "MCTIMFILUA.DAT") :
            "data/input/MCTIMFILUA.DAT"
    end
    if isempty(airports_path)
        dir = pkgdir(ItinerarySearch)
        airports_path = dir !== nothing ?
            joinpath(dir, "data", "input", "mdstua.txt") :
            "data/input/mdstua.txt"
    end

    return (; misconnect_path, mct_path, airports_path, replay, detailed)
end

# ── Main ─────────────────────────────────────────────────────────────────────

function main()
    args = parse_args()

    if isempty(args.misconnect_path)
        println("Usage: julia --project=. scripts/mct_inspect.jl <misconnect.csv> [--mct mct.dat] [--replay] [--detailed]")
        println()
        println("Examples:")
        println("  julia --project=. scripts/mct_inspect.jl data/input/UA_Misconnect_Report.csv")
        println("  julia --project=. scripts/mct_inspect.jl data/input/UAOA_Misconnect_Report.csv --replay")
        exit(1)
    end

    if !isfile(args.misconnect_path)
        println("Error: misconnect file not found: $(args.misconnect_path)")
        exit(1)
    end

    println("MCT Audit Inspector")
    println("="^50)
    println("  Misconnect: $(args.misconnect_path)")
    println("  MCT file:   $(args.mct_path)")
    println("  Airports:   $(args.airports_path)")
    println()

    # Load MCT lookup
    println("Loading MCT data...")
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
    end

    lookup = materialize_mct_lookup(store)
    close(store)
    println("  MCT lookup ready")
    println()

    if args.replay
        # Replay mode — write comparison CSV/JSONL
        detail = args.detailed ? :detailed : :summary
        ext = args.detailed ? ".jsonl" : ".csv"
        base = replace(basename(args.misconnect_path), ".csv" => "")
        outpath = "data/output/$(base)_replay$(ext)"
        mkpath(dirname(outpath))
        io = open(outpath, "w")
        replay_misconnects(args.misconnect_path, lookup;
            output_io=io, detail=detail, airports=airports)
        close(io)
        println("Replay written to: $outpath")
    else
        # Interactive mode
        mct_inspect(lookup;
            misconnect=args.misconnect_path,
            airports=airports)
    end
end

using DBInterface, InlineStrings
main()

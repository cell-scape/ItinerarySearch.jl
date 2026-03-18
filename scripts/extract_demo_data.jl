#!/usr/bin/env julia
# scripts/extract_demo_data.jl — Extract demo dataset from full TripBuilder data
#
# Usage: julia --project=. scripts/extract_demo_data.jl [--ssim PATH] [--mct PATH] [--outdir PATH]
#
# Defaults to TripBuilder data/ paths. Output to data/demo/

using Dates
using CodecZstd, TranscodingStreams

# Demo station set (57 stations across all 12 IATA regions)
const DEMO_STATIONS = Set([
    # NOA
    "ORD", "LAX", "SFO", "EWR", "IAH", "DEN", "IAD", "YYZ", "MEX", "SJU", "HNL",
    # EUR
    "LHR", "CDG", "AMS", "MAD", "BCN", "LIS", "ATH",
    # SCH
    "FRA", "MUC", "ZRH", "VIE", "BRU", "FCO", "GVA", "BER", "HAM", "CPH", "OSL", "ARN", "HEL", "WAW", "PRG", "BUD",
    # SWP
    "SYD", "MEL", "AKL", "GUM", "PPT",
    # SEA
    "NRT", "HND", "PVG", "ICN", "SIN",
    # SOA
    "EZE", "GRU", "BOG", "SCL", "LIM",
    # AFR
    "JNB", "ADD",
    # MDE
    "DXB", "DOH",
    # CAR
    "CUN",
    # CEM
    "PTY", "SJO",
    # SAS
    "DEL", "BOM",
])

function extract_ssim(input_path::String, output_path::String)
    println("Extracting SSIM from $input_path...")
    type3_count = 0
    type4_count = 0

    open(ZstdCompressorStream, output_path, "w"; level=19) do out
        current_type3_included = false
        for line in eachline(input_path)
            length(line) < 1 && continue
            rt = line[1]

            if rt == '1' || rt == '2' || rt == '5'
                # Header, carrier, trailer — always include
                println(out, line)
            elseif rt == '3' && length(line) >= 57
                org = strip(line[37:39])
                dst = strip(line[55:57])
                if org in DEMO_STATIONS && dst in DEMO_STATIONS
                    println(out, line)
                    current_type3_included = true
                    type3_count += 1
                else
                    current_type3_included = false
                end
            elseif rt == '4'
                if current_type3_included
                    println(out, line)
                    type4_count += 1
                end
            end
        end
    end

    println("  Type 3: $type3_count legs")
    println("  Type 4: $type4_count DEI records")
    println("  Written to $output_path")
end

function extract_mct(input_path::String, output_path::String)
    println("Extracting MCT from $input_path...")
    count = 0

    open(ZstdCompressorStream, output_path, "w"; level=19) do out
        for line in eachline(input_path)
            length(line) < 1 && continue
            rt = line[1]

            if rt == '1'
                # Header — always include
                println(out, line)
                count += 1
            elseif rt == '2' && length(line) >= 13
                arr_stn = strip(line[2:4])
                dep_stn = strip(line[11:13])
                # Include if: global record (blank stations) OR either station in demo set
                if isempty(arr_stn) || isempty(dep_stn) ||
                   arr_stn in DEMO_STATIONS || dep_stn in DEMO_STATIONS
                    println(out, line)
                    count += 1
                end
            elseif rt == '3'
                # Connection Building Filter — always include
                println(out, line)
                count += 1
            end
        end
    end

    println("  MCT records: $count")
    println("  Written to $output_path")
end

function main()
    # Default paths
    ssim_in = get(ENV, "SSIM_PATH", joinpath(@__DIR__, "..", "..", "TripBuilder", "data", "dataset", "uaoa_ssim.new.dat"))
    mct_in = get(ENV, "MCT_PATH", joinpath(@__DIR__, "..", "..", "TripBuilder", "data", "MCTIMFILUA.DAT"))
    outdir = get(ENV, "OUTDIR", joinpath(@__DIR__, "..", "data", "demo"))

    mkpath(outdir)

    if isfile(ssim_in)
        extract_ssim(ssim_in, joinpath(outdir, "ssim_demo.dat.zst"))
    else
        println("SSIM file not found: $ssim_in")
        println("Set SSIM_PATH environment variable")
    end

    if isfile(mct_in)
        extract_mct(mct_in, joinpath(outdir, "mct_demo.dat.zst"))
    else
        println("MCT file not found: $mct_in")
        println("Set MCT_PATH environment variable")
    end

    # Copy reference tables (small, no filtering needed)
    ref_dir = joinpath(@__DIR__, "..", "..", "TripBuilder", "data")
    for file in ["mdstua.txt", "REGIMFILUA.DAT", "aircraft.txt"]
        src = joinpath(ref_dir, file)
        if isfile(src)
            dst_name = file == "mdstua.txt" ? "airports.txt" :
                       file == "REGIMFILUA.DAT" ? "regions.dat" : file
            cp(src, joinpath(outdir, dst_name); force=true)
            println("Copied $file → $dst_name")
        end
    end

    println("\nDone! Demo data in $outdir")
end

main()

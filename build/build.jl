#!/usr/bin/env julia
# build/build.jl — Build a sysimage or standalone app with PackageCompiler
#
# Usage:
#   julia --project=. build/build.jl sysimage              # custom sysimage
#   julia --project=. build/build.jl sysimage --output path # custom output path
#   julia --project=. build/build.jl app                    # standalone app
#   julia --project=. build/build.jl app --output path      # custom app dir
#
# The test suite is used as the precompile execution file, ensuring all
# code paths exercised by tests are compiled into native code.
#
# Sysimage: load with `julia --sysimage=build/ItinerarySearch.so`
# App:      run directly as `build/app/bin/ItinerarySearch`

using Pkg
Pkg.activate(dirname(@__DIR__))

# PackageCompiler is a build-time dependency — install if not present
try
    @eval using PackageCompiler
catch
    println("Installing PackageCompiler...")
    Pkg.add("PackageCompiler")
    @eval using PackageCompiler
end

# ── Parse arguments ───────────────────────────────────────────────────────────

function parse_build_args(args)
    mode = length(args) >= 1 ? args[1] : "sysimage"
    output = nothing
    trim = "safe"

    i = 2
    while i <= length(args)
        if args[i] == "--output" && i + 1 <= length(args)
            output = args[i + 1]
            i += 2
        elseif args[i] == "--trim" && i + 1 <= length(args)
            trim = args[i + 1]
            i += 2
        else
            println(stderr, "Unknown argument: $(args[i])")
            exit(2)
        end
    end

    if mode ∉ ("sysimage", "app", "juliac")
        println(stderr, "Usage: julia build/build.jl [sysimage|app|juliac] [--output PATH] [--trim safe|unsafe|no]")
        exit(2)
    end

    return (; mode, output, trim)
end

# ── Build ─────────────────────────────────────────────────────────────────────

function main(args)
    opts = parse_build_args(args)
    project_dir = dirname(@__DIR__)
    test_file = joinpath(project_dir, "test", "runtests.jl")
    build_dir = joinpath(project_dir, "build")
    mkpath(build_dir)

    println("ItinerarySearch Build")
    println("="^50)
    println("  Mode:              $(opts.mode)")
    println("  Project:           $project_dir")
    println("  Precompile file:   $test_file")

    if opts.mode == "sysimage"
        output = something(opts.output, joinpath(build_dir, "ItinerarySearch.so"))
        println("  Output:            $output")
        println()
        println("Building sysimage...")

        create_sysimage(
            :ItinerarySearch;
            sysimage_path = output,
            precompile_execution_file = test_file,
            project = project_dir,
        )

        filesize_mb = round(filesize(output) / 1024^2; digits=1)
        println("\nSysimage built: $output ($filesize_mb MB)")
        println("Run with: julia --sysimage=$output --project=. -e 'using ItinerarySearch'")

    elseif opts.mode == "app"
        output = something(opts.output, joinpath(build_dir, "app"))
        println("  Output:            $output")
        println()
        println("Building app...")

        create_app(
            project_dir,
            output;
            precompile_execution_file = test_file,
            executables = ["itinsearch" => "bin/itinsearch.jl"],
            force = true,
        )

        println("\nApp built: $output/")
        println("Run with: $output/bin/itinsearch --help")

    elseif opts.mode == "juliac"
        output = something(opts.output, joinpath(build_dir, "itinsearch"))
        entry_file = joinpath(build_dir, "juliac_entry.jl")
        juliac_script = joinpath(Sys.BINDIR, "..", "share", "julia", "juliac", "juliac.jl")

        if !isfile(juliac_script)
            println(stderr, "Error: juliac.jl not found at $juliac_script")
            println(stderr, "juliac requires Julia 1.12+")
            return
        end

        println("  Output:            $output")
        println("  Entry point:       $entry_file")
        println("  Trim mode:         $(opts.trim)")
        println()
        println("Building with juliac (this may take several minutes)...")

        trim_arg = "--trim=$(opts.trim)"
        cmd = `$(Base.julia_cmd()) --startup-file=no $juliac_script
            --output-exe $output
            --experimental $trim_arg
            --verbose
            $entry_file`

        println("  Command: $cmd")
        println()
        run(cmd)

        if isfile(output)
            filesize_mb = round(filesize(output) / 1024^2; digits=1)
            println("\njuliac binary built: $output ($filesize_mb MB)")
            println("Run with: $output --help")
        else
            println(stderr, "\njuliac build may have failed — check output above")
        end
    end
end

main(ARGS)

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

    i = 2
    while i <= length(args)
        if args[i] == "--output" && i + 1 <= length(args)
            output = args[i + 1]
            i += 2
        else
            println(stderr, "Unknown argument: $(args[i])")
            exit(2)
        end
    end

    if mode ∉ ("sysimage", "app")
        println(stderr, "Usage: julia build/build.jl [sysimage|app] [--output PATH]")
        exit(2)
    end

    return (; mode, output)
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
    end
end

main(ARGS)

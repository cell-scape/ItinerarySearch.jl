# build/juliac_entry.jl — Entry point for juliac static compilation
#
# Usage:
#   julia /path/to/juliac.jl --output-exe itinsearch --experimental --trim=safe build/juliac_entry.jl
#
# Or via the build script:
#   julia --project=. build/build.jl juliac
#
# This file defines Main.main(args::Vector{String})::Cint which juliac
# expects as the entry point for --output-exe.

# Load the package (juliac resolves this at compile time)
include(joinpath(@__DIR__, "..", "src", "ItinerarySearch.jl"))
using .ItinerarySearch

function main(args::Vector{String})::Cint
    try
        return Cint(ItinerarySearch.CLI.main(args))
    catch e
        println(stderr, "Fatal: ", sprint(showerror, e))
        return Cint(1)
    end
end

#!/usr/bin/env julia
# bin/itinsearch.jl — CLI entry point for ItinerarySearch
#
# Usage: julia --project=. bin/itinsearch.jl [command] [args...]
#   or:  julia --project=/path/to/ItinerarySearch.jl bin/itinsearch.jl [command] [args...]

using Pkg
Pkg.activate(dirname(@__DIR__); io=devnull)
using ItinerarySearch
exit(ItinerarySearch.CLI.main(ARGS))

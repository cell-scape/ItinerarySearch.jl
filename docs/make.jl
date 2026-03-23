using Documenter
using ItinerarySearch

makedocs(
    sitename = "ItinerarySearch.jl",
    modules  = [ItinerarySearch],
    source   = joinpath(@__DIR__, "src"),
    build    = joinpath(@__DIR__, "build"),
    format   = Documenter.HTML(
        prettyurls = get(ENV, "CI", nothing) == "true",
    ),
    pages = [
        "Home"             => "index.md",
        "Architecture"     => "architecture.md",
        "Getting Started"  => "getting-started.md",
        "API Reference"    => [
            "Types"           => "api/types.md",
            "Ingest"          => "api/ingest.md",
            "Graph & Search"  => "api/graph.md",
            "Output & Formats"=> "api/output.md",
        ],
        "Itinerary Leg Index" => "leg-index.md",
    ],
)

deploydocs(
    repo = "github.com/yourorg/ItinerarySearch.jl.git",
    devbranch = "main",
)

using Test
using Documenter
using ItinerarySearch

@testset "Doctests" begin
    DocMeta.setdocmeta!(ItinerarySearch, :DocTestSetup,
        :(using ItinerarySearch, Dates, InlineStrings); recursive=true)
    doctest(ItinerarySearch; manual=false)
end

using Test
using JET
using Aqua
using ItinerarySearch

@testset "JET" begin
    JET.test_package(ItinerarySearch; target_modules=(ItinerarySearch,))
end

@testset "Aqua" begin
    Aqua.test_all(ItinerarySearch)
end

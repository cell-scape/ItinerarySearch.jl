include("_test_setup.jl")

@testset "RuntimeContext worker_slot field" begin
    @testset "defaults to 0" begin
        ctx = ItinerarySearch.RuntimeContext()
        @test ctx.worker_slot == 0
    end

    @testset "settable via kwarg" begin
        ctx = ItinerarySearch.RuntimeContext(worker_slot=7)
        @test ctx.worker_slot == 7
    end

    @testset "mutable — can be reassigned" begin
        ctx = ItinerarySearch.RuntimeContext()
        ctx.worker_slot = 3
        @test ctx.worker_slot == 3
    end
end

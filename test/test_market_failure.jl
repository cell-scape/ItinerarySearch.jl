include("_test_setup.jl")

@testset "MarketSearchFailure" begin
    @testset "construction and field access" begin
        (e, bt) = try
            throw(ArgumentError("bad airport"))
        catch ex
            (ex, stacktrace(catch_backtrace()))
        end
        fail = MarketSearchFailure(
            ("ORD", "XXX", Date(2026, 6, 15)),
            e, bt, 2, 12.5,
        )
        @test fail.market == ("ORD", "XXX", Date(2026, 6, 15))
        @test fail.exception isa ArgumentError
        @test !isempty(fail.backtrace)
        @test fail.worker_slot == 2
        @test fail.elapsed_ms == 12.5
    end

    @testset "is_failure predicate" begin
        (e, bt) = try
            throw(ArgumentError("x"))
        catch ex
            (ex, stacktrace(catch_backtrace()))
        end
        fail = MarketSearchFailure(("A", "B", Date(2026, 1, 1)), e, bt, 1, 0.0)
        @test is_failure(fail) === true
        @test is_failure(Itinerary[]) === false
        @test is_failure("not a failure") === false
    end

    @testset "failed_markets helper extracts sentinels from a dict" begin
        (e, bt) = try
            throw(ArgumentError("x"))
        catch ex
            (ex, stacktrace(catch_backtrace()))
        end
        fail1 = MarketSearchFailure(("A", "B", Date(2026, 1, 1)), e, bt, 1, 0.0)
        fail2 = MarketSearchFailure(("C", "D", Date(2026, 1, 2)), e, bt, 2, 0.0)
        d = Dict{Tuple{String,String,Date}, Union{Vector{Itinerary}, MarketSearchFailure}}(
            ("A", "B", Date(2026, 1, 1)) => fail1,
            ("X", "Y", Date(2026, 1, 1)) => Itinerary[],
            ("C", "D", Date(2026, 1, 2)) => fail2,
        )
        fails = failed_markets(d)
        @test length(fails) == 2
        @test eltype(fails) == MarketSearchFailure
        @test Set(f.market for f in fails) == Set([
            ("A", "B", Date(2026, 1, 1)),
            ("C", "D", Date(2026, 1, 2)),
        ])
    end
end

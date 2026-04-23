include("_test_setup.jl")

@testset "TraceContext and ID helpers" begin
    @testset "ID generation is non-zero with overwhelming probability" begin
        ids = [ItinerarySearch._new_trace_id() for _ in 1:100]
        @test all(!iszero, ids)
        @test length(unique(ids)) == 100       # all distinct (collision probability ~ 0)

        sids = [ItinerarySearch._new_span_id() for _ in 1:100]
        @test all(!iszero, sids)
        @test length(unique(sids)) == 100
    end

    @testset "TraceContext default root has parent_span_id == 0" begin
        tc = TraceContext(ItinerarySearch._new_trace_id(), UInt64(0))
        @test tc.parent_span_id == 0
        @test tc.trace_id != 0
    end

    @testset "_unix_nano_now monotonic and in sensible range" begin
        t1 = ItinerarySearch._unix_nano_now()
        t2 = ItinerarySearch._unix_nano_now()
        @test t2 >= t1
        # Assert at least in the 21st century (nanoseconds since 1970).
        # 2001-01-01 UTC = 978_307_200_000_000_000 ns.
        @test t1 > Int64(978_307_200_000_000_000)
        # And not absurdly far in the future.
        @test t1 < Int64(4_102_444_800_000_000_000)    # year 2100
    end
end

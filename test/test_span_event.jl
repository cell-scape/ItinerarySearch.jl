include("_test_setup.jl")

@testset "SpanEvent" begin
    @testset "construction with required fields" begin
        ev = SpanEvent(
            kind=:start, name=:search_markets,
            trace_id=UInt128(0x0123456789abcdef0123456789abcdef),
            span_id=UInt64(0xdeadbeef),
            parent_span_id=UInt64(0),
            unix_nano=Int64(1_700_000_000_000_000_000),
        )
        @test ev.kind === :start
        @test ev.name === :search_markets
        @test ev.parent_span_id == 0
        @test ev.worker_slot == 0        # default
        @test ev.status === :ok          # default
        @test isempty(ev.attributes)     # default
    end

    @testset "end event with attributes" begin
        ev = SpanEvent(
            kind=:end, name=:market_search,
            trace_id=UInt128(1), span_id=UInt64(2), parent_span_id=UInt64(3),
            unix_nano=Int64(100), worker_slot=4, status=:error,
            attributes=Dict{Symbol,Any}(:exception_type => "BoundsError"),
        )
        @test ev.status === :error
        @test ev.attributes[:exception_type] == "BoundsError"
    end

    @testset "kind accepts only :start or :end semantically (documented invariant)" begin
        # Not enforced at the type level — documented invariant. Just verify both work.
        @test SpanEvent(kind=:start, name=:x, trace_id=UInt128(0), span_id=UInt64(0),
                        parent_span_id=UInt64(0), unix_nano=Int64(0)).kind === :start
        @test SpanEvent(kind=:end,   name=:x, trace_id=UInt128(0), span_id=UInt64(0),
                        parent_span_id=UInt64(0), unix_nano=Int64(0)).kind === :end
        # The invariant is documentation-only, not type-level — any symbol is accepted at runtime.
        @test SpanEvent(kind=:unexpected, name=:x, trace_id=UInt128(0),
                        span_id=UInt64(0), parent_span_id=UInt64(0),
                        unix_nano=Int64(0)).kind === :unexpected
    end
end

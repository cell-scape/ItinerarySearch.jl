include("_test_setup.jl")
include("test_helpers.jl")

# Test subset selection via environment variables:
#   ITINSEARCH_SKIP_STATIC=1 → skip test_jet_aqua.jl (used by `make test` for fast iteration)
#   ITINSEARCH_ONLY_STATIC=1 → run only test_jet_aqua.jl (used by `make test-static`)
# Default (neither set) runs everything — used by `make test-all`.
const ITINSEARCH_ONLY_STATIC = get(ENV, "ITINSEARCH_ONLY_STATIC", "0") == "1"
const ITINSEARCH_SKIP_STATIC = get(ENV, "ITINSEARCH_SKIP_STATIC", "0") == "1"
if ITINSEARCH_ONLY_STATIC && ITINSEARCH_SKIP_STATIC
    error("ITINSEARCH_ONLY_STATIC and ITINSEARCH_SKIP_STATIC are mutually exclusive")
end

@testset "ItinerarySearch" begin
    # Static analysis (JET + Aqua) runs first so `make test-static` can stop here.
    ITINSEARCH_SKIP_STATIC || include("test_jet_aqua.jl")

    if !ITINSEARCH_ONLY_STATIC
    include("test_module_surface.jl")
    include("test_status.jl")
    include("test_stats.jl")
    include("test_graph_types.jl")
    include("test_constraints.jl")
    include("test_config.jl")
    include("test_compression.jl")
    include("test_ingest.jl")
    include("test_store.jl")
    include("test_schedule_queries.jl")
    include("test_mct_lookup.jl")
    include("test_rules_cnx.jl")
    include("test_rules_itn.jl")
    include("test_connect.jl")
    include("test_search.jl")
    include("test_builder.jl")
    include("test_formats.jl")
    include("test_integration_graph.jl")
    include("test_instrumentation.jl")
    include("test_observe.jl")
    include("test_span_event.jl")
    include("test_trace_context.jl")
    include("test_market_failure.jl")
    include("test_runtime_context_worker_slot.jl")
    include("test_parallel_markets_functional.jl")
    include("test_parallel_markets_failures.jl")
    include("test_logging.jl")
    include("test_cli.jl")
    include("test_server.jl")
    include("test_newssim_ingest.jl")
    include("test_ssim_parsing.jl")
    include("test_audit_misconnect.jl")
    include("test_circuity_tiers.jl")
    end  # if !ITINSEARCH_ONLY_STATIC
end

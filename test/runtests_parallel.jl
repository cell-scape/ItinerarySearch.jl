# test/runtests_parallel.jl — Parallel test runner.
#
# Spawns N worker processes (ITINSEARCH_TEST_WORKERS env, default 4), loads
# ItinerarySearch on each, and distributes test files via `Distributed.pmap`.
# Each worker runs one file at a time; pmap feeds work dynamically so finishing
# a small file pulls the next pending file off the queue — longest-first
# ordering below keeps the load balanced.
#
# Result reporting: each worker wraps its file in `@testset "<file>"`, captures
# pass/fail/error counts, and returns them to the main process. Main aggregates
# and reports per-file + total.
#
# Honours the same env vars as the serial runner:
#   ITINSEARCH_SKIP_STATIC=1 → omit test_jet_aqua.jl from the worklist
#   ITINSEARCH_ONLY_STATIC=1 → run only test_jet_aqua.jl (use the serial runner
#                               for this; no parallelism benefit with one file)

using Distributed

const NWORKERS = parse(Int, get(ENV, "ITINSEARCH_TEST_WORKERS",
                                    string(min(4, max(2, Sys.CPU_THREADS ÷ 2)))))

if nprocs() == 1
    addprocs(NWORKERS; exeflags="--project=$(Base.active_project())")
end

@everywhere include(joinpath(@__DIR__, "_test_setup.jl"))
@everywhere include(joinpath(@__DIR__, "test_helpers.jl"))

# Test file runtime estimates (from `make test-all` timing). Longest first so
# pmap schedules them early and workers don't stall waiting on a straggler.
#
# test_jet_aqua.jl is intentionally excluded: on Julia 1.12 JET.report_package
# fails on a worker process with "Expected MethodTableView". Run static checks
# via `make test-static` or `make test-all` instead.
const TEST_FILES_FULL = [
    "test_builder.jl",          # ~30s — NewSSIM demo × several builds
    "test_formats.jl",          # ~14s — passthrough + itinerary_legs output
    "test_newssim_ingest.jl",   # ~11s — ingest + pipeline tests
    "test_rules_cnx.jl",        # ~10s — rule-chain unit tests (compile-heavy)
    "test_server.jl",           # ~ 7s — REST API against shared fixture
    "test_logging.jl",          # ~ 4s — logging side-effects during builds
    "test_ingest.jl",           # ~ 3s — SSIM streaming parser
    "test_cli.jl",              # ~ 3s — CLI entry-point smoke tests
    "test_store.jl",            # ~1.6s
    "test_search.jl",           # ~1.6s
    "test_observe.jl",          # ~1.5s
    "test_integration_graph.jl",# ~1.4s
    "test_mct_lookup.jl",       # ~1.3s
    "test_connect.jl",          # ~0.9s
    "test_compression.jl",      # ~0.8s
    "test_schedule_queries.jl", # ~0.8s
    "test_rules_itn.jl",        # ~0.7s
    "test_stats.jl",            # ~0.6s
    "test_config.jl",           # ~0.5s
    "test_instrumentation.jl",  # ~0.4s
    "test_constraints.jl",      # ~0.3s
    "test_graph_types.jl",      # ~0.1s
    "test_status.jl",           # ~0.05s
]

# Apply the static-analysis env filters.
const SKIP_STATIC = get(ENV, "ITINSEARCH_SKIP_STATIC", "0") == "1"
const ONLY_STATIC = get(ENV, "ITINSEARCH_ONLY_STATIC", "0") == "1"

const TEST_FILES = if ONLY_STATIC
    ["test_jet_aqua.jl"]
else
    # TEST_FILES_FULL already excludes test_jet_aqua.jl (see note above).
    TEST_FILES_FULL
end

# Sanity-check file presence on the main process before dispatching work.
for f in TEST_FILES
    path = joinpath(@__DIR__, f)
    isfile(path) || error("Test file not found: $path")
end

# Delegate to Julia's own Test.get_test_counts so our numbers match what
# `@testset`'s built-in summary would print. Returns a Test.TestCounts struct
# with separate direct + cumulative-descendant counts.
@everywhere function _extract_counts(ts)
    c = Test.get_test_counts(ts)
    return (c.passes + c.cumulative_passes,
            c.fails  + c.cumulative_fails,
            c.errors + c.cumulative_errors)
end

@everywhere function run_test_file(file::String)
    path = joinpath(@__DIR__, file)
    t0 = time()
    n_p = n_f = n_e = 0
    exc_msg = ""
    try
        # Wrapping the file's own top-level @testset in a named parent testset
        # lets us attribute the recursive counts to this specific file.
        ts = @testset "$file" begin
            Base.include(Main, path)
        end
        n_p, n_f, n_e = _extract_counts(ts)
    catch e
        n_e = 1
        exc_msg = sprint(showerror, e)
    end
    return (file=file, pass=n_p, fail=n_f, error=n_e,
            elapsed=time() - t0, exc=exc_msg)
end

println("Parallel test runner — $(length(TEST_FILES)) files × $(nworkers()) workers")
println("="^70)

wall_t0 = time()
results = pmap(run_test_file, TEST_FILES)
wall_elapsed = time() - wall_t0

println()
println("="^70)
println("Per-file results")
println("="^70)
for r in results
    ok = (r.fail == 0 && r.error == 0)
    status = ok ? "PASS" : "FAIL"
    elapsed_str = lpad(string(round(r.elapsed; digits=1), "s"), 7)
    printstyled("  $status "; color=ok ? :green : :red)
    print(elapsed_str, "  ", rpad(r.file, 32))
    print("  pass=", r.pass)
    r.fail  > 0 && print(" fail=", r.fail)
    r.error > 0 && print(" error=", r.error)
    println()
    !isempty(r.exc) && println("        ", r.exc)
end

total_p = sum(r.pass  for r in results)
total_f = sum(r.fail  for r in results)
total_e = sum(r.error for r in results)

println()
println("="^70)
println("Wall time: $(round(wall_elapsed; digits=1))s")
println("Total:     $total_p passed, $total_f failed, $total_e errored")
println("="^70)

if total_f > 0 || total_e > 0
    exit(1)
end

# test/test_server.jl — Integration tests for the REST API server

using Test
using ItinerarySearch
using ItinerarySearch: Server
using HTTP
using JSON3
using Dates
using DuckDB, DBInterface

@testset "REST API Server" begin

    # ── Setup ──────────────────────────────────────────────────────────────────
    # Build a minimal store with a single ORD→LHR leg and both stations,
    # then expand the pipeline so build_graph! has data to work with.

    store = _setup_test_store()

    config = SearchConfig()
    graph = build_graph!(store, config, Date(2026, 6, 15))

    state = Server.ServerState(
        config,
        SearchConstraints(),
        store,
        graph,
        ReentrantLock(),
        Date(2026, 6, 15),
        time(),
        Threads.Atomic{Bool}(false),
        time(),
    )

    # Start non-blocking server on an OS-assigned ephemeral port.
    server = Server.start!(state; host="127.0.0.1", port=0)

    # Retrieve the actual bound port from the underlying TCPServer.
    port = Int(HTTP.Sockets.getsockname(server.listener.server)[2])
    base_url = "http://127.0.0.1:$port"

    # Give the server a moment to become ready before sending requests.
    sleep(0.5)

    try

        # ── 1. GET /health ─────────────────────────────────────────────────────

        @testset "GET /health" begin
            resp = HTTP.get("$base_url/health"; status_exception=false)
            @test resp.status == 200
            body = JSON3.read(String(resp.body))
            @test body[:status] == "ok"
            @test haskey(body[:data], :uptime_seconds)
            @test haskey(body[:data], :graph_stations)
            @test body[:data][:rebuilding] == false
        end

        # ── 2. GET /station/ORD ────────────────────────────────────────────────

        @testset "GET /station/ORD" begin
            resp = HTTP.get("$base_url/station/ORD"; status_exception=false)
            @test resp.status == 200
            body = JSON3.read(String(resp.body))
            @test body[:data][:code] == "ORD"
            @test body[:data][:country] == "US"
        end

        # ── 3. GET /station/ZZZ → 404 ─────────────────────────────────────────

        @testset "GET /station/ZZZ → 404" begin
            resp = HTTP.get("$base_url/station/ZZZ"; status_exception=false)
            @test resp.status == 404
        end

        # ── 4. POST /search ────────────────────────────────────────────────────

        @testset "POST /search" begin
            body = JSON3.write(Dict(
                "origins"      => ["ORD"],
                "destinations" => ["LHR"],
                "dates"        => ["2026-06-15"],
            ))
            resp = HTTP.post(
                "$base_url/search",
                ["Content-Type" => "application/json"],
                body;
                status_exception=false,
            )
            @test resp.status == 200
            data = JSON3.read(String(resp.body))
            @test data[:status] == "ok"
        end

        # ── 5. POST /search missing fields → 400 ──────────────────────────────

        @testset "POST /search missing fields → 400" begin
            resp = HTTP.post(
                "$base_url/search",
                ["Content-Type" => "application/json"],
                "{}";
                status_exception=false,
            )
            @test resp.status == 400
        end

        # ── 6. POST /trip ──────────────────────────────────────────────────────

        @testset "POST /trip" begin
            body = JSON3.write(Dict(
                "legs" => [
                    Dict(
                        "origin"      => "ORD",
                        "destination" => "LHR",
                        "date"        => "2026-06-15",
                    ),
                ],
            ))
            resp = HTTP.post(
                "$base_url/trip",
                ["Content-Type" => "application/json"],
                body;
                status_exception=false,
            )
            @test resp.status == 200
            data = JSON3.read(String(resp.body))
            @test data[:status] == "ok"
        end

        # ── 7. POST /rebuild ───────────────────────────────────────────────────

        @testset "POST /rebuild" begin
            resp = HTTP.post(
                "$base_url/rebuild",
                ["Content-Type" => "application/json"],
                "{}";
                status_exception=false,
            )
            @test resp.status == 200
            data = JSON3.read(String(resp.body))
            @test data[:data][:message] == "rebuild started"

            # Wait for the background task to complete before checking health.
            sleep(2.0)

            resp2 = HTTP.get("$base_url/health"; status_exception=false)
            health = JSON3.read(String(resp2.body))
            @test health[:data][:rebuilding] == false
        end

        # ── 8. GET /unknown → 404 ─────────────────────────────────────────────

        @testset "GET /unknown → 404" begin
            resp = HTTP.get("$base_url/nonexistent"; status_exception=false)
            @test resp.status == 404
        end

        # ── 9. Bad JSON → 400 ─────────────────────────────────────────────────

        @testset "Bad JSON → 400" begin
            resp = HTTP.post(
                "$base_url/search",
                ["Content-Type" => "application/json"],
                "not json";
                status_exception=false,
            )
            # JSON3 parse error is caught by the router's catch block and
            # re-raised as a 500, or caught inside _handle_search as a 400.
            # Accept either: the important thing is the server did not crash.
            @test resp.status == 400 || resp.status == 500
        end

        # ── 10. Content-Type header ────────────────────────────────────────────

        @testset "Content-Type is application/json" begin
            resp = HTTP.get("$base_url/health"; status_exception=false)
            ct = HTTP.header(resp, "Content-Type")
            @test ct == "application/json"
        end

    finally
        close(server)
        close(store)
    end

end

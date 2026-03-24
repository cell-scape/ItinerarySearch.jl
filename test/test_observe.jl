using Test
using ItinerarySearch
using JSON3

@testset "Observe" begin

    @testset "SystemMetricsEvent" begin
        evt = SystemMetricsEvent()
        @test evt.timestamp > UInt64(0)
        @test evt.thread_id >= 1
        @test isbitstype(SystemMetricsEvent)
    end

    @testset "PhaseEvent" begin
        evt = PhaseEvent(phase = :test, action = :start)
        @test evt.phase == :test
        @test evt.action == :start
        @test evt.elapsed_ns == UInt64(0)
    end

    @testset "BuildSnapshotEvent" begin
        evt = BuildSnapshotEvent(stations_processed = Int32(5), total_stations = Int32(10))
        @test evt.stations_processed == Int32(5)
        @test evt.stats isa BuildStats
    end

    @testset "SearchSnapshotEvent" begin
        evt = SearchSnapshotEvent(origin = StationCode("ORD"), destination = StationCode("LHR"))
        @test evt.origin == StationCode("ORD")
        @test evt.stats isa SearchStats
    end

    @testset "CustomEvent" begin
        evt = CustomEvent(name = :test, message = "hello", metadata = Dict("k" => 1))
        @test evt.name == :test
        @test evt.metadata["k"] == 1
    end

    @testset "EventLog emit! enabled/disabled" begin
        log = EventLog(enabled = false)
        emit!(log, PhaseEvent(phase = :test))
        @test isempty(log.events)  # disabled = no-op

        log.enabled = true
        emit!(log, PhaseEvent(phase = :test))
        @test length(log.events) == 1
        @test log.events[1] isa PhaseEvent
    end

    @testset "EventLog sink invocation" begin
        call_count = Ref(0)
        test_sink(event) = (call_count[] += 1; nothing)

        log = EventLog(enabled = true, sinks = Any[test_sink])
        emit!(log, PhaseEvent(phase = :test))
        @test call_count[] == 1

        emit!(log, SystemMetricsEvent())
        @test call_count[] == 2
    end

    @testset "with_phase emits start + end + checkpoint" begin
        log = EventLog(enabled = true)
        result = with_phase(log, :test_phase) do
            42
        end
        @test result == 42
        # start, end, checkpoint (SystemMetricsEvent)
        @test length(log.events) == 3
        @test log.events[1] isa PhaseEvent
        @test log.events[1].action == :start
        @test log.events[1].phase == :test_phase
        @test log.events[2] isa PhaseEvent
        @test log.events[2].action == :end
        @test log.events[2].elapsed_ns >= UInt64(0)
        @test log.events[3] isa SystemMetricsEvent
    end

    @testset "with_phase disabled still executes f()" begin
        log = EventLog(enabled = false)
        result = with_phase(log, :test_phase) do
            99
        end
        @test result == 99
        @test isempty(log.events)  # no events emitted when disabled
    end

    @testset "checkpoint! emits SystemMetricsEvent" begin
        log = EventLog(enabled = true)
        checkpoint!(log)
        @test length(log.events) == 1
        @test log.events[1] isa SystemMetricsEvent
    end

    @testset "collect_system_metrics" begin
        m = collect_system_metrics()
        @test m isa SystemMetricsEvent
        @test m.total_memory > UInt64(0)
        @test m.julia_threads >= 1
        @test m.cpu_threads >= 1
    end

    @testset "JsonlSink writes valid JSONL" begin
        buf = IOBuffer()
        sink = JsonlSink(buf)
        sink(PhaseEvent(phase = :test, action = :start))

        line = String(take!(buf))
        parsed = JSON3.read(strip(line))
        @test parsed[:type] == "PhaseEvent"
        @test haskey(parsed, :data)
        @test parsed[:data][:phase] == "test"
    end

    @testset "JsonlSink file path constructor" begin
        path = tempname() * ".jsonl"
        try
            sink = JsonlSink(path)
            sink(SystemMetricsEvent())
            flush(sink.io)
            close(sink.io)
            @test isfile(path)
            content = readline(path)
            parsed = JSON3.read(content)
            @test parsed[:type] == "SystemMetricsEvent"
        finally
            rm(path; force = true)
        end
    end

    @testset "EventLog close flushes sinks" begin
        path = tempname() * ".jsonl"
        try
            log = EventLog(enabled = true)
            push!(log.sinks, JsonlSink(path))
            emit!(log, PhaseEvent(phase = :test))
            close(log)
            @test isempty(log.sinks)
            @test isfile(path)
            @test !isempty(readline(path))
        finally
            rm(path; force = true)
        end
    end

    @testset "Integration: build_graph! with event log enabled" begin
        using DuckDB, DBInterface
        store = DuckDBStore()
        try
            DBInterface.execute(store.db, """
            INSERT INTO legs VALUES (
                1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
                'ORD', 'LHR', 540, 1320, 535, 1325,
                -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
                '2026-06-15', '2026-06-15', 127,
                'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
            )
            """)
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)",
            )
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)",
            )
            post_ingest_sql!(store)

            path = tempname() * ".jsonl"
            config = SearchConfig(event_log_enabled = true, event_log_path = path)
            graph = build_graph!(store, config, Date(2026, 6, 15))

            # JSONL file should exist with events
            @test isfile(path)
            lines = readlines(path)
            @test length(lines) >= 3  # at least: baseline checkpoint + some phases

            # Parse first line
            parsed = JSON3.read(lines[1])
            @test haskey(parsed, :type)

            # Check we got PhaseEvents and SystemMetricsEvents
            types = [JSON3.read(l)[:type] for l in lines]
            @test "PhaseEvent" in types
            @test "SystemMetricsEvent" in types

            rm(path; force = true)
        finally
            close(store)
        end
    end

    @testset "build_graph! with event log disabled (default)" begin
        using DuckDB, DBInterface
        store = DuckDBStore()
        try
            DBInterface.execute(store.db, """
            INSERT INTO legs VALUES (
                1, 1, 'UA', 1234, ' ', 1, ' ', 1, 'J',
                'ORD', 'LHR', 540, 1320, 535, 1325,
                -300, 0, 0, 0, '1', '2', '789', 'W', 'UA',
                '2026-06-15', '2026-06-15', 127,
                'D', 'I', '', ' ', 'JCDZPY', 3941.0, false
            )
            """)
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('ORD','US','IL','CHI','NOA',41.9742,-87.9073,-300)",
            )
            DBInterface.execute(
                store.db,
                "INSERT INTO stations VALUES ('LHR','GB','','LON','EUR',51.4700,-0.4543,0)",
            )
            post_ingest_sql!(store)

            config = SearchConfig()  # default: event_log_enabled = false
            graph = build_graph!(store, config, Date(2026, 6, 15))

            # Should still build successfully
            @test graph.build_stats.total_stations > 0
        finally
            close(store)
        end
    end

end

# src/types/graph.jl — Subsystem 2 stubs
# These types are defined here so Subsystem 1 can reference them in interfaces,
# but are not implemented until Subsystem 2.

"""
    mutable struct GraphStation

Station node in the flight network graph. Holds departure/arrival segment
lists and connection references. Implemented in Subsystem 2.
"""
mutable struct GraphStation
    code::StationCode
end

"""
    mutable struct GraphSegment

Segment node in the flight network graph. Wraps a SegmentRecord with
its constituent legs and graph connectivity. Implemented in Subsystem 2.
"""
mutable struct GraphSegment
    segment::SegmentRecord
end

"""
    mutable struct GraphConnection

Connection between two segments at a station. Contains MCT, connection time,
and status bits. Implemented in Subsystem 2.
"""
mutable struct GraphConnection
    station::GraphStation
end

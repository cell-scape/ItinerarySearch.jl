using Test
using ItinerarySearch
using ItinerarySearch: open_maybe_compressed
using CodecZstd, CodecZlib, CodecBzip2, CodecXz, TranscodingStreams

@testset "Compression Detection" begin
    test_content = "3UA 1234 01 01J01JAN2631DEC261234567 ORD0900090000-0500 1 LHR" * " "^137

    @testset "Plain text" begin
        path = tempname()
        write(path, test_content)
        io = open_maybe_compressed(path)
        @test readline(io) == test_content
        close(io)
        rm(path)
    end

    @testset "Gzip" begin
        path = tempname() * ".gz"
        open(GzipCompressorStream, path, "w") do io
            write(io, test_content)
        end
        io = open_maybe_compressed(path)
        @test readline(io) == test_content
        close(io)
        rm(path)
    end

    @testset "Zstd" begin
        path = tempname() * ".zst"
        open(ZstdCompressorStream, path, "w") do io
            write(io, test_content)
        end
        io = open_maybe_compressed(path)
        @test readline(io) == test_content
        close(io)
        rm(path)
    end

    @testset "Bzip2" begin
        path = tempname() * ".bz2"
        open(Bzip2CompressorStream, path, "w") do io
            write(io, test_content)
        end
        io = open_maybe_compressed(path)
        @test readline(io) == test_content
        close(io)
        rm(path)
    end

    @testset "Xz" begin
        path = tempname() * ".xz"
        open(XzCompressorStream, path, "w") do io
            write(io, test_content)
        end
        io = open_maybe_compressed(path)
        @test readline(io) == test_content
        close(io)
        rm(path)
    end
end

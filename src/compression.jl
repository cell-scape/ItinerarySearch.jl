# src/compression.jl — Magic-number detection, transparent decompression

using TranscodingStreams
using CodecZstd
using CodecZlib
using CodecBzip2
using CodecXz

"""
    `open_maybe_compressed(path::String)::IO`
---

# Description
- Open a file with transparent decompression
- Detection is by magic number, never by file extension
- Supported formats: gzip, zstd, bzip2, xz, plain text

# Arguments
1. `path::String`: path to the file to open

# Returns
- `::IO`: an IO stream that reads decompressed content

# Examples
```julia
julia> io = open_maybe_compressed("data/schedule.dat.gz");
julia> readline(io)
```
"""
function open_maybe_compressed(path::String)::IO
    magic = open(io -> read(io, 6), path)
    len = length(magic)

    if len >= 2 && magic[1:2] == UInt8[0x1f, 0x8b]
        # gzip
        GzipDecompressorStream(open(path))
    elseif len >= 4 && magic[1:4] == UInt8[0x28, 0xb5, 0x2f, 0xfd]
        # zstd
        ZstdDecompressorStream(open(path))
    elseif len >= 6 && magic[1:6] == UInt8[0xfd, 0x37, 0x7a, 0x58, 0x5a, 0x00]
        # xz
        XzDecompressorStream(open(path))
    elseif len >= 3 && magic[1:3] == UInt8[0x42, 0x5a, 0x68]  # "BZh"
        # bzip2
        Bzip2DecompressorStream(open(path))
    else
        # plain text
        open(path)
    end
end

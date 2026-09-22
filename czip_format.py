import struct

MAGIC = b"CZIP0001"

# Header: magic (8 bytes), chunk size (4 bytes), chunk count (8 bytes)
HEADER_FORMAT = ">8sIQ"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

# Chunk record: chunk ID, original size, compressed size
CHUNK_FORMAT = ">QQQ"
CHUNK_RECORD_SIZE = struct.calcsize(CHUNK_FORMAT)


def create_header(chunk_size, chunk_count):
    return struct.pack(
        HEADER_FORMAT,
        MAGIC,
        chunk_size,
        chunk_count
    )


def read_header(file):
    data = file.read(HEADER_SIZE)

    if len(data) != HEADER_SIZE:
        raise ValueError("Invalid or incomplete C-ZIP header.")

    magic, chunk_size, chunk_count = struct.unpack(
        HEADER_FORMAT,
        data
    )

    if magic != MAGIC:
        raise ValueError("This is not a valid C-ZIP file.")

    if chunk_size == 0:
        raise ValueError("Invalid chunk size in C-ZIP header.")

    return chunk_size, chunk_count
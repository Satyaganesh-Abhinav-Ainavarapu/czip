import zlib
from dataclasses import dataclass


@dataclass
class CompressedChunk:
    chunk_id: int
    compressed_data: bytes
    original_size: int


class CompressionEngine:
    """
    Compresses individual chunks using the
    DEFLATE algorithm through Python's zlib.
    """

    def compress(self, chunk):

        compressed_data = zlib.compress(
            chunk.data,
            level=6
        )

        return CompressedChunk(
            chunk_id=chunk.chunk_id,
            compressed_data=compressed_data,
            original_size=len(chunk.data)
        )
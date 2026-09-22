import zlib
from dataclasses import dataclass


@dataclass
class CompressedChunk:
    chunk_id: int
    compressed_data: bytes
    original_size: int


class CompressionEngine:

    def __init__(self, level=6):
        if not 0 <= level <= 9:
            raise ValueError("Compression level must be 0–9.")

        self.level = level

    def compress(self, chunk):
        compressed_data = zlib.compress(
            chunk.data,
            level=self.level
        )

        return CompressedChunk(
            chunk_id=chunk.chunk_id,
            compressed_data=compressed_data,
            original_size=len(chunk.data)
        )

    @staticmethod
    def decompress(compressed_data):
        return zlib.decompress(compressed_data)
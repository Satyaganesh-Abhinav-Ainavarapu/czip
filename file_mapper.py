import mmap
import os
from dataclasses import dataclass


@dataclass
class Chunk:
    chunk_id: int
    data: bytes


class FileMapper:

    def __init__(self, filename, chunk_size=1024 * 1024):
        if chunk_size <= 0:
            raise ValueError("Chunk size must be positive.")

        self.filename = filename
        self.chunk_size = chunk_size

    def get_file_size(self):
        return os.path.getsize(self.filename)

    def get_chunk_count(self):
        file_size = self.get_file_size()

        if file_size == 0:
            return 0

        return (
            file_size + self.chunk_size - 1
        ) // self.chunk_size

    def create_chunks(self):
        file_size = self.get_file_size()

        if file_size == 0:
            return

        with open(self.filename, "rb") as file:
            with mmap.mmap(
                file.fileno(),
                length=0,
                access=mmap.ACCESS_READ
            ) as mapped_file:

                chunk_id = 0
                offset = 0

                while offset < file_size:
                    end = min(
                        offset + self.chunk_size,
                        file_size
                    )

                    yield Chunk(
                        chunk_id=chunk_id,
                        data=mapped_file[offset:end]
                    )

                    chunk_id += 1
                    offset = end
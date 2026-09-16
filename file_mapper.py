import mmap
from dataclasses import dataclass


@dataclass
class Chunk:
    chunk_id: int
    data: bytes


class FileMapper:
    

    def __init__(self, filename, chunk_size=1024):
        self.filename = filename
        self.chunk_size = chunk_size

    def create_chunks(self):
        chunks = []

        with open(self.filename, "rb") as file:
            with mmap.mmap(
                file.fileno(),
                length=0,
                access=mmap.ACCESS_READ
            ) as mapped_file:

                file_size = mapped_file.size()

                chunk_id = 0
                offset = 0

                while offset < file_size:

                    end = min(
                        offset + self.chunk_size,
                        file_size
                    )

                    chunk = Chunk(
                        chunk_id=chunk_id,
                        data=mapped_file[offset:end]
                    )

                    chunks.append(chunk)

                    chunk_id += 1
                    offset = end

        return chunks
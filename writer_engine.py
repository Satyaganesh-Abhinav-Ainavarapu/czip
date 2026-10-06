import os
import tempfile

from czip_format import (
    create_header,
    create_content_section,
    create_chunk_record,
    CHUNKS_START,
    CHUNKS_END,
)


class WriterEngine:
    """
    Writes a human-readable C-ZIP archive.

    The archive contains:
    - C-ZIP metadata
    - Original file contents for demonstration
    - Actual zlib-compressed chunk data
    """

    def __init__(self, output_file):
        self.output_file = output_file

    def write(
        self,
        completion_buffer,
        chunk_count,
        chunk_size,
        original_filename,
        original_data
    ):
        output_dir = os.path.dirname(
            os.path.abspath(self.output_file)
        )

        temp_path = None

        try:
            with tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                dir=output_dir,
                prefix=".czip_tmp_",
                delete=False
            ) as output:

                temp_path = output.name

                output.write(
                    create_header(
                        original_filename,
                        len(original_data),
                        chunk_size,
                        chunk_count
                    )
                )

                output.write(
                    create_content_section(
                        original_data
                    )
                )

                output.write(
                    f"{CHUNKS_START}\n"
                )

                for expected_id in range(chunk_count):

                    chunk = completion_buffer.get_next(
                        expected_id
                    )

                    if chunk.chunk_id != expected_id:
                        raise ValueError(
                            f"Unexpected chunk ID: "
                            f"expected {expected_id}, "
                            f"got {chunk.chunk_id}"
                        )

                    output.write(
                        create_chunk_record(
                            chunk.chunk_id,
                            chunk.original_size,
                            chunk.compressed_data
                        )
                    )

                output.write(
                    f"{CHUNKS_END}\n"
                )

                output.flush()
                os.fsync(output.fileno())

            os.replace(
                temp_path,
                self.output_file
            )

            temp_path = None

        finally:
            if (
                temp_path is not None
                and os.path.exists(temp_path)
            ):
                os.remove(temp_path)

import os
import struct
import tempfile

from czip_format import create_header, CHUNK_FORMAT


class WriterEngine:
    """
    Writes compressed chunks to a C-ZIP archive
    in their original chunk_id order.
    """

    def __init__(self, output_file):
        self.output_file = output_file

    def write(
        self,
        completion_buffer,
        chunk_count,
        chunk_size
    ):
        output_dir = os.path.dirname(
            os.path.abspath(self.output_file)
        )

        temp_path = None

        try:
            # Create a temporary file in the output directory.
            # The final output is published only after writing
            # completes successfully.
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=output_dir,
                prefix=".czip_tmp_",
                delete=False
            ) as output:

                temp_path = output.name

                # Write the C-ZIP file header.
                output.write(
                    create_header(
                        chunk_size,
                        chunk_count
                    )
                )

                # Write chunks strictly in chunk_id order.
                for expected_id in range(chunk_count):
                    chunk = completion_buffer.get_next(
                        expected_id
                    )

                    if chunk.chunk_id != expected_id:
                        raise ValueError(
                            "Unexpected chunk ID: "
                            f"expected {expected_id}, "
                            f"got {chunk.chunk_id}"
                        )

                    # Write chunk metadata:
                    # chunk_id, original_size, compressed_size
                    output.write(
                        struct.pack(
                            CHUNK_FORMAT,
                            chunk.chunk_id,
                            chunk.original_size,
                            len(chunk.compressed_data)
                        )
                    )

                    # Write the compressed chunk data.
                    output.write(
                        chunk.compressed_data
                    )

                # Ensure buffered data is passed to the OS.
                output.flush()
                os.fsync(output.fileno())

            # Publish the completed archive.
            os.replace(
                temp_path,
                self.output_file
            )

            temp_path = None

        finally:
            # Remove the temporary file if writing failed.
            if (
                temp_path is not None
                and os.path.exists(temp_path)
            ):
                os.remove(temp_path)
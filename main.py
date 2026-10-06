import argparse
import base64
import os
import threading
import zlib

from file_mapper import FileMapper
from work_queue import WorkQueue
from compression_engine import CompressionEngine
from completion_buffer import CompletionBuffer
from writer_engine import WriterEngine

from czip_format import (
    MAGIC,
    HEADER_END,
    CONTENT_START,
    CONTENT_END,
    CHUNKS_START,
    CHUNKS_END,
    decode_compressed_data,
)


def worker(
    work_queue,
    completion_buffer,
    compression_engine,
    worker_id
):
    try:
        while True:

            chunk = work_queue.get()

            try:
                if chunk is None:
                    return

                compressed = compression_engine.compress(
                    chunk
                )

                completion_buffer.put(
                    compressed
                )

                print(
                    f"Worker {worker_id}: "
                    f"Compressed chunk {chunk.chunk_id}"
                )

            except Exception as e:
                completion_buffer.report_error(e)

            finally:
                work_queue.task_done()

    finally:
        completion_buffer.worker_finished()


def compress_file(
    input_file,
    output_file,
    chunk_size,
    worker_count,
    queue_size
):

    if os.path.abspath(input_file) == os.path.abspath(
        output_file
    ):
        raise ValueError(
            "Input and output files must be different"
        )

    if worker_count <= 0:
        raise ValueError(
            "Worker count must be greater than zero"
        )

    if os.path.exists(output_file):
        raise FileExistsError(
            f"Output file already exists: {output_file}"
        )

    # Read original file for the readable content section
    with open(input_file, "rb") as f:
        original_data = f.read()

    file_mapper = FileMapper(
        input_file,
        chunk_size
    )

    chunk_count = file_mapper.get_chunk_count()

    work_queue = WorkQueue(
        queue_size
    )

    completion_buffer = CompletionBuffer()

    compression_engine = CompressionEngine()

    workers = []

    for i in range(worker_count):

        thread = threading.Thread(
            target=worker,
            args=(
                work_queue,
                completion_buffer,
                compression_engine,
                i + 1
            )
        )

        thread.start()
        workers.append(thread)

    try:

        # Producer
        for chunk in file_mapper.create_chunks():
            work_queue.put(chunk)

        # One sentinel per worker
        for _ in range(worker_count):
            work_queue.put(None)

        # Writer
        writer = WriterEngine(
            output_file
        )

        writer.write(
            completion_buffer,
            chunk_count,
            chunk_size,
            os.path.basename(input_file),
            original_data
        )

        work_queue.join()

        for thread in workers:
            thread.join()

        print()
        print(
            f"Compression complete: {output_file}"
        )
        print(
            f"Original size: {len(original_data)} bytes"
        )
        print(
            f"Chunks: {chunk_count}"
        )

    except Exception:
        raise


def parse_archive(path):

    with open(
        path,
        "r",
        encoding="utf-8"
    ) as f:

        lines = f.readlines()

    if not lines:
        raise ValueError(
            "Empty C-ZIP archive"
        )

    if lines[0].strip() != MAGIC:
        raise ValueError(
            "Invalid C-ZIP file"
        )

    filename = None
    original_size = None
    chunk_size = None
    chunk_count = None

    index = 1

    # -------------------------
    # HEADER
    # -------------------------

    while index < len(lines):

        line = lines[index].rstrip("\n")

        if line == HEADER_END:
            index += 1
            break

        if line.startswith("FILE: "):
            filename = line[6:]

        elif line.startswith("ORIGINAL SIZE: "):
            original_size = int(
                line[len("ORIGINAL SIZE: "):]
            )

        elif line.startswith("CHUNK SIZE: "):
            chunk_size = int(
                line[len("CHUNK SIZE: "):]
            )

        elif line.startswith("CHUNKS: "):
            chunk_count = int(
                line[len("CHUNKS: "):]
            )

        index += 1

    if (
        filename is None
        or original_size is None
        or chunk_size is None
        or chunk_count is None
    ):
        raise ValueError(
            "Incomplete C-ZIP header"
        )

    # -------------------------
    # ORIGINAL CONTENT
    # -------------------------

    while index < len(lines):

        if lines[index].rstrip("\n") == CONTENT_START:
            index += 1
            break

        index += 1

    original_lines = []

    while index < len(lines):

        line = lines[index].rstrip("\n")

        if line == CONTENT_END:
            index += 1
            break

        original_lines.append(
            line
        )

        index += 1

    # -------------------------
    # COMPRESSED CHUNKS
    # -------------------------

    while index < len(lines):

        if lines[index].rstrip("\n") == CHUNKS_START:
            index += 1
            break

        index += 1

    chunks = {}

    while index < len(lines):

        line = lines[index].rstrip("\n")

        if line == CHUNKS_END:
            break

        if line.startswith("CHUNK "):

            chunk_id = int(
                line[len("CHUNK "):]
            )

            original_line = lines[index + 1].rstrip("\n")
            compressed_line = lines[index + 2].rstrip("\n")
            data_line = lines[index + 3].rstrip("\n")

            original_chunk_size = int(
                original_line[
                    len("ORIGINAL SIZE: "):
                ]
            )

            compressed_size = int(
                compressed_line[
                    len("COMPRESSED SIZE: "):
                ]
            )

            encoded_data = data_line[
                len("DATA: "):
            ]

            compressed_data = (
                decode_compressed_data(
                    encoded_data
                )
            )

            if len(compressed_data) != compressed_size:
                raise ValueError(
                    f"Compressed size mismatch "
                    f"for chunk {chunk_id}"
                )

            chunks[chunk_id] = (
                original_chunk_size,
                compressed_data
            )

            index += 4

        else:
            index += 1

    return (
        filename,
        original_size,
        chunk_size,
        chunk_count,
        original_lines,
        chunks
    )


def decompress_file(
    input_file,
    output_file
):

    (
        filename,
        original_size,
        chunk_size,
        chunk_count,
        original_lines,
        chunks
    ) = parse_archive(input_file)

    if os.path.exists(output_file):
        raise FileExistsError(
            f"Output file already exists: {output_file}"
        )

    if len(chunks) != chunk_count:
        raise ValueError(
            "Chunk count mismatch"
        )

    temp_file = output_file + ".tmp"

    try:

        with open(
            temp_file,
            "wb"
        ) as output:

            for chunk_id in range(chunk_count):

                if chunk_id not in chunks:
                    raise ValueError(
                        f"Missing chunk {chunk_id}"
                    )

                expected_size, compressed_data = (
                    chunks[chunk_id]
                )

                data = zlib.decompress(
                    compressed_data
                )

                if len(data) != expected_size:
                    raise ValueError(
                        f"Size mismatch for "
                        f"chunk {chunk_id}"
                    )

                output.write(data)

        if os.path.getsize(temp_file) != original_size:
            raise ValueError(
                "Final file size does not match "
                "original size"
            )

        os.replace(
            temp_file,
            output_file
        )

        print(
            f"Decompression complete: {output_file}"
        )

    finally:

        if os.path.exists(temp_file):
            os.remove(temp_file)


def main():

    parser = argparse.ArgumentParser(
        description="ConcurrentZip (C-ZIP)"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    compress_parser = subparsers.add_parser(
        "compress"
    )

    compress_parser.add_argument(
        "input"
    )

    compress_parser.add_argument(
        "output"
    )

    compress_parser.add_argument(
        "--chunk-size",
        type=int,
        default=64 * 1024
    )

    compress_parser.add_argument(
        "--workers",
        type=int,
        default=4
    )

    compress_parser.add_argument(
        "--queue-size",
        type=int,
        default=8
    )

    decompress_parser = subparsers.add_parser(
        "decompress"
    )

    decompress_parser.add_argument(
        "input"
    )

    decompress_parser.add_argument(
        "output"
    )

    args = parser.parse_args()

    try:

        if args.command == "compress":

            compress_file(
                args.input,
                args.output,
                args.chunk_size,
                args.workers,
                args.queue_size
            )

        elif args.command == "decompress":

            decompress_file(
                args.input,
                args.output
            )

    except Exception as e:

        print(
            f"Error: {e}"
        )


if __name__ == "__main__":
    main()
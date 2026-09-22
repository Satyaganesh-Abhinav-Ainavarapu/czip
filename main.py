import argparse
import os
import queue
import struct
import threading
import zlib

from file_mapper import FileMapper
from work_queue import WorkQueue
from compression_engine import CompressionEngine
from completion_buffer import CompletionBuffer
from writer_engine import WriterEngine
from czip_format import (
    read_header,
    CHUNK_FORMAT,
    CHUNK_RECORD_SIZE
)


STOP = None


def worker(
    worker_id,
    work_queue,
    compressor,
    completion_buffer
):
    while True:
        chunk = work_queue.get()

        try:
            if chunk is STOP:
                return

            compressed_chunk = compressor.compress(chunk)

            completion_buffer.put(compressed_chunk)

            print(
                f"Worker {worker_id}: "
                f"Compressed Chunk {chunk.chunk_id}"
            )

        except Exception as error:
            completion_buffer.report_error(error)

        finally:
            work_queue.task_done()

    completion_buffer.worker_finished()


def run_worker(
    worker_id,
    work_queue,
    compressor,
    completion_buffer
):
    try:
        worker(
            worker_id,
            work_queue,
            compressor,
            completion_buffer
        )
    except Exception as error:
        completion_buffer.report_error(error)
    finally:
        completion_buffer.worker_finished()


def compress_file(
    input_file,
    output_file,
    chunk_size=1024 * 1024,
    worker_count=3,
    queue_size=4
):
    if os.path.abspath(input_file) == os.path.abspath(output_file):
        raise ValueError("Input and output files must be different.")

    if worker_count <= 0:
        raise ValueError("Worker count must be positive.")

    if os.path.exists(output_file):
        raise FileExistsError(
            f"Output file already exists: {output_file}"
        )

    mapper = FileMapper(input_file, chunk_size)
    chunk_count = mapper.get_chunk_count()

    work_queue = WorkQueue(queue_size)
    completion_buffer = CompletionBuffer()
    compressor = CompressionEngine()

    workers = []

    for worker_id in range(worker_count):
        thread = threading.Thread(
            target=run_worker,
            args=(
                worker_id + 1,
                work_queue,
                compressor,
                completion_buffer
            ),
            daemon=True
        )

        thread.start()
        workers.append(thread)

    print(f"Input file: {input_file}")
    print(f"Chunks: {chunk_count}")
    print(f"Workers: {worker_count}")

    writer = WriterEngine(output_file)

    try:
        # Producer: generate chunks and enqueue them
        for chunk in mapper.create_chunks():
            work_queue.put(chunk)

        # Send one termination marker to each worker
        for _ in workers:
            work_queue.put(STOP)

        # Writer consumes completed chunks in order
        writer.write(
            completion_buffer,
            chunk_count,
            chunk_size
        )

        work_queue.join()

        for thread in workers:
            thread.join()

        print(f"\nCompression complete: {output_file}")

    except KeyboardInterrupt:
        print("\nInterrupted. Waiting for workers to stop...")
        raise


def decompress_file(input_file, output_file):
    if os.path.abspath(input_file) == os.path.abspath(output_file):
        raise ValueError("Input and output files must be different.")

    if os.path.exists(output_file):
        raise FileExistsError(
            f"Output file already exists: {output_file}"
        )

    with open(input_file, "rb") as source:
        chunk_size, chunk_count = read_header(source)

        temp_path = output_file + ".tmp"

        try:
            with open(temp_path, "wb") as destination:
                for expected_id in range(chunk_count):
                    record = source.read(CHUNK_RECORD_SIZE)

                    if len(record) != CHUNK_RECORD_SIZE:
                        raise ValueError(
                            "Incomplete chunk record."
                        )

                    chunk_id, original_size, compressed_size = (
                        struct.unpack(CHUNK_FORMAT, record)
                    )

                    if chunk_id != expected_id:
                        raise ValueError(
                            "Invalid chunk ordering in archive."
                        )

                    compressed_data = source.read(
                        compressed_size
                    )

                    if len(compressed_data) != compressed_size:
                        raise ValueError(
                            "Incomplete compressed chunk."
                        )

                    data = zlib.decompress(compressed_data)

                    if len(data) != original_size:
                        raise ValueError(
                            "Decompressed chunk size mismatch."
                        )

                    destination.write(data)

                    print(
                        f"Decompressed Chunk {chunk_id}"
                    )

                if source.read(1):
                    raise ValueError(
                        "Unexpected trailing data in archive."
                    )

            os.replace(temp_path, output_file)

        except Exception:
            if os.path.exists(temp_path):
                os.remove(temp_path)

            raise

    print(f"\nDecompression complete: {output_file}")


def main():
    parser = argparse.ArgumentParser(
        description="C-ZIP concurrent file compression utility"
    )

    subparsers = parser.add_subparsers(
        dest="command",
        required=True
    )

    compress_parser = subparsers.add_parser(
        "compress",
        help="Compress a file"
    )

    compress_parser.add_argument("input")
    compress_parser.add_argument("output")

    compress_parser.add_argument(
        "--chunk-size",
        type=int,
        default=1024 * 1024
    )

    compress_parser.add_argument(
        "--workers",
        type=int,
        default=3
    )

    compress_parser.add_argument(
        "--queue-size",
        type=int,
        default=4
    )

    decompress_parser = subparsers.add_parser(
        "decompress",
        help="Decompress a C-ZIP file"
    )

    decompress_parser.add_argument("input")
    decompress_parser.add_argument("output")

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

    except (OSError, ValueError, RuntimeError, zlib.error) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
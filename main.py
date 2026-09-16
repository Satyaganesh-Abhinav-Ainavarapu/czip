import threading

from file_mapper import FileMapper
from work_queue import WorkQueue
from compression_engine import CompressionEngine


def worker(worker_id, work_queue, compressor, results):
    

    while True:

        chunk = work_queue.get()

        # None is used as a termination signal
        if chunk is None:
            work_queue.task_done()
            break

        compressed_chunk = compressor.compress(chunk)

        results.append(compressed_chunk)

        print(
            f"Worker {worker_id}: "
            f"Compressed Chunk {chunk.chunk_id}"
        )

        work_queue.task_done()


def main():

    input_file = "sample.txt"

    print("========== C-ZIP PYTHON PROTOTYPE ==========")

    # --------------------------------------------------
    # 1. FILE MAPPING
    # --------------------------------------------------

    mapper = FileMapper(
        input_file,
        chunk_size=1024
    )

    chunks = mapper.create_chunks()

    print("\n[FileMapper]")
    print(f"Input file: {input_file}")
    print(f"Chunks created: {len(chunks)}")

    # --------------------------------------------------
    # 2. WORK QUEUE
    # --------------------------------------------------

    work_queue = WorkQueue(max_size=4)

    print("\n[WorkQueue]")
    print("Bounded queue created with capacity 4.")

    # --------------------------------------------------
    # 3. COMPRESSION ENGINE
    # --------------------------------------------------

    compressor = CompressionEngine()

    results = []

    # --------------------------------------------------
    # 4. WORKER THREADS
    # --------------------------------------------------

    workers = []

    number_of_workers = 3

    for worker_id in range(number_of_workers):

        thread = threading.Thread(
            target=worker,
            args=(
                worker_id + 1,
                work_queue,
                compressor,
                results
            )
        )

        thread.start()
        workers.append(thread)

    # --------------------------------------------------
    # 5. PRODUCER
    # --------------------------------------------------

    print("\n[Producer]")

    for chunk in chunks:
        work_queue.put(chunk)

        print(
            f"Added Chunk {chunk.chunk_id} "
            f"to WorkQueue"
        )

    # --------------------------------------------------
    # 6. TERMINATION SIGNALS
    # --------------------------------------------------

    for _ in range(number_of_workers):
        work_queue.put(None)

    # Wait until every queued item is processed
    work_queue.join()

    # Wait for all workers to terminate
    for thread in workers:
        thread.join()

    # --------------------------------------------------
    # 7. DISPLAY RESULTS
    # --------------------------------------------------

    results.sort(key=lambda chunk: chunk.chunk_id)

    print("\n[Compression Results]")

    for chunk in results:

        print(
            f"Chunk {chunk.chunk_id}: "
            f"{chunk.original_size} bytes -> "
            f"{len(chunk.compressed_data)} bytes"
        )

    print("\n========== PROTOTYPE COMPLETE ==========")


if __name__ == "__main__":
    main()
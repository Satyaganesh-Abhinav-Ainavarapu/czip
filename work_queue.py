import queue


class WorkQueue:
    """
    Bounded thread-safe queue used to transfer
    chunks from the producer to worker threads.
    """

    def __init__(self, max_size=4):
        self.queue = queue.Queue(maxsize=max_size)

    def put(self, chunk):
        """
        Add a chunk to the queue.
        Blocks when the queue is full.
        """
        self.queue.put(chunk)

    def get(self):
        """
        Retrieve a chunk from the queue.
        Blocks when the queue is empty.
        """
        return self.queue.get()

    def task_done(self):
        """Mark a chunk as processed."""
        self.queue.task_done()

    def join(self):
        """Wait until all queued chunks are processed."""
        self.queue.join()

    def empty(self):
        """Return True if the queue is empty."""
        return self.queue.empty()
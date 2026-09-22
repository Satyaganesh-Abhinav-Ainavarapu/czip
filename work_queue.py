import queue


class WorkQueue:

    def __init__(self, max_size=4):
        if max_size <= 0:
            raise ValueError("Queue size must be positive.")

        self.queue = queue.Queue(maxsize=max_size)

    def put(self, item):
        self.queue.put(item)

    def get(self):
        return self.queue.get()

    def task_done(self):
        self.queue.task_done()

    def join(self):
        self.queue.join()
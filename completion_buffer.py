import threading


class CompletionBuffer:

    def __init__(self):
        self.condition = threading.Condition()
        self.completed = {}
        self.error = None
        self.workers_finished = 0

    def put(self, chunk):
        with self.condition:
            self.completed[chunk.chunk_id] = chunk
            self.condition.notify_all()

    def get_next(self, expected_id):
        with self.condition:
            while (
                expected_id not in self.completed
                and self.error is None
            ):
                self.condition.wait()

            if self.error is not None:
                raise RuntimeError(
                    "A worker failed."
                ) from self.error

            return self.completed.pop(expected_id)

    def report_error(self, error):
        with self.condition:
            if self.error is None:
                self.error = error

            self.condition.notify_all()

    def worker_finished(self):
        with self.condition:
            self.workers_finished += 1
            self.condition.notify_all()
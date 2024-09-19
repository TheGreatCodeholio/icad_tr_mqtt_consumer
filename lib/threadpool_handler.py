from concurrent.futures import ThreadPoolExecutor
import threading

class TrackingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super(TrackingThreadPoolExecutor, self).__init__(*args, **kwargs)
        self._lock = threading.Lock()
        self._pending_tasks = 0

    def submit(self, fn, *args, **kwargs):
        with self._lock:
            self._pending_tasks += 1
        future = super(TrackingThreadPoolExecutor, self).submit(fn, *args, **kwargs)
        future.add_done_callback(self._task_done)
        return future

    def _task_done(self, future):
        with self._lock:
            self._pending_tasks -= 1

    def get_pending_tasks(self):
        with self._lock:
            return self._pending_tasks

    def get_running_threads(self):
        with self._lock:
            return len(self._threads)
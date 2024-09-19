from concurrent.futures import ThreadPoolExecutor
import threading

class TrackingThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super(TrackingThreadPoolExecutor, self).__init__(*args, **kwargs)
        self._lock = threading.Lock()
        self._pending_tasks = 0
        self._running_tasks = 0

    def submit(self, fn, *args, **kwargs):
        with self._lock:
            self._pending_tasks += 1

        def wrapper_fn(*args, **kwargs):
            with self._lock:
                self._running_tasks += 1
            try:
                return fn(*args, **kwargs)
            finally:
                with self._lock:
                    self._running_tasks -= 1
                    self._pending_tasks -= 1

        future = super(TrackingThreadPoolExecutor, self).submit(wrapper_fn, *args, **kwargs)
        return future


    def _task_done(self):
        with self._lock:
            self._pending_tasks -= 1

    def get_pending_tasks(self):
        with self._lock:
            return self._pending_tasks

    def get_running_tasks(self):
        with self._lock:
            return self._running_tasks
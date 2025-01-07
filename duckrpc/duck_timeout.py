from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import logging
import time
import heapq

TIMEOUT_INTERVAL_DEFAULT = 60


@dataclass(order=True)
class TimeoutItem(object):
    timeout_at: int
    value: str


class TimeoutHandler(ABC):

    @abstractmethod
    def handle_timeout_item(self, item: TimeoutItem):
        raise NotImplementedError()


class DuckTimeout(object):
    name: str
    executor: ThreadPoolExecutor
    timeout_interval: int
    handler: TimeoutHandler
    logger: logging.Logger
    _shutdown_flag: bool = False
    _heapq: list[TimeoutItem] = []

    def __init__(self,
                 name="",
                 timeout_interval=TIMEOUT_INTERVAL_DEFAULT,
                 handler: TimeoutHandler = TimeoutHandler(),
                 logger=None):
        self.name = name
        self.executor = ThreadPoolExecutor(thread_name_prefix=f"{name}-timeout-", max_workers=1)
        self.timeout_interval = timeout_interval
        self.handler = handler
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.executor.submit(self._timeout_loop)

    def _timeout_loop(self):
        while not self._shutdown_flag:
            start = int(time.time())
            while self._heapq and heapq.nsmallest(1, self._heapq)[0].timeout_at > start:
                item = heapq.heappop(self._heapq)
                self.logger.debug(f"handle timeout item {item}")
                self.handler.handle_timeout_item(item)
            cost = int(time.time()) - start
            sleep = max(0, self.timeout_interval - cost)
            if sleep > 0:
                time.sleep(sleep)

    def add_item(self, value: any, timeout: int):
        timeout_at = int(time.time()) + timeout
        item = TimeoutItem(timeout_at=timeout_at, value=value)
        heapq.heappush(self._heapq, item)
        self.logger.debug(f"heappush {item}")

    def shutdown(self):
        self._shutdown_flag = True
        self.executor.shutdown()

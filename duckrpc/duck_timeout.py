from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import logging
import time
import heapq
import os

TIMEOUT_INTERVAL_DEFAULT = 60
DISPATCH_THREAD_POOL_SIZE = min(32, (os.cpu_count() or 1) + 4)


@dataclass(order=True)
class TimeoutItem(object):
    timeout_at: int
    value: any


class TimeoutHandler(ABC):

    @abstractmethod
    def handle_timeout(self, item: TimeoutItem):
        raise NotImplementedError()


class DuckTimeoutMgr(object):
    name: str
    timeout_interval: int
    handler: TimeoutHandler
    logger: logging.Logger
    _loop_executor: ThreadPoolExecutor
    _dispatch_executor: ThreadPoolExecutor
    _shutdown_flag: bool = False
    _heapq: list[TimeoutItem] = []

    def __init__(self,
                 name="",
                 timeout_interval=TIMEOUT_INTERVAL_DEFAULT,
                 dispatch_thread_size=DISPATCH_THREAD_POOL_SIZE,
                 handler: TimeoutHandler = None,
                 logger=None):
        self.name = name
        self.timeout_interval = timeout_interval
        self.handler = handler
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._loop_executor = ThreadPoolExecutor(thread_name_prefix=f"{name}-timeout",
                                                 max_workers=1)
        self._dispatch_executor = ThreadPoolExecutor(thread_name_prefix=f"{name}-timeout-dispatch",
                                                     max_workers=dispatch_thread_size)
        self._loop_executor.submit(self._timeout_loop)

    def _timeout_loop(self):
        while not self._shutdown_flag:
            self.logger.debug(f"timeout begin.")
            start = int(time.time())
            while len(self._heapq) > 0:
                item: TimeoutItem = heapq.heappop(self._heapq)
                if item.timeout_at < start:
                    heapq.heappush(self._heapq, item)
                    break
                self.logger.debug(f"handle timeout item {item}")
                self.handler.handle_timeout(item)

            cost = int(time.time()) - start
            sleep = max(0, self.timeout_interval - cost)
            if sleep > 0:
                time.sleep(sleep)

    def add_item(self, value: any, timeout):
        timeout_at = time.time() + timeout
        item = TimeoutItem(timeout_at=timeout_at, value=value)
        heapq.heappush(self._heapq, item)
        self.logger.debug(f"heappush {item}")

    def shutdown(self):
        self.logger.debug("shutdown start")
        self._shutdown_flag = True
        self.logger.debug("shutdown _loop_executor")
        self._loop_executor.shutdown()
        self.logger.debug("shutdown _dispatch_executor")
        self._dispatch_executor.shutdown()
        self.logger.debug("shutdown end")

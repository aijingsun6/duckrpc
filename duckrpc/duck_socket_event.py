import selectors
import os
import logging
import threading
import queue
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
from typing import Optional
from _typeshed import FileDescriptorLike

DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


class DuckSocketEventHandler(ABC):

    @abstractmethod
    def handle_event(self, fileobj: FileDescriptorLike, mask: int) -> None:
        raise NotImplementedError()


class DuckSocketEventDispatch(object):
    select_timeout: Optional[int]
    logger: logging.Logger
    select_executor: ThreadPoolExecutor

    dispatch_thread_size: int
    dispatch_executor: ThreadPoolExecutor

    _start_flag: bool
    _shutdown_flag: bool
    _lock: threading.Lock
    _selector: selectors.DefaultSelector

    def __init__(self, select_timeout=None, logger=None, dispatch_thread_size=None):
        self.select_timeout = select_timeout
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.select_executor = ThreadPoolExecutor(thread_name_prefix="DuckSocketEventDispatch-select",
                                                  max_workers=1)
        if dispatch_thread_size is None or dispatch_thread_size < 1:
            self.dispatch_thread_size = DISPATCH_THREAD_SIZE_DEFAULT
        else:
            self.dispatch_thread_size = dispatch_thread_size
        self.dispatch_executor = ThreadPoolExecutor(thread_name_prefix="DuckSocketEventDispatch-dispatch",
                                                    max_workers=self.dispatch_thread_size)

        self._shutdown_flag = False
        self._lock = threading.Lock()
        self._selector = selectors.DefaultSelector()
        self.select_executor.submit(self._select_loop)

    def _select_loop(self):
        while not self._shutdown_flag:
            self.logger.debug("select start")
            events = self._selector.select(timeout=self.select_timeout)
            self.logger.debug(f"select end, events: {len(events)}")
            q = queue.Queue()
            for recv in events:
                q.put(recv)
                self.dispatch_executor.submit(self._dispatch, q)
            q.join()

    def _dispatch(self, q: queue.Queue[tuple[selectors.SelectorKey, int]]):
        event: tuple[selectors.SelectorKey, int] = q.get()
        fileobj: FileDescriptorLike = event[0].fileobj
        mask: int = event[1]
        handle: DuckSocketEventHandler = event[0].data
        self.logger.debug(f"dispatch {fileobj}")
        handle.handle_event(fileobj=fileobj, mask=mask)
        q.task_done()

    def shutdown(self):
        self._shutdown_flag = True
        self.select_executor.shutdown()
        self.dispatch_executor.shutdown()

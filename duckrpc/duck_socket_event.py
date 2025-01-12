import selectors
import os
import logging
import threading
import queue
import traceback
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
from enum import EnumType

SELECT_TIMEOUT_DEFAULT = 1.0
DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


class EventDispatchMode(EnumType):
    THREAD = "THREAD"
    ASYNCIO = "ASYNCIO"
    SINGLE = "SINGLE"  # dispatch with select thread


class DuckSocketEventHandler(ABC):

    @abstractmethod
    def handle_event(self, fileobj, mask: int) -> None:
        raise NotImplementedError()


class DuckSocketEventDispatchConfig(object):
    select_timeout: int | float
    dispatch_mode: EventDispatchMode
    dispatch_thread_size: int

    def __init__(self,
                 select_timeout: int | float = SELECT_TIMEOUT_DEFAULT,
                 dispatch_mode: EventDispatchMode = EventDispatchMode.THREAD,
                 dispatch_thread_size: int = DISPATCH_THREAD_SIZE_DEFAULT):
        if select_timeout is None or select_timeout < 0:
            select_timeout = SELECT_TIMEOUT_DEFAULT
        self.select_timeout = select_timeout

        if dispatch_mode is None:
            dispatch_mode = EventDispatchMode.THREAD
        self.dispatch_mode = dispatch_mode

        if dispatch_thread_size is None or int(dispatch_thread_size) < 1:
            dispatch_thread_size = DISPATCH_THREAD_SIZE_DEFAULT
        self.dispatch_thread_size = int(dispatch_thread_size)


class DuckSocketEventDispatch(object):
    config: DuckSocketEventDispatchConfig

    selector: selectors.DefaultSelector
    select_executor: ThreadPoolExecutor

    dispatch_executor: ThreadPoolExecutor
    logger: logging.Logger

    _shutdown_flag: bool

    def __init__(self,
                 config: DuckSocketEventDispatchConfig,
                 logger=None):
        self.config = config
        self.selector = selectors.DefaultSelector()
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.select_executor = ThreadPoolExecutor(thread_name_prefix="DuckSocketEventDispatch-select",
                                                  max_workers=1)
        self.dispatch_executor = ThreadPoolExecutor(thread_name_prefix="DuckSocketEventDispatch-dispatch",
                                                    max_workers=self.config.dispatch_thread_size)

        self._shutdown_flag = False
        self._lock = threading.Lock()
        self._selector = selectors.DefaultSelector()
        self.select_executor.submit(self._select_loop)

    def _select_loop(self):
        while not self._shutdown_flag:
            self.logger.debug("select start")
            events = self._selector.select(timeout=self.config.select_timeout)
            self.logger.debug(f"select end, events: {len(events)}")
            if len(events) < 1:
                continue
            if self.config.dispatch_mode == EventDispatchMode.SINGLE:
                self.dispatch_with_single(events=events)
            elif self.config.dispatch_mode == EventDispatchMode.THREAD:
                self.dispatch_with_thread(events=events)

    def dispatch_with_single(self, events):
        for key, mask in events:
            handler: DuckSocketEventHandler = key.data
            handler.handle_event(key.fileobj, mask=mask)

    def _do_dispatch_with_thread(self, q: queue.Queue[tuple[selectors.SelectorKey, int]]):
        event: tuple[selectors.SelectorKey, int] = q.get()
        try:
            fileobj = event[0].fileobj
            mask: int = event[1]
            handle: DuckSocketEventHandler = event[0].data
            self.logger.debug(f"dispatch {fileobj}")
            handle.handle_event(fileobj=fileobj, mask=mask)
        except Exception as exp:
            self.logger.error(f"handle_event failed with {exp}, stack:{traceback.format_exc()}")
        finally:
            q.task_done()

    def dispatch_with_thread(self, events):
        q = queue.Queue()
        for recv in events:
            q.put(recv)
            self.dispatch_executor.submit(self._do_dispatch_with_thread, q)
        q.join()

    def register(self, fileobj, events, data: DuckSocketEventHandler = None):
        self.logger.debug(f"register {fileobj} {data}")
        self._selector.register(fileobj, events, data)

    def unregister(self, fileobj):
        self.logger.debug(f"unregister {fileobj}")
        self._selector.unregister(fileobj)

    def shutdown(self):
        self.logger.debug("shutdown")
        self._shutdown_flag = True
        self.select_executor.shutdown()
        self.dispatch_executor.shutdown()
        self.selector.close()

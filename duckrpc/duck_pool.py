import queue
from queue import Queue
import threading
import logging
import time
from typing import Optional, Union
from .duck_factory import DuckFactory

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16


class DuckPoolConfig(object):
    name: str
    core_size: int
    max_size: int

    def __init__(self,
                 name="",
                 core_size=CORE_SIZE_DEFAULT,
                 max_size=MAX_SIZE_DEFAULT):
        self.name = name
        self.core_size = core_size
        self.max_size = max_size


class DuckPool(object):
    config: DuckPoolConfig
    factory: DuckFactory
    logger: logging.Logger
    _all_set: set[any]
    _idle_queue: Queue[any]
    _lock: threading.Lock
    _count: int
    _shutdown_flag: bool = False

    def __init__(self, config: DuckPoolConfig, factory: DuckFactory, logger=None):
        self.config = config
        self.factory = factory
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._all_set = set()
        self._idle_queue = Queue()
        self._count = 0
        self._lock = threading.Lock()
        self._shutdown_flag = False
        self._build_core_items()

    def _build_core_items(self):
        for _ in range(self.config.core_size):
            conn = self._create_item()
            self._all_set.add(conn)
            self._idle_queue.put(conn)

    def _create_item(self) -> any:
        item = self.factory.create()
        logging.debug("create_item {}".format(item))
        self._count += 1
        return item

    def remove_item(self, item: any) -> None:
        if self._shutdown_flag:
            raise RuntimeError("pool has shutdown.")
        if item in self._all_set:
            with self._lock:
                logging.debug("remote_item {}".format(item))
                self._all_set.remove(item)
                self._count -= 1
                self.factory.destroy(item)

    def check_in(self, item: any) -> None:
        if self._shutdown_flag:
            raise RuntimeError("pool has shutdown.")
        logging.debug("check_in {}".format(item))
        self._idle_queue.put(item)

    def check_out(self, timeout: Union[None, int, float] = None) -> Optional[any]:
        if self._shutdown_flag:
            raise RuntimeError("pool has shutdown.")

        if timeout is not None and not isinstance(timeout, (int, float)):
            raise ValueError("timeout must be one of None,int,float")
        timeout_at = None
        if timeout is not None:
            timeout_at = time.time() + timeout
        while True:
            self.logger.debug(f"check_out start with timeout_at = {timeout_at}")
            item = self.do_check_out(timeout_at=timeout_at)
            if item in self._all_set:
                self.logger.debug(f"check_out {item}")
                return item
            else:
                self.logger.debug(f"{item} has removed.")


    def do_check_out(self, timeout_at: Union[None, int, float] = None):
        try:
            conn = self._idle_queue.get_nowait()
            if conn in self._all_set:
                logging.debug("check_out {}".format(conn))
                return conn
        except queue.Empty:
            pass

        with self._lock:
            if self._count < self.config.max_size:
                conn = self._create_item()
                self._all_set.add(conn)
                logging.debug("check_out {}".format(conn))
                return conn
        try:
            timeout = None
            if timeout_at is not None:
                timeout = timeout_at - time.time()
            self.logger.debug(f"check_out with timeout={timeout}")
            return self._idle_queue.get(timeout=timeout)
        except:
            raise

    def shutdown(self):
        logging.debug("shutdown...")
        self._shutdown_flag = True
        for e in self._all_set:
            self.factory.destroy(e)

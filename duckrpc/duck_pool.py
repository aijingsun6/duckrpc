import queue
from abc import ABC, abstractmethod
from queue import Queue
import threading
import logging

from typing import Optional, Union
from .duck_common import T, DuckPoolItem

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16


class DuckPoolFactory(ABC):
    @abstractmethod
    def create(self) -> T:
        raise NotImplementedError()

    @abstractmethod
    def destroy(self, value: T) -> None:
        raise NotImplementedError()


class DuckPoolConfig(object):
    name: str
    core_size: int
    max_size: int
    factory: DuckPoolFactory

    def __init__(self,
                 name="",
                 core_size=CORE_SIZE_DEFAULT,
                 max_size=MAX_SIZE_DEFAULT,
                 factory=None):
        self.name = name
        self.core_size = core_size
        self.max_size = max_size
        self.factory = factory


class DuckPool(object):
    _config: DuckPoolConfig
    _all_set: set[DuckPoolItem]
    _idle_queue: Queue[DuckPoolItem]
    _lock: threading.Lock
    _count: int
    _shutdown_flag: bool = False
    _logger: logging.Logger

    def __init__(self, config: DuckPoolConfig, logger=None):
        self._config = config
        self._all_set = set()
        self._idle_queue = Queue()
        self._count = 0
        self._lock = threading.Lock()
        self._build_core_items()
        self._shutdown_flag = False
        if logger is None:
            self._logger = logging.getLogger(__name__)
        else:
            self._logger = logger

    def _build_core_items(self):
        for _ in range(self._config.core_size):
            conn = self._create_item()
            self._all_set.add(conn)
            self._idle_queue.put(conn)

    def _create_item(self) -> DuckPoolItem:
        item = DuckPoolItem(item=self._config.factory.create())
        logging.debug("create_item {}".format(item))
        self._count += 1
        return item

    def remove_item(self, item: DuckPoolItem) -> None:
        if self._shutdown_flag:
            raise RuntimeError("pool has shutdown.")
        if item in self._all_set:
            with self._lock:
                logging.debug("remote_item {}".format(item))
                self._all_set.remove(item)
                self._count -= 1

    def check_in(self, item: DuckPoolItem) -> None:
        if self._shutdown_flag:
            raise RuntimeError("pool has shutdown.")
        logging.debug("check_in {}".format(item))
        self._idle_queue.put(item)

    def check_out(self, timeout: Union[None, int, float] = None) -> Optional[DuckPoolItem]:
        if self._shutdown_flag:
            raise RuntimeError("pool has shutdown.")

        if timeout is not None and not isinstance(timeout, (int, float)):
            raise ValueError("timeout must be one of None,int,float")

        try:
            conn = self._idle_queue.get_nowait()
            if conn is not None:
                logging.debug("check_out {}".format(conn))
                return conn
        except queue.Empty:
            pass

        with self._lock:
            if self._count < self._config.max_size:
                conn = self._create_item()
                self._all_set.add(conn)
                logging.debug("check_out {}".format(conn))
                return conn
        item = None
        try:
            item = self._idle_queue.get(timeout=timeout)
        except queue.Empty:
            pass
        logging.debug("check_out {}".format(item))
        return item

    def shutdown(self):
        logging.debug("shutdown...")
        self._shutdown_flag = True
        for e in self._all_set:
            self._config.factory.destroy(e.item)

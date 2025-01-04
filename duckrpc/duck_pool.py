import queue
from abc import ABC, abstractmethod
from queue import Queue
import socket
import threading

from .duck_common import DuckConn

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16


class DuckPoolConfig(ABC):
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

    @abstractmethod
    def create_socket(self) -> socket.socket:
        raise NotImplementedError()


class DuckPool(object):
    _config: DuckPoolConfig
    _all_conn_set: set[DuckConn]
    _idle_conn_queue: Queue[DuckConn]
    _lock: threading.Lock
    _wait_cond: threading.Condition
    _conn_index: int
    _conn_count: int

    def __init__(self, config: DuckPoolConfig):
        self._config = config
        self._all_conn_set = set()
        self._idle_conn_queue = Queue(maxsize=self._config.max_size)
        self._conn_index = 0
        self._conn_count = 0
        self._lock = threading.Lock()
        self._build_core_conn()

    def _build_core_conn(self):
        for _ in range(self._config.core_size):
            conn = self._create_conn()
            self._all_conn_set.add(conn)
            self._idle_conn_queue.put(conn)

    def _create_conn(self) -> DuckConn:
        sock = self._config.create_socket()
        conn = DuckConn(index=self._conn_index, sock=sock)
        self._conn_index += 1
        self._conn_count += 1
        return conn

    def remove_conn(self, conn: DuckConn) -> None:
        if conn in self._all_conn_set:
            self._all_conn_set.remove(conn)
            self._conn_count -= 1

    def check_in(self, conn: DuckConn) -> None:
        self._idle_conn_queue.put(conn)

    def check_out(self, timeout=None) -> DuckConn | None:
        if timeout is not None and isinstance(timeout, (int, float)):
            raise ValueError("timeout must be one of None,int,float")

        try:
            conn = self._idle_conn_queue.get_nowait()
            if conn is not None:
                return conn
        except queue.Empty:
            pass

        with self._lock:
            if self._conn_count < self._config.max_size:
                conn = self._create_conn()
                self._all_conn_set.add(conn)
                return conn

        try:
            return self._idle_conn_queue.get(timeout=timeout)
        except queue.Empty:
            return None

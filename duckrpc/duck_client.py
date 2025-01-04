import selectors
import socket
import os
import threading
import heapq
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from .duck_pool import DuckPool, DuckPoolFactory, DuckPoolConfig
from .duck_common import DuckContext, DuckCoder, DuckSocketReceiver

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16
NAX_READ_THREAD_SIZE = min(32, (os.cpu_count() or 1) + 4)
TIMEOUT_INTERVAL_DEFAULT = 60
TIMEOUT_DEFAULT = 600


class SocketFactory(DuckPoolFactory):

    def create(self) -> socket.socket:
        pass

    def destroy(self, value: socket.socket) -> None:
        pass


@dataclass(order=True)
class TimeoutItem(object):
    timeout_at: int
    ctx_id: str


@dataclass()
class ReplyItem(object):
    ctx_id: str
    lock: threading.Lock


class DuckClient(SocketFactory):
    factory: SocketFactory
    pool: DuckPool
    coder: DuckCoder
    selector = selectors.DefaultSelector()
    _shutdown_flag: bool = False
    _select_thread_pool: ThreadPoolExecutor
    _read_thread_pool: ThreadPoolExecutor
    _timeout_thread_pool: ThreadPoolExecutor
    _timeout_interval: int
    _timeout_default: int
    _reply_map: dict[str, ReplyItem] = dict()
    _timeout_heapq: list[TimeoutItem] = []
    _recv_map: dict[socket.socket, DuckSocketReceiver] = dict()

    def __init__(self,
                 name="",
                 core_conn_size=CORE_SIZE_DEFAULT,
                 max_conn_size=MAX_SIZE_DEFAULT,
                 max_read_thread_size=NAX_READ_THREAD_SIZE,
                 timeout_interval=TIMEOUT_INTERVAL_DEFAULT,
                 timeout_default=TIMEOUT_DEFAULT,
                 factory: SocketFactory = None,
                 coder=None):
        self.factory = factory
        config = DuckPoolConfig(name=name, core_size=core_conn_size, max_size=max_conn_size, factory=self)
        self.pool = DuckPool(config=config)
        self.coder = coder
        self._timeout_interval = timeout_interval
        self._timeout_default = timeout_default
        self._shutdown_flag = False
        self._reply_map = dict()
        self._timeout_heapq = []
        self._recv_map = dict()
        if max_read_thread_size is None:
            max_read_thread_size = NAX_READ_THREAD_SIZE
        self._select_thread_pool = ThreadPoolExecutor(thread_name_prefix=name + "-selector-",
                                                      max_workers=1)
        self._timeout_thread_pool = ThreadPoolExecutor(thread_name_prefix=name + "-timeout-",
                                                       max_workers=1)
        self._read_thread_pool = ThreadPoolExecutor(thread_name_prefix=name + "-read-",
                                                    max_workers=max_read_thread_size)
        self._select_thread_pool.submit(self._select_loop)
        self._timeout_thread_pool.submit(self._timeout_loop)

    def _select_loop(self):
        while not self._shutdown_flag:
            events = self.selector.select()
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

    def _timeout_loop(self):
        while not self._shutdown_flag:
            start = int(time.time())
            while self._timeout_heapq and heapq.nsmallest(1, self._timeout_heapq)[0].timeout_at > start:
                timeout_item = heapq.heappop(self._timeout_heapq)
                del self._reply_map[timeout_item.ctx_id]
            cost = int(time.time()) - start
            sleep = max(0, self._timeout_interval - cost)
            if sleep > 0:
                time.sleep(sleep)

    def _recv_sock(self, sock, mask):
        pass

    def create(self) -> socket.socket:
        sock = self.factory.create()
        sock.setblocking(False)
        self.selector.register(sock, selectors.EVENT_READ, self._recv_sock)
        self._recv_map[sock] = DuckSocketReceiver(coder=self.coder)
        return sock

    def destroy(self, sock: socket.socket) -> None:
        self.selector.unregister(sock)
        self.factory.destroy(sock)

    def rpc(self, ctx: DuckContext, data: any, timeout=TIMEOUT_DEFAULT):
        if timeout is None:
            timeout = self._timeout_default



        pass

    def shutdown(self):
        pass

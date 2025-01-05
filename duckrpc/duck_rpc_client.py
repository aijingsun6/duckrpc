import selectors
import socket
import os
import threading
import heapq
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from .duck_pool import DuckPool, DuckPoolFactory, DuckPoolConfig
from .duck_common import DuckSocketWrap, DuckCoder, DuckSocketReceiver, DuckSocketSender, DuckPacket

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16
READ_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)
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
    iid: str


@dataclass()
class ReplyItem(object):
    iid: str
    cond: threading.Condition
    reply: any

    def __init__(self, iid: str):
        self.iid = iid
        self.cond = threading.Condition()
        self.reply = None


@dataclass
class DuckRpcClientConfig(object):
    name: str
    core_conn_size: int
    max_conn_size: int
    recv_thread_size: int
    timeout_interval: int
    timeout_default: int
    local_addr: str
    local_port: int
    remote_addr: str
    remote_port: int

    def __init__(self):
        pass


class DuckRpcClient(SocketFactory):
    config: DuckRpcClientConfig
    factory: SocketFactory
    pool: DuckPool
    coder: DuckCoder
    selector = selectors.DefaultSelector()
    sender: DuckSocketSender
    _shutdown_flag: bool = False
    _select_thread_pool: ThreadPoolExecutor
    _recv_thread_pool: ThreadPoolExecutor
    _timeout_thread_pool: ThreadPoolExecutor
    _reply_map: dict[str, ReplyItem] = dict()
    _timeout_heapq: list[TimeoutItem] = []

    def __init__(self,
                 config: DuckRpcClientConfig = None,
                 factory: SocketFactory = None,
                 coder=None):
        self.config = config
        if self.config.read_thread_size is None:
            self.config.read_thread_size = READ_THREAD_SIZE_DEFAULT
        if self.config.timeout_default is None:
            self.config.timeout_default = TIMEOUT_DEFAULT
        if self.config.timeout_interval is None:
            self.config.timeout_interval = TIMEOUT_INTERVAL_DEFAULT
        self.factory = factory
        pool_config = DuckPoolConfig(name=config.name,
                                     core_size=self.config.core_conn_size,
                                     max_size=self.config.max_conn_size,
                                     factory=self)
        self.pool = DuckPool(config=pool_config)
        self.coder = coder
        self.sender = DuckSocketSender(coder=coder)
        self._select_thread_pool = ThreadPoolExecutor(thread_name_prefix=self.config.name + "-selector-",
                                                      max_workers=1)
        self._timeout_thread_pool = ThreadPoolExecutor(thread_name_prefix=self.config.name + "-timeout-",
                                                       max_workers=1)
        self._recv_thread_pool = ThreadPoolExecutor(thread_name_prefix=self.config.name + "-read-",
                                                    max_workers=self.config.recv_thread_size)
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
                del self._reply_map[timeout_item.iid]
            cost = int(time.time()) - start
            sleep = max(0, self.config.timeout_interval - cost)
            if sleep > 0:
                time.sleep(sleep)


    def create(self) -> DuckSocketWrap:
        sock = self.factory.create()
        sock.setblocking(False)
        sock.connect((self.config.remote_addr, self.config.remote_port))
        socket_wrap = DuckSocketWrap(sock=sock)
        socket_receiver = DuckSocketReceiver(socket_wrap=socket_wrap, coder=self.coder)
        self.selector.register(sock, selectors.EVENT_READ, socket_receiver)
        return socket_wrap

    def destroy(self, socket_wrap: DuckSocketWrap) -> None:
        self.selector.unregister(socket_wrap.sock)
        self.factory.destroy(socket_wrap.sock)

    def rpc(self, body: any, timeout=None) -> any:
        if timeout is None:
            timeout = self.config.timeout_default
        start = time.time()
        socket_wrap = self.pool.check_out(timeout=timeout)
        packet = self._build_packet(body=body)
        self.sender.send(socket_wrap=socket_wrap, packet=packet)

        reply_item = ReplyItem(iid=packet.iid)
        self._reply_map[packet.iid] = reply_item
        timeout = timeout - time.time() + start

        def pred():
            return reply_item.reply is not None

        reply_item.cond.wait_for(predicate=pred, timeout=timeout)
        del self._reply_map[packet.iid]
        return reply_item.reply

    def _build_packet(self, body: any) -> DuckPacket:
        return DuckPacket(name=self.config.name,
                          local_addr=self.config.local_addr,
                          local_port=self.config.local_port,
                          iid=str(uuid.uuid4()),
                          body=body)

    def shutdown(self):
        pass

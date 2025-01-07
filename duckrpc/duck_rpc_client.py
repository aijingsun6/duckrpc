import queue
import selectors
import os
import threading
import heapq
import time
import uuid
import logging

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from .duck_pool import DuckPool, DuckPoolConfig
from .duck_timeout import DuckTimeoutMgr, TimeoutItem, TimeoutHandler
from .duck_factory import DuckSocketFactory
from .duck_coder import DuckCoder
from .duck_socket_send import DuckSocketSender
from .duck_socket_dispatch import DuckSocketDispatch

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16
RECV_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)
DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)
TIMEOUT_INTERVAL_DEFAULT = 60
TIMEOUT_DEFAULT = 600


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
    read_select_timeout: int
    decode_thread_size: int
    dispatch_thread_size: int
    timeout_interval: int
    timeout_default: int
    timeout_dispatch_thread_size: int
    remote_addr: str
    remote_port: int


class DuckRpcClient(DuckSocketFactory, TimeoutHandler):
    config: DuckRpcClientConfig
    factory: DuckSocketFactory
    coder: DuckCoder
    logger: logging.Logger

    _conn_pool: DuckPool
    _read_selector = selectors.DefaultSelector()
    _read_executor: ThreadPoolExecutor
    _sender: DuckSocketSender
    _timeout_mgr: DuckTimeoutMgr
    _reply_map: dict[str, ReplyItem] = dict()
    _dispatch: DuckSocketDispatch

    def __init__(self,
                 config: DuckRpcClientConfig = None,
                 factory: DuckSocketFactory = None,
                 coder=None,
                 logger=None):
        if logger is None:
            logger = logging.getLogger(__name__)
        super(DuckRpcClient, self).__init__(logger=logger)
        self.config = config
        self.factory = factory
        self.coder = coder
        self.logger = logger

        pool_config = DuckPoolConfig(name=self.config.name,
                                     core_size=self.config.core_conn_size,
                                     max_size=self.config.max_conn_size)
        self._conn_pool = DuckPool(config=pool_config, factory=self.factory, logger=self.logger)
        self._read_executor = ThreadPoolExecutor(thread_name_prefix=f"{self.config.name}-read-", max_workers=1)
        self._sender = DuckSocketSender(coder=self.coder)
        self._timeout_mgr = DuckTimeoutMgr(name=self.config.name,
                                           timeout_interval=self.config.timeout_interval,
                                           handler=self,
                                           dispatch_thread_size=self.config.timeout_dispatch_thread_size,
                                           logger=self.logger)

        self._dispatch = DuckSocketDispatch(name=self.config.name,
                                            decode_thread_size=self.config.decode_thread_size,
                                            dispatch_thread_size=self.config.dispatch_thread_size,
                                            logger=self.logger)
        self._read_executor.submit(self._select_loop)

    def _select_loop(self):
        while not self._shutdown_flag:
            events = self._read_selector.select(timeout=self.config.read_select_timeout)
            for key, mask in events:
                socket_receiver: DuckSocketReceiver = key.data
                socket_receiver_queue.put(socket_receiver)
                self._recv_thread_pool.submit(self._recv_sock, socket_receiver_queue)
            socket_receiver_queue.join()

    def _recv_sock(self, q: queue.Queue[DuckSocketReceiver]):
        socket_receiver: DuckSocketReceiver = q.get()
        res, packet = socket_receiver.recv()
        if res == RecvResult.SOCKET_CLOSED:
            socket_receiver.socket_wrap.sock.close()
            self.pool.remove_item(socket_receiver.socket_wrap)

        if packet is not None:
            self._dispatch_thread_pool.submit(self._dispatch_packet, packet)
        q.task_done()

    def _dispatch_packet(self, packet: DuckPacket):
        iid = packet.iid
        if iid in self._reply_map:
            reply_item = self._reply_map[iid]
            with reply_item.cond:
                reply_item.reply = packet.body
                reply_item.cond.notify()

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
        sock.connect((self.config.remote_addr, self.config.remote_port))
        sock.setblocking(False)
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
        self.pool.check_in(socket_wrap)

        reply_item = ReplyItem(iid=packet.iid)
        self._reply_map[packet.iid] = reply_item
        timeout = timeout - time.time() + start

        def pred():
            return reply_item.reply

        with reply_item.cond:
            reply = reply_item.cond.wait_for(predicate=pred, timeout=timeout)
        del self._reply_map[packet.iid]
        return reply

    def _build_packet(self, body: any) -> DuckPacket:
        return DuckPacket(name=self.config.name,
                          local_addr=self.config.local_addr,
                          local_port=self.config.local_port,
                          iid=str(uuid.uuid4()),
                          body=body)

    def handle_timeout(self, item: TimeoutItem):
        pass

    def shutdown(self):
        self._shutdown_flag = True
        self.pool.shutdown()
        self.selector.close()
        self._select_thread_pool.shutdown()
        self._timeout_thread_pool.shutdown()
        self._recv_thread_pool.shutdown()
        self._dispatch_thread_pool.shutdown()

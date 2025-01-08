import selectors
import os
import threading
import time
import uuid
import logging

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from .duck_pool import DuckPool, DuckPoolConfig
from .duck_timeout import DuckTimeoutMgr, TimeoutItem, TimeoutHandler
from .duck_factory import DuckFactory, DuckSocketFactory
from .duck_coder import DuckCoder
from .duck_packet import DuckPacket
from .duck_socket_wrap import DuckSocketWrap
from .duck_socket_send import DuckSocketSender
from .duck_socket_dispatch import DuckSocketDispatch, DispatchHandler
from .duck_socket_recv import DuckSocketReceiver

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


class DuckRpcClient(DuckFactory, TimeoutHandler, DispatchHandler):
    config: DuckRpcClientConfig
    factory: DuckSocketFactory
    coder: DuckCoder
    logger: logging.Logger
    _shutdown_flag: bool
    _conn_pool: DuckPool
    _read_selector: selectors.DefaultSelector
    _read_executor: ThreadPoolExecutor
    _sender: DuckSocketSender
    _timeout_mgr: DuckTimeoutMgr
    _reply_map: dict[str, ReplyItem]
    _dispatch: DuckSocketDispatch

    def __init__(self,
                 config: DuckRpcClientConfig = None,
                 factory: DuckSocketFactory = None,
                 coder=None,
                 logger=None):
        self.config = config
        self.factory = factory
        self.coder = coder
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

        self._shutdown_flag = False
        pool_config = DuckPoolConfig(name=self.config.name,
                                     core_size=self.config.core_conn_size,
                                     max_size=self.config.max_conn_size)
        self._conn_pool = DuckPool(config=pool_config, factory=self.factory, logger=logger)
        self._read_selector = selectors.DefaultSelector()
        self._read_executor = ThreadPoolExecutor(thread_name_prefix=f"{self.config.name}-read-", max_workers=1)
        self._sender = DuckSocketSender(coder=self.coder)
        self._timeout_mgr = DuckTimeoutMgr(name=self.config.name,
                                           timeout_interval=self.config.timeout_interval,
                                           handler=self,
                                           dispatch_thread_size=self.config.timeout_dispatch_thread_size,
                                           logger=self.logger)
        self._reply_map = dict()
        self._dispatch = DuckSocketDispatch(name=self.config.name,
                                            decode_thread_size=self.config.decode_thread_size,
                                            dispatch_thread_size=self.config.dispatch_thread_size,
                                            dispatch_handler=self,
                                            logger=logger)
        self._read_executor.submit(self._select_loop)

    def _select_loop(self):
        while not self._shutdown_flag:
            events = self._read_selector.select(timeout=self.config.read_select_timeout)
            recv_list: list[DuckSocketReceiver] = []
            for key, mask in events:
                socket_receiver: DuckSocketReceiver = key.data
                recv_list.append(socket_receiver)
            self._dispatch.dispatch(recv_list)

    def create(self) -> DuckSocketWrap:
        sock = self.factory.create()
        sock.connect((self.config.remote_addr, self.config.remote_port))
        sock.setblocking(False)
        socket_wrap = DuckSocketWrap(sock=sock)
        socket_receiver = DuckSocketReceiver(socket_wrap=socket_wrap, coder=self.coder)
        self._read_selector.register(sock, selectors.EVENT_READ, socket_receiver)
        return socket_wrap

    def destroy(self, socket_wrap: DuckSocketWrap) -> None:
        self._read_selector.unregister(socket_wrap.sock)
        self.factory.destroy(socket_wrap.sock)

    def rpc(self, body: any, timeout=None) -> any:
        if timeout is None:
            timeout = self.config.timeout_default
        start = time.time()
        socket_wrap = self._conn_pool.check_out(timeout=timeout)
        packet = self._build_packet(body=body)
        self._sender.send(socket_wrap=socket_wrap, packet=packet)
        self._conn_pool.check_in(socket_wrap)

        reply_item = ReplyItem(iid=packet.iid)
        self._reply_map[packet.iid] = reply_item
        timeout = timeout - time.time() + start
        self._timeout_mgr.add_item(packet.iid, timeout)

        def pred():
            return reply_item.reply

        with reply_item.cond:
            reply = reply_item.cond.wait_for(predicate=pred, timeout=timeout)
        del self._reply_map[packet.iid]
        return reply

    def _build_packet(self, body: any) -> DuckPacket:
        return DuckPacket(name=self.config.name,
                          iid=str(uuid.uuid4()),
                          body=body)

    def handle_timeout(self, item: TimeoutItem):
        if item.value in self._reply_map:
            logging.info(f"delete timeout reply {item.value}")
            del self._reply_map[item.value]

    def dispatch_packet(self, recv: DuckSocketReceiver, packet: DuckPacket) -> None:
        iid = packet.iid
        if iid not in self._reply_map:
            return
        logging.debug(f"reply {iid}")
        reply_item: ReplyItem = self._reply_map[iid]
        with reply_item.cond:
            reply_item.reply = packet.body
            reply_item.cond.notify()

    def dispatch_socket_close(self, recv: DuckSocketReceiver):
        self.logger.info(f"socket {recv.socket_wrap.sock} closed")
        self.destroy(recv.socket_wrap)

    def shutdown(self):
        self._shutdown_flag = True
        self._conn_pool.shutdown()
        self._read_executor.shutdown()
        self._timeout_mgr.shutdown()
        self._dispatch.shutdown()

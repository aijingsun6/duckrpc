import selectors
import os
import threading
import time
import uuid
import logging

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from .duck_coder import DuckCoder, DefaultDuckCoder
from .duck_pool import DuckPool, DuckPoolConfig
from .duck_factory import DuckFactory, DuckSocketFactory
from .duck_packet import DuckPacket
from .duck_socket_wrap import DuckSocketWrap
from .duck_socket_send import DuckSocketSender
from .duck_socket_dispatch import DuckSocketDispatchHandler, DuckSocketDispatch
from .duck_socket_recv import DuckSocketReceiver
from .duck_socket_event import DuckSocketEventDispatch, DuckSocketEventDispatchConfig

CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16
TIMEOUT_DEFAULT = 600
PACKET_DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


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
    timeout_default: int | float
    remote_addr: str
    remote_port: int
    packet_dispatch_thread_size: int
    coder: DuckCoder
    socket_factory: DuckSocketFactory

    def __init__(self,
                 name="",
                 core_conn_size=CORE_SIZE_DEFAULT,
                 max_conn_size=MAX_SIZE_DEFAULT,
                 timeout_default=TIMEOUT_DEFAULT,
                 remote_addr="",
                 remote_port=0,
                 packet_dispatch_thread_size=PACKET_DISPATCH_THREAD_SIZE_DEFAULT,
                 coder: DuckCoder = DefaultDuckCoder(),
                 socket_factory: DuckFactory = DuckSocketFactory()):
        self.name = name
        self.core_conn_size = core_conn_size
        self.max_conn_size = max_conn_size
        self.timeout_default = timeout_default
        self.remote_addr = remote_addr
        self.remote_port = remote_port
        self.packet_dispatch_thread_size = packet_dispatch_thread_size
        self.coder = coder
        self.socket_factory = socket_factory


class DuckRpcClient(DuckFactory, DuckSocketDispatchHandler):
    logger: logging.Logger
    _origin_logger: logging.Logger
    config: DuckRpcClientConfig
    socket_event_dispatch: DuckSocketEventDispatch
    socket_sender: DuckSocketSender
    packet_dispatch_executor: ThreadPoolExecutor

    _shutdown_flag: bool
    _conn_pool: DuckPool
    _sender: DuckSocketSender
    _reply_map: dict[str, ReplyItem]

    def __init__(self,
                 config: DuckRpcClientConfig = None,
                 event_config: DuckSocketEventDispatchConfig = None,
                 logger=None):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._origin_logger = logger
        self.config = config
        self.socket_event_dispatch = DuckSocketEventDispatch(config=event_config, logger=logger)
        self.socket_sender = DuckSocketSender(coder=self.config.coder, logger=logger)
        self.packet_dispatch_executor = ThreadPoolExecutor(thread_name_prefix=f"packet-dispatch-{self.config.name}",
                                                           max_workers=self.config.packet_dispatch_thread_size)
        self._shutdown_flag = False
        pool_config = DuckPoolConfig(name=self.config.name,
                                     core_size=self.config.core_conn_size,
                                     max_size=self.config.max_conn_size)
        self._reply_map = dict()
        self._conn_pool = DuckPool(config=pool_config, factory=self, logger=logger)

    def create(self) -> DuckSocketWrap:
        sock = self.config.socket_factory.create()
        self.logger.info(f" {sock} connect remote {self.config.remote_addr}  {self.config.remote_port}")
        sock.connect((self.config.remote_addr, self.config.remote_port))
        sock.setblocking(False)
        socket_wrap = DuckSocketWrap(sock=sock)
        socket_dispatch = DuckSocketDispatch(socket_wrap=socket_wrap,
                                             coder=self.config.coder,
                                             dispatch_handler=self,
                                             dispatch_executor=self.packet_dispatch_executor,
                                             logger=self._origin_logger)
        self.socket_event_dispatch.register(sock, selectors.EVENT_READ, socket_dispatch)
        return socket_wrap

    def destroy(self, socket_wrap: DuckSocketWrap) -> None:
        self.socket_event_dispatch.unregister(socket_wrap.sock)
        self.config.socket_factory.destroy(socket_wrap.sock)

    def rpc(self, body: any, timeout=None) -> any:
        if timeout is None:
            timeout = self.config.timeout_default

        start = time.time()
        socket_wrap = self._conn_pool.check_out(timeout=timeout)
        packet = self._build_packet(body=body)
        self.logger.debug(f"rpc start, {packet.iid} {packet.body}")
        self.socket_sender.send(socket_wrap=socket_wrap, packet=packet)
        self._conn_pool.check_in(socket_wrap)

        reply_item = ReplyItem(iid=packet.iid)
        self._reply_map[packet.iid] = reply_item
        timeout = max(0, timeout - time.time() + start)

        def pred():
            return reply_item.reply

        with reply_item.cond:
            reply = reply_item.cond.wait_for(predicate=pred, timeout=timeout)
        self.logger.debug(f"rpc end, {packet.iid} {reply}, cost {time.time() - start}")
        del self._reply_map[packet.iid]
        return reply

    def _build_packet(self, body: any) -> DuckPacket:
        return DuckPacket(name=self.config.name,
                          iid=str(uuid.uuid4()),
                          body=body)

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
        self.socket_event_dispatch.shutdown()

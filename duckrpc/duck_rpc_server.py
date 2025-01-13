import selectors
import socket
import time
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
import logging
import os
from abc import ABC, abstractmethod

from .duck_factory import DuckSocketFactory
from .duck_coder import DuckCoder, DefaultDuckCoder
from .duck_packet import DuckPacket
from .duck_socket_send import DuckSocketSender
from .duck_socket_wrap import DuckSocketWrap
from .duck_socket_accept import DuckSocketAccept
from .duck_socket_dispatch import DuckSocketDispatchHandler
from .duck_socket_event import DuckSocketEventDispatch, EventDispatchMode, DuckSocketEventDispatchConfig

EVENT_DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)
PACKET_DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


@dataclass
class DuckRpcServerConfig(object):
    name = "",
    bind_addr: str
    bind_port: int
    backlog: int
    packet_dispatch_thread_size: int
    coder: DuckCoder
    socket_factory: DuckSocketFactory

    def __init__(self,
                 name="",
                 bind_addr="127.0.0.1",
                 bind_port=18080,
                 backlog=100,
                 event_dispatch_mode=EventDispatchMode.THREAD,
                 event_dispatch_thread_size=EVENT_DISPATCH_THREAD_SIZE_DEFAULT,
                 packet_dispatch_thread_size=PACKET_DISPATCH_THREAD_SIZE_DEFAULT,
                 coder: DuckCoder = DefaultDuckCoder(),
                 socket_factory: DuckSocketFactory = DuckSocketFactory()
                 ):
        self.name = name
        self.bind_addr = bind_addr
        self.bind_port = bind_port
        backlog = max(1, backlog)
        self.backlog = backlog
        if event_dispatch_mode is None:
            event_dispatch_mode = EventDispatchMode.THREAD
        self.event_dispatch_mode = event_dispatch_mode
        if event_dispatch_thread_size is None or event_dispatch_thread_size < 1:
            event_dispatch_thread_size = EVENT_DISPATCH_THREAD_SIZE_DEFAULT
        self.event_dispatch_thread_size = event_dispatch_thread_size
        if packet_dispatch_thread_size is None or packet_dispatch_thread_size < 1:
            packet_dispatch_thread_size = PACKET_DISPATCH_THREAD_SIZE_DEFAULT
        self.packet_dispatch_thread_size = packet_dispatch_thread_size
        self.coder = coder
        self.socket_factory = socket_factory


class DuckRpcBodyHandler(ABC):

    @abstractmethod
    def handle_body(self, body: any) -> any:
        raise NotImplementedError()


class DuckRpcServer(DuckSocketDispatchHandler):
    config: DuckRpcServerConfig
    socket_event_dispatch: DuckSocketEventDispatch
    handler: DuckRpcBodyHandler
    socket_sender: DuckSocketSender
    packet_dispatch_executor: ThreadPoolExecutor

    logger: logging.Logger
    origin_logger: logging.Logger
    _sock: socket.socket
    _shutdown_flag: bool = False

    def __init__(self,
                 config: DuckRpcServerConfig,
                 event_config: DuckSocketEventDispatchConfig,
                 handler: DuckRpcBodyHandler,
                 logger: logging.Logger = None):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.origin_logger = logger
        self.config = config
        self.handler = handler
        self.socket_sender = DuckSocketSender(coder=self.config.coder, logger=logger)
        self.socket_event_dispatch = DuckSocketEventDispatch(config=event_config, logger=logger)
        self.packet_dispatch_executor = ThreadPoolExecutor(thread_name_prefix=f"packet-dispatch-{self.config.name}",
                                                           max_workers=self.config.packet_dispatch_thread_size)

        self._shutdown_flag = False

    def start(self, block=False) -> None:
        sock = self.config.socket_factory.create()
        bind_tuple = (self.config.bind_addr, self.config.bind_port)
        sock.bind(bind_tuple)
        logging.info("start rpc server, sock={}, bind={}".format(sock, bind_tuple))
        sock.listen(self.config.backlog)
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock = sock
        socket_wrap = DuckSocketWrap(sock=sock)
        socket_accept: DuckSocketAccept = DuckSocketAccept(socket_wrap=socket_wrap,
                                                           coder=self.config.coder,
                                                           socket_event_dispatch=self.socket_event_dispatch,
                                                           dispatch_handler=self,
                                                           dispatch_executor=self.packet_dispatch_executor,
                                                           logger=self.origin_logger)
        self.socket_event_dispatch.register(sock, selectors.EVENT_READ, socket_accept)
        self._shutdown_flag = False
        if block:
            while not self._shutdown_flag:
                time.sleep(1)

    def dispatch_packet(self, socket_wrap: DuckSocketWrap, packet: DuckPacket) -> None:
        result = self.handler.handle_body(packet.body)
        packet.body = result
        self.socket_sender.send(socket_wrap, packet=packet)

    def dispatch_socket_close(self, socket_wrap: DuckSocketWrap):
        self.socket_event_dispatch.unregister(socket_wrap.sock)
        socket_wrap.sock.close()

    def shutdown(self):
        if self._sock is not None:
            self.socket_event_dispatch.unregister(self._sock)
            self.config.socket_factory.destroy(self._sock)
        self.socket_event_dispatch.shutdown()

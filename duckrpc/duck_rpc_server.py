import os
import selectors
import socket
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import logging
from abc import ABC, abstractmethod

from .duck_packet import DuckPacket
from .duck_socket_recv import DuckSocketReceiver
from .duck_socket_wrap import DuckSocketWrap
from .duck_socket_accept import DuckSocketAccept
from .duck_factory import DuckSocketFactory
from .duck_coder import DuckCoder
from .duck_socket_send import DuckSocketSender
from .duck_socket_dispatch import DuckSocketDispatch, DispatchHandler

DECODE_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)
DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


@dataclass
class DuckRpcServerConfig(object):
    name: str
    decode_thread_size: int
    dispatch_thread_size: int
    bind_addr: str
    bind_port: int
    backlog: int
    accept_select_timeout: int
    read_select_timeout: int

    def __init__(self,
                 name="",
                 decode_thread_size=DECODE_THREAD_SIZE_DEFAULT,
                 dispatch_thread_size=DISPATCH_THREAD_SIZE_DEFAULT,
                 bind_addr="127.0.0.1",
                 bind_port=18080,
                 backlog=100,
                 accept_select_timeout=5,
                 read_select_timeout=5):
        self.name = name
        self.decode_thread_size = decode_thread_size
        self.dispatch_thread_size = dispatch_thread_size
        self.bind_addr = bind_addr
        self.bind_port = bind_port
        self.backlog = backlog
        self.accept_select_timeout = accept_select_timeout
        self.read_select_timeout = read_select_timeout


class DuckRpcBodyHandler(ABC):

    @abstractmethod
    def handle_body(self, body: any) -> any:
        raise NotImplementedError()


class DuckRpcServer(DispatchHandler):
    config: DuckRpcServerConfig
    factory: DuckSocketFactory
    coder: DuckCoder
    handler: DuckRpcBodyHandler
    logger: logging.Logger
    _sock: socket.socket
    _sender: DuckSocketSender
    _accept_selector = selectors.DefaultSelector()
    _read_selector = selectors.DefaultSelector()
    _shutdown_flag: bool = False
    _accept_executor: ThreadPoolExecutor
    _read_executor: ThreadPoolExecutor
    _dispatcher: DuckSocketDispatch

    def __init__(self,
                 config: DuckRpcServerConfig,
                 factory: DuckSocketFactory,
                 coder: DuckCoder,
                 handler: DuckRpcBodyHandler,
                 logger: logging.Logger):
        self.config = config
        self.factory = factory
        self.coder = coder
        self.handler = handler
        if self.logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._sender = DuckSocketSender(coder=coder)

        self._shutdown_flag = False
        self._accept_executor = ThreadPoolExecutor(thread_name_prefix="{}-accept-".format(self.config.name),
                                                   max_workers=1)
        self._read_executor = ThreadPoolExecutor(thread_name_prefix="{}-read-".format(self.config.name),
                                                 max_workers=1)
        self._dispatcher = DuckSocketDispatch(name=self.config.name,
                                              decode_thread_size=self.config.decode_thread_size,
                                              dispatch_thread_size=self.config.dispatch_thread_size,
                                              dispatch_handler=self,
                                              logger=self.logger)

    def accept_loop(self):
        while not self._shutdown_flag:
            events = self._accept_selector.select(timeout=self.config.accept_select_timeout)
            for key, _mask in events:
                socket_accept: DuckSocketAccept = key.data
                socket_accept.accept()

    def read_loop(self):
        while not self._shutdown_flag:
            events = self._read_selector.select(timeout=self.config.read_select_timeout)
            acc: list[DuckSocketReceiver] = []
            for key, _mask in events:
                acc.append(key.data)
            self._dispatcher.dispatch(acc)

    def start(self) -> None:
        sock = self.factory.create()
        bind_tuple = (self.config.bind_addr, self.config.bind_port)
        sock.bind(bind_tuple)
        logging.info("start rpc server, sock={}, bind={}".format(sock, bind_tuple))
        sock.listen(self.config.backlog)
        sock.setblocking(False)
        self._sock = sock
        socket_wrap = DuckSocketWrap(sock=sock)
        socket_accept: DuckSocketAccept = DuckSocketAccept(socket_wrap=socket_wrap,
                                                           selector=self._read_selector,
                                                           coder=self.coder,
                                                           logger=self.logger)
        self._accept_selector.register(sock, selectors.EVENT_READ, socket_accept)
        self._accept_executor.submit(self.accept_loop)
        self._read_executor.submit(self.read_loop)

    def dispatch_packet(self, recv: DuckSocketReceiver, packet: DuckPacket) -> None:
        result = self.handler.handle_body(packet.body)
        packet.body = result
        self._sender.send(recv.socket_wrap, packet=packet)

    def dispatch_socket_close(self, recv: DuckSocketReceiver):
        self._read_selector.unregister(recv.socket_wrap.sock)

    def shutdown(self):
        if self._sock is not None:
            self.factory.destroy(self._sock)
        self._shutdown_flag = True
        self._accept_executor.shutdown()
        self._read_executor.shutdown()
        self._dispatcher.shutdown()
import selectors
import socket
from dataclasses import dataclass
import logging
from abc import ABC, abstractmethod

from .duck_packet import DuckPacket
from .duck_socket_wrap import DuckSocketWrap
from .duck_socket_accept import DuckSocketAccept
from .duck_socket_dispatch import DuckSocketDispatchHandler
from .duck_socket_event import DuckSocketEventDispatch
from .duck_rpc_context import DuckRpcContext


@dataclass
class DuckRpcServerConfig(object):
    bind_addr: str
    bind_port: int
    backlog: int

    def __init__(self,
                 bind_addr="127.0.0.1",
                 bind_port=18080,
                 backlog=100):
        self.bind_addr = bind_addr
        self.bind_port = bind_port
        self.backlog = backlog


class DuckRpcBodyHandler(ABC):

    @abstractmethod
    def handle_body(self, body: any) -> any:
        raise NotImplementedError()


class DuckRpcServer(DuckSocketDispatchHandler):
    config: DuckRpcServerConfig
    context: DuckRpcContext
    socket_event_dispatch: DuckSocketEventDispatch
    handler: DuckRpcBodyHandler

    logger: logging.Logger
    _sock: socket.socket

    def __init__(self,
                 config: DuckRpcServerConfig,
                 context: DuckRpcContext,
                 socket_event_dispatch: DuckSocketEventDispatch,
                 handler: DuckRpcBodyHandler,
                 logger: logging.Logger = None):
        self.config = config
        self.context = context
        self.socket_event_dispatch = socket_event_dispatch
        self.handler = handler
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._shutdown_flag = False

    def start(self) -> None:
        sock = self.context.socket_factory.create()
        bind_tuple = (self.config.bind_addr, self.config.bind_port)
        sock.bind(bind_tuple)
        logging.info("start rpc server, sock={}, bind={}".format(sock, bind_tuple))
        sock.listen(self.config.backlog)
        sock.setblocking(False)
        self._sock = sock
        socket_wrap = DuckSocketWrap(sock=sock)
        socket_accept: DuckSocketAccept = DuckSocketAccept(socket_wrap=socket_wrap,
                                                           context=self.context,
                                                           socket_event_dispatch=self.socket_event_dispatch,
                                                           dispatch_handler=self,
                                                           logger=self.logger)
        self.socket_event_dispatch.register(sock, selectors.EVENT_READ, socket_accept)

    def dispatch_packet(self, socket_wrap: DuckSocketWrap, packet: DuckPacket) -> None:
        result = self.handler.handle_body(packet.body)
        packet.body = result
        self.context.socket_sender.send(socket_wrap, packet=packet)

    def dispatch_socket_close(self, socket_wrap: DuckSocketWrap):
        self.socket_event_dispatch.unregister(socket_wrap.sock)

    def shutdown(self):
        if self._sock is not None:
            self.socket_event_dispatch.unregister(self._sock)
            self.context.socket_factory.destroy(self._sock)
        self.socket_event_dispatch.shutdown()

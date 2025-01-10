from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import logging

from .duck_packet import DuckPacket
from .duck_socket_wrap import DuckSocketWrap
from .duck_coder import DuckCoder
from .duck_socket_recv import DuckSocketReceiver, RecvResult
from .duck_socket_event import DuckSocketEventHandler


class DuckSocketDispatchHandler(ABC):
    @abstractmethod
    def dispatch_packet(self, socket_wrap: DuckSocketWrap, packet: DuckPacket) -> None:
        raise NotImplementedError()

    @abstractmethod
    def dispatch_socket_close(self, socket_wrap: DuckSocketWrap):
        pass


class DuckSocketDispatch(DuckSocketEventHandler):
    coder: DuckCoder
    recv: DuckSocketReceiver
    socket_dispatch_handler: DuckSocketDispatchHandler
    logger: logging.Logger
    dispatch_executor: ThreadPoolExecutor

    def __init__(self,
                 socket_wrap: DuckSocketWrap,
                 coder: DuckCoder,
                 dispatch_handler: DuckSocketDispatchHandler = None,
                 dispatch_executor: ThreadPoolExecutor = None,
                 logger=None):
        self.coder = coder
        self.recv = DuckSocketReceiver(socket_wrap=socket_wrap, logger=logger)
        self.socket_dispatch_handler = dispatch_handler
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.dispatch_executor = dispatch_executor

    def handle_event(self, fileobj, mask: int) -> None:
        result, data = self.recv.recv()
        if result == RecvResult.SOCKET_CLOSED:
            self.logger.debug(f"dispatch socket close {fileobj}")
            self.socket_dispatch_handler.dispatch_socket_close(self.recv.socket_wrap)
            # TODO:
            # self.dispatch_executor.submit(self.socket_dispatch_handler.dispatch_socket_close, self.recv.socket_wrap)

        if data is not None:
            self.dispatch_data(data)
            # TODO:
            # self.dispatch_executor.submit(self.dispatch_data, data)

    def dispatch_data(self, data: bytes):
        packet = self.coder.decode_packet(data)
        self.logger.debug(f"dispatch packet {packet}")
        self.socket_dispatch_handler.dispatch_packet(self.recv.socket_wrap, packet)

    def shutdown(self):
        self.dispatch_executor.shutdown()

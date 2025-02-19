from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import logging
import traceback

from .duck_packet import DuckPacket
from .duck_socket import DuckSocket, RecvResult
from .duck_coder import DuckCoder
from .duck_socket_event_dispatch import DuckSocketEventHandler


class DuckSocketDispatchHandler(ABC):
    @abstractmethod
    def dispatch_packet(self, sock: DuckSocket, packet: DuckPacket) -> None:
        raise NotImplementedError()

    @abstractmethod
    def dispatch_socket_close(self, sock: DuckSocket):
        pass


class DuckSocketDispatch(DuckSocketEventHandler):
    sock: DuckSocket
    coder: DuckCoder
    socket_dispatch_handler: DuckSocketDispatchHandler
    logger: logging.Logger
    dispatch_executor: ThreadPoolExecutor

    def __init__(self,
                 sock: DuckSocket,
                 coder: DuckCoder,
                 dispatch_handler: DuckSocketDispatchHandler = None,
                 dispatch_executor: ThreadPoolExecutor = None,
                 logger=None):
        self.sock = sock
        self.coder = coder
        self.socket_dispatch_handler = dispatch_handler
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self.dispatch_executor = dispatch_executor

    def handle_event(self, fileobj, mask: int) -> None:
        result, data = self.sock.recv()
        if result == RecvResult.SOCKET_CLOSED:
            self.logger.debug(f"dispatch socket close {fileobj}")
            if self.dispatch_executor is None:
                self.socket_dispatch_handler.dispatch_socket_close(self.sock)
            else:
                self.dispatch_executor.submit(self.socket_dispatch_handler.dispatch_socket_close, self.sock)

        if data is not None:
            if self.dispatch_executor is None:
                self.dispatch_data(data)
            else:
                self.dispatch_executor.submit(self.dispatch_data, data)

    def dispatch_data(self, data: bytes):
        packet = self.coder.decode_packet(data)
        self.logger.debug(f"dispatch packet {packet}")
        try:
            self.socket_dispatch_handler.dispatch_packet(self.sock, packet)
        except Exception as exp:
            self.logger.error(f"dispatch_data failed with {exp}, trace: {traceback.format_exc()}")
            self.socket_dispatch_handler.dispatch_socket_close(self.sock)

    def shutdown(self):
        self.dispatch_executor.shutdown()

import os
from concurrent.futures import ThreadPoolExecutor
from abc import ABC, abstractmethod
import queue
import logging

from .duck_packet import DuckPacket
from .duck_socket_recv import DuckSocketReceiver, RecvResult

DECODE_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)

DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


class DispatchHandler(ABC):
    @abstractmethod
    def handle_packet(self, packet: DuckPacket) -> None:
        raise NotImplementedError()

    @abstractmethod
    def handle_socket_close(self, recv: DuckSocketReceiver):
        pass


class DuckSocketDispatch(object):
    name: str = ""
    decode_thread_size: int = DECODE_THREAD_SIZE_DEFAULT
    dispatch_thread_size: int = DISPATCH_THREAD_SIZE_DEFAULT
    dispatch_handler: DispatchHandler
    logger: logging.Logger
    _decode_executor: ThreadPoolExecutor
    _dispatch_executor: ThreadPoolExecutor

    def __init__(self,
                 name="",
                 decode_thread_size=DECODE_THREAD_SIZE_DEFAULT,
                 dispatch_thread_size=DISPATCH_THREAD_SIZE_DEFAULT,
                 dispatch_handler: DispatchHandler = None,
                 logger=None):
        self.name = name
        self.decode_thread_size = decode_thread_size
        self.dispatch_thread_size = dispatch_thread_size
        self.dispatch_handler = dispatch_handler
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger
        self._decode_executor = ThreadPoolExecutor(thread_name_prefix=f"{name}-decode-",
                                                   max_workers=self.decode_thread_size)
        self._dispatch_executor = ThreadPoolExecutor(thread_name_prefix=f"{name}-dispatch-packet-",
                                                     max_workers=self.dispatch_thread_size)

    def dispatch(self, recv_list: list[DuckSocketReceiver]):
        q = queue.Queue()
        for recv in recv_list:
            q.put(recv)
            self._decode_executor.submit(self._decode, q)
        q.join()

    def _decode(self, q: queue.Queue[DuckSocketReceiver]):
        recv: DuckSocketReceiver = q.get()
        result, packet = recv.recv()
        if result == RecvResult.SOCKET_CLOSED:
            self.logger.debug(f"dispatch socket close {recv}")
            self._dispatch_executor.submit(self.dispatch_handler.handle_socket_close, recv)

        if packet is not None:
            self.logger.debug(f"dispatch packet {packet}")
            self._dispatch_executor.submit(self.dispatch_handler.handle_packet, packet)
        q.task_done()
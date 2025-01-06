import os
import selectors
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
import queue
from abc import ABC, abstractmethod

from .duck_common import DuckSocketFactory, DuckCoder, DuckPacket, DuckSocketAccepter, DuckSocketSender, \
    DuckSocketReceiver, DuckSocketWrap, RecvResult

RECV_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)
DISPATCH_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


@dataclass
class DuckRpcServerConfig(object):
    name: str
    recv_thread_size: int
    dispatch_thread_size: int
    timeout_interval: int
    timeout_default: int
    bind_addr: str
    bind_port: int
    backlog: int

    def __init__(self):
        pass


class DuckRpcHandler(ABC):

    @abstractmethod
    def handle(self, body: any) -> any:
        raise NotImplementedError()


class DuckRpcServer(object):
    config: DuckRpcServerConfig
    factory: DuckSocketFactory
    coder: DuckCoder
    handler: DuckRpcHandler
    sender: DuckSocketSender
    accept_selector = selectors.DefaultSelector()
    read_selector = selectors.DefaultSelector()
    sender: DuckSocketSender
    _shutdown_flag: bool = False
    _accept_thread_pool: ThreadPoolExecutor
    _read_thread_pool: ThreadPoolExecutor
    _recv_thread_pool: ThreadPoolExecutor
    _dispatch_thread_pool: ThreadPoolExecutor

    def __init__(self, config: DuckRpcServerConfig,
                 factory: DuckSocketFactory,
                 coder: DuckCoder,
                 handler: DuckRpcHandler):
        self.config = config
        self.factory = factory
        self.coder = coder
        self.handler = handler
        self.sender = DuckSocketSender(coder=coder)
        self._shutdown_flag = False
        self._accept_thread_pool = ThreadPoolExecutor(thread_name_prefix="{}-accept-".format(self.config.name),
                                                      max_workers=1)
        self._read_thread_pool = ThreadPoolExecutor(thread_name_prefix="{}-read-".format(self.config.name),
                                                    max_workers=1)
        self._recv_thread_pool = ThreadPoolExecutor(thread_name_prefix="{}-recv-".format(self.config.name),
                                                    max_workers=self.config.recv_thread_size)
        self._dispatch_thread_pool = ThreadPoolExecutor(thread_name_prefix="{}-dispatch-".format(self.config.name),
                                                        max_workers=self.config.dispatch_thread_size)

    def accept(self):
        while not self._shutdown_flag:
            events = self.accept_selector.select()
            for key, _mask in events:
                accpter: DuckSocketAccepter = key.data
                accpter.accept()

    def read(self):
        while not self._shutdown_flag:
            events = self.read_selector.select()
            socket_receiver_queue = queue.Queue()
            for key, _mask in events:
                socket_receiver: DuckSocketReceiver = key.data
                socket_receiver_queue.put(socket_receiver)
                self._recv_thread_pool.submit(self._recv_sock, socket_receiver_queue)
            socket_receiver_queue.join()

    def _recv_sock(self, q: queue.Queue[DuckSocketReceiver]):
        socket_receiver: DuckSocketReceiver = q.get()
        res, packet = socket_receiver.recv()
        if res == RecvResult.SOCKET_CLOSED:
            socket_receiver.socket_wrap.sock.close()
            self.read_selector.unregister(socket_receiver.socket_wrap.sock)
        if packet is not None:
            self._dispatch_thread_pool.submit(self._dispatch_packet, socket_receiver.socket_wrap, packet)
        q.task_done()

    def _dispatch_packet(self, socket_wrap: DuckSocketWrap, packet: DuckPacket):
        packet.body = self.handler.handle(packet.body)
        self.sender.send(socket_wrap=socket_wrap, packet=packet)

    def start(self) -> None:
        sock = self.factory.create()
        sock.bind((self.config.bind_addr, self.config.bind_port))
        sock.listen(self.config.backlog)
        sock.setblocking(False)
        socket_wrap = DuckSocketWrap(sock=sock)
        accepter: DuckSocketAccepter = DuckSocketAccepter(socket_wrap=socket_wrap, selector=self.read_selector,
                                                          coder=self.coder)
        self.accept_selector.register(sock, selectors.EVENT_READ, accepter)
        self._accept_thread_pool.submit(self.accept)
        self._read_thread_pool.submit(self.read)

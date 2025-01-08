import os
from concurrent.futures import ThreadPoolExecutor
from .duck_coder import DuckCoder
from .duck_factory import DuckSocketFactory
from .duck_socket_event import DuckSocketEventDispatch
from .duck_socket_send import DuckSocketSender
DISPATCH_PACKET_THREAD_SIZE_DEFAULT = min(32, (os.cpu_count() or 1) + 4)


class DuckRpcContext(object):
    dispatch_packet_executor: ThreadPoolExecutor
    coder: DuckCoder
    socket_factory: DuckSocketFactory
    socket_event_dispatch: DuckSocketEventDispatch
    socket_sender: DuckSocketSender

    def __init__(self,
                 dispatch_packet_thread_size: int,
                 coder: DuckCoder,
                 socket_factory: DuckSocketFactory,
                 socket_event_dispatch: DuckSocketEventDispatch,
                 socket_sender: DuckSocketSender):
        if dispatch_packet_thread_size is None or dispatch_packet_thread_size < 1:
            dispatch_packet_thread_size = DISPATCH_PACKET_THREAD_SIZE_DEFAULT
        self.dispatch_socket_executor = ThreadPoolExecutor(thread_name_prefix="duck-dispatch-packet-exec",
                                                           max_workers=dispatch_packet_thread_size)
        self.coder = coder
        self.socket_factory = socket_factory
        self.socket_event_dispatch = socket_event_dispatch
        self.socket_sender = socket_sender
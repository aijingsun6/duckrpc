import selectors
import logging
from concurrent.futures.thread import ThreadPoolExecutor

from .duck_socket_wrap import DuckSocketWrap
from .duck_coder import DuckCoder
from .duck_socket_event_dispatch import DuckSocketEventHandler, DuckSocketEventDispatch
from .duck_socket_dispatch import DuckSocketDispatchHandler, DuckSocketDispatch


class DuckSocketAccept(DuckSocketEventHandler):
    socket_wrap: DuckSocketWrap
    coder: DuckCoder
    socket_event_dispatch: DuckSocketEventDispatch
    dispatch_handler: DuckSocketDispatchHandler
    dispatch_executor: ThreadPoolExecutor
    logger: logging.Logger
    _origin_logger: logging.Logger

    def __init__(self,
                 socket_wrap: DuckSocketWrap,
                 coder: DuckCoder,
                 socket_event_dispatch: DuckSocketEventDispatch,
                 dispatch_handler: DuckSocketDispatchHandler,
                 dispatch_executor: ThreadPoolExecutor,
                 logger=None):
        self.socket_wrap = socket_wrap
        self.coder = coder
        self.socket_event_dispatch = socket_event_dispatch
        self.dispatch_handler = dispatch_handler
        self.dispatch_executor = dispatch_executor
        self._origin_logger = logger
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def handle_event(self, fileobj, mask: int) -> None:
        with self.socket_wrap.read_lock:
            sock, addr = self.socket_wrap.sock.accept()  # 应当已就绪
            sock.setblocking(False)
            self.logger.debug(f"accept {sock}")
            socket_wrap: DuckSocketWrap = DuckSocketWrap(sock=sock)
            dispatch: DuckSocketDispatch = DuckSocketDispatch(socket_wrap=socket_wrap,
                                                              coder=self.coder,
                                                              dispatch_handler=self.dispatch_handler,
                                                              dispatch_executor=self.dispatch_executor,
                                                              logger=self._origin_logger)
            self.socket_event_dispatch.register(sock, selectors.EVENT_READ, dispatch)

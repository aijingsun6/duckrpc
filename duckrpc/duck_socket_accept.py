import selectors
import logging
from _typeshed import FileDescriptorLike

from .duck_socket_wrap import DuckSocketWrap
from .duck_socket_event import DuckSocketEventHandler
from .duck_rpc_context import DuckRpcContext
from .duck_socket_dispatch import DuckSocketDispatchHandler, DuckSocketDispatch


class DuckSocketAccept(DuckSocketEventHandler):
    socket_wrap: DuckSocketWrap
    context: DuckRpcContext
    dispatch_handler: DuckSocketDispatchHandler
    logger: logging.Logger
    _origin_logger: logging.Logger

    def __init__(self,
                 socket_wrap: DuckSocketWrap,
                 context: DuckRpcContext,
                 dispatch_handler: DuckSocketDispatchHandler,
                 logger=None):
        self.socket_wrap = socket_wrap
        self.context = context
        self.dispatch_handler = dispatch_handler
        self._origin_logger = logger
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def handle_event(self, fileobj: FileDescriptorLike, mask: int) -> None:
        with self.socket_wrap.read_lock:
            sock, addr = self.socket_wrap.sock.accept()  # 应当已就绪
            sock.setblocking(False)
            self.logger.debug(f"accep {sock} {addr}")
            socket_wrap: DuckSocketWrap = DuckSocketWrap(sock=sock)
            dispatch: DuckSocketDispatch = DuckSocketDispatch(socket_wrap=socket_wrap,
                                                              coder=self.context.coder,
                                                              dispatch_handler=self.dispatch_handler,
                                                              dispatch_executor=self.context.dispatch_packet_executor,
                                                              logger=self._origin_logger)
            self.context.socket_event_dispatch.register(sock, selectors.EVENT_READ, dispatch)

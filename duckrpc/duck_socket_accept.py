import selectors
import logging

from .duck_socket_wrap import DuckSocketWrap
from .duck_coder import DuckCoder
from .duck_socket_recv import DuckSocketReceiver


class DuckSocketAccepter(object):
    socket_wrap: DuckSocketWrap
    selector: selectors.DefaultSelector
    coder: DuckCoder
    logger: logging.Logger

    def __init__(self, socket_wrap: DuckSocketWrap, selector: selectors.DefaultSelector, coder: DuckCoder, logger=None):
        self.socket_wrap = socket_wrap
        self.selector = selector
        self.coder = coder
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def accept(self):
        with self.socket_wrap.read_lock:
            conn, addr = self.socket_wrap.sock.accept()  # 应当已就绪
            conn.setblocking(False)
            self.logger.debug(f"accept remote_addr: {addr}")
            socket_receiver = DuckSocketReceiver(socket_wrap=DuckSocketWrap(sock=conn), coder=self.coder)
            self.selector.register(conn, selectors.EVENT_READ, socket_receiver)

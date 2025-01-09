import selectors
import socket
import logging

from concurrent.futures import ThreadPoolExecutor


class EchoSocketServer(object):
    selector: selectors.DefaultSelector
    port: int
    select_executor: ThreadPoolExecutor
    logger: logging.Logger
    shutdown_flag: bool
    sock: socket.socket

    def __init__(self, port: int):
        self.selector = selectors.DefaultSelector()
        self.port = port
        self.select_executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("EchoSocketServer")
        self.shutdown_flag = False

    def start(self):
        self.logger.info(f"start. {self.port}")
        sock = socket.socket()
        sock.bind(('127.0.0.1', self.port))
        sock.listen(100)
        sock.setblocking(False)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock = sock
        self.selector.register(sock, selectors.EVENT_READ, self.accept)
        self.shutdown_flag = False
        self.select_executor.submit(self._select_loop)

    def _select_loop(self):
        while not self.shutdown_flag:
            events = self.selector.select(timeout=1.0)
            for key, mask in events:
                callback = key.data
                callback(key.fileobj, mask)

    def accept(self, sock, mask):
        conn, addr = sock.accept()  # 应当已就绪
        self.logger.info(f'accepted  {conn} from {addr}')
        conn.setblocking(False)
        self.selector.register(conn, selectors.EVENT_READ, self.read)

    def read(self, conn, mask):
        data = conn.recv(1000)  # 应当已就绪
        if data:
            self.logger.info(f"echoing {data} to {conn}")
            conn.sendall(data)  # 希望不会阻塞
        else:
            self.logger.info(f"closing {conn}")
            self.selector.unregister(conn)
            conn.close()

    def shutdown(self):
        self.sock.close()
        self.shutdown_flag = True
        self.select_executor.shutdown()

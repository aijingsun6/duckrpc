import socket
import threading


class DuckSocketWrap(object):
    sock: socket.socket
    write_lock: threading.Lock = threading.Lock()
    read_lock: threading.Lock = threading.Lock()

    def __init__(self, sock: socket.socket):
        self.sock = sock

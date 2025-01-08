import socket
import threading


class DuckSocketWrap(object):
    sock: socket.socket
    write_lock: threading.Lock
    read_lock: threading.Lock

    def __init__(self, sock: socket.socket):
        self.sock = sock
        self.write_lock = threading.Lock()
        self.read_lock = threading.Lock()


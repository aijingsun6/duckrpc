from abc import ABC, abstractmethod
from collections import deque
import socket
import threading
import uuid
import struct
from .duck_proto import DuckHeader

NAME_DEFAULT = ""
CORE_SIZE_DEFAULT = 8
MAX_SIZE_DEFAULT = 16


class DuckPoolConfig(ABC):
    name: str
    core_size: int
    max_size: int

    def __init__(self,
                 name=NAME_DEFAULT,
                 core_size=CORE_SIZE_DEFAULT,
                 max_size=MAX_SIZE_DEFAULT):
        self.name = name
        self.core_size = core_size
        self.max_size = max_size

    @abstractmethod
    def create_conn(self) -> socket.socket:
        raise NotImplementedError()


class DuckConn(object):
    index: int
    sock: socket.socket
    write_lock: threading.Lock

    def __init__(self, index: int, sock: socket.socket):
        self.index = index
        self.sock = sock
        self.write_lock = threading.Lock()

    def send(self, header: DuckHeader, data: bytes):
        header.req_id = str(uuid.uuid4())
        header.body_size = len(data)
        header_body = header.encode()
        with self.write_lock:
            self.sock.sendall(struct.pack("!I", len(header_body)))
            self.sock.sendall(header_body)
            self.sock.sendall(data)




class DuckPool(object):
    _config: DuckPoolConfig
    _queue: deque[socket.socket]
    _idle_queue: deque[socket.socket]

    def __init__(self, config: DuckPoolConfig):
        self._config = config
        self._queue = deque(maxlen=config.max_size)
        self._idle_queue = deque(maxlen=config.max_size)

    def check_out(self,timeout=None):
        # TODO:
        pass

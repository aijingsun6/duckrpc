from dataclasses import dataclass
from abc import ABC, abstractmethod
import socket
import threading


@dataclass
class DuckContext(object):
    name: str
    local_addr: str
    local_port: int
    ctx_id: str
    body_size: int

    def __init__(self,
                 name=None,
                 local_addr=None,
                 local_port=0,
                 ctx_id=None,
                 body_size=0):
        self.name = name
        self.local_addr = local_addr
        self.local_port = local_port
        self.ctx_id = ctx_id
        self.body_size = body_size


class DuckerCoder(ABC):

    @abstractmethod
    def encode_context(self, ctx: DuckContext) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def encode_body(self, body: any) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode_context(self, data: bytes) -> DuckContext:
        raise NotImplementedError()

    @abstractmethod
    def decode_body(self, data: bytes) -> any:
        raise NotImplementedError()


class DuckConn(object):
    index: int
    sock: socket.socket
    write_lock: threading.Lock

    def __init__(self,
                 index: int,
                 sock: socket.socket):
        self.index = index
        self.sock = sock
        self.write_lock = threading.Lock()

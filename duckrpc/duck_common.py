import struct
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import socket
import threading
import json
import selectors

from typing import Optional


@dataclass
class DuckPacket(object):
    name: str
    local_addr: str
    local_port: int
    iid: str
    body: any

    def __init__(self, name=None, local_addr=None, local_port=None, iid=None, body=None):
        self.name = name
        self.local_addr = local_addr
        self.local_port = local_port
        self.iid = iid
        self.body = body


class DuckFactory(ABC):
    @abstractmethod
    def create(self) -> any:
        raise NotImplementedError()

    @abstractmethod
    def destroy(self, value: any) -> None:
        raise NotImplementedError()


class DuckSocketFactory(DuckFactory, ABC):

    @abstractmethod
    def create(self) -> socket.socket:
        raise NotImplementedError()

    @abstractmethod
    def destroy(self, value: socket.socket) -> None:
        raise NotImplementedError()


class DuckCoder(ABC):

    @abstractmethod
    def encode_packet(self, packet: DuckPacket) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode_packet(self, data: bytes) -> DuckPacket:
        raise NotImplementedError()


class DefaultDuckCoder(DuckCoder):

    def decode_packet(self, data: bytes) -> DuckPacket:
        packet = DuckPacket()
        packet.__dict__ = json.loads(data.decode(encoding="utf-8"))
        return packet

    def encode_packet(self, packet: DuckPacket) -> bytes:
        return json.dumps(packet.__dict__).encode(encoding="utf-8")


class RecvStatus(Enum):
    READ_HEAD = "READ_HEAD"
    READ_BODY = "READ_BODY"


class RecvResult(Enum):
    CONTINUE = "CONTINUE"
    COMPLETE = "COMPLETE"
    SOCKET_CLOSED = "SOCKET_CLOSED"


class DuckSocketWrap(object):
    sock: socket.socket
    write_lock: threading.Lock = threading.Lock()
    read_lock: threading.Lock = threading.Lock()

    def __init__(self, sock: socket.socket):
        self.sock = sock


class DuckSocketAccepter(object):
    socket_wrap: DuckSocketWrap
    selector: selectors.DefaultSelector
    coder: DuckCoder

    def __init__(self, socket_wrap: DuckSocketWrap, selector: selectors.DefaultSelector, coder: DuckCoder):
        self.socket_wrap = socket_wrap
        self.selector = selector
        self.coder = coder

    def accept(self):
        with self.socket_wrap.read_lock:
            conn, addr = self.socket_wrap.sock.accept()  # 应当已就绪
            conn.setblocking(False)
            socket_receiver = DuckSocketReceiver(socket_wrap=DuckSocketWrap(sock=conn), coder=self.coder)
            self.selector.register(conn, selectors.EVENT_READ, socket_receiver)


class DuckSocketSender(object):
    coder: DuckCoder

    def __init__(self, coder: DuckCoder):
        self.code = coder

    def send(self, socket_wrap: DuckSocketWrap, packet: DuckPacket):
        data = self.coder.encode_packet(packet=packet)
        with socket_wrap.write_lock:
            socket_wrap.sock.sendall(struct.pack("!I", len(data)))
            socket_wrap.sock.sendall(data)


class DuckSocketReceiver(object):
    recv_status: RecvStatus
    acc_bytes: bytes = b''
    body_size: int = 0
    socket_wrap: DuckSocketWrap
    coder: DuckCoder

    def __init__(self, socket_wrap: DuckSocketWrap, coder: DuckCoder):
        self.recv_status = RecvStatus.READ_HEAD
        self.acc_bytes = b''
        self.socket_wrap = socket_wrap
        self.body_size = 0
        self.coder = coder

    def recv(self) -> tuple[RecvResult, Optional[DuckPacket]]:

        if self.recv_status == RecvStatus.READ_HEAD:
            return self._recv_head()
        elif self.recv_status == RecvStatus.READ_BODY:
            return self._recv_body()

    def _recv_head(self) -> tuple[RecvResult, Optional[DuckPacket]]:
        with self.socket_wrap.read_lock:
            data = self.socket_wrap.sock.recv(4 - len(self.acc_bytes))
            if len(data) == 0:
                return RecvResult.SOCKET_CLOSED, None
            self.acc_bytes += data
            if len(self.acc_bytes) == 4:
                self.ctx_size = struct.unpack("!I", self.acc_bytes)[0]
                self.acc_bytes = b''
                self.recv_status = RecvStatus.READ_BODY
            return RecvResult.CONTINUE, None

    def _recv_body(self) -> tuple[RecvResult, Optional[DuckPacket]]:
        with self.socket_wrap.read_lock:
            data = self.socket_wrap.sock.recv(self.body_size - len(self.acc_bytes))
            if len(data) == 0:
                return RecvResult.SOCKET_CLOSED, None
            self.acc_bytes += data
            if len(self.acc_bytes) == self.body_size:
                packet = self.coder.decode_packet(self.acc_bytes)
                self.recv_status = RecvStatus.READ_HEAD
                self.acc_bytes = b''
                return RecvResult.COMPLETE, packet

            return RecvResult.CONTINUE, None

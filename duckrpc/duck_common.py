import struct
import uuid
from dataclasses import dataclass
from abc import ABC, abstractmethod
from enum import Enum
import socket
import threading

from typing import Optional


@dataclass
class DuckReqCtx(object):
    name: str
    local_addr: str
    local_port: int
    iid: str
    body_size: int

    def __init__(self,
                 name=None,
                 local_addr=None,
                 local_port=0,
                 body_size=0):
        self.name = name
        self.local_addr = local_addr
        self.local_port = local_port
        self.body_size = body_size


class DuckCoder(ABC):

    @abstractmethod
    def encode_context(self, ctx: DuckReqCtx) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def encode_body(self, body: any) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode_context(self, data: bytes) -> DuckReqCtx:
        raise NotImplementedError()

    @abstractmethod
    def decode_body(self, data: bytes) -> any:
        raise NotImplementedError()


class RecvStatus(Enum):
    READ_HEAD = "READ_HEAD"
    READ_CTX = "READ_CTX"
    READ_BODY = "READ_BODY"


class RecvResult(Enum):
    CONTINUE = "CONTINUE"
    COMPLETE = "COMPLETE"
    SOCKET_CLOSED = "SOCKET_CLOSED"


class DuckSocketSender(object):
    coder: DuckCoder

    def __init__(self, coder: DuckCoder):
        self.code = coder

    def send(self, sock: socket.socket, ctx: DuckReqCtx, body: any):
        body_bytes = self.coder.encode_body(body)
        ctx.body_size = len(body_bytes)
        ctx.iid = str(uuid.uuid4())
        ctc_data = self.coder.encode_context(ctx)


        pass


class DuckSocketReceiver(object):
    acc_bytes: bytes
    ctx_size: int = 0
    recv_status: RecvStatus
    ctx: Optional[DuckReqCtx]
    coder: DuckCoder
    body: any

    def __init__(self, coder: DuckCoder):
        self.acc_bytes = b''
        self.recv_status = RecvStatus.READ_HEAD
        self.ctx = None
        self.ctx_size = 0
        self.coder = coder
        self.body = None

    def recv(self, sock: socket.socket) -> RecvResult:

        if self.recv_status == RecvStatus.READ_HEAD:
            return self._recv_head(sock=sock)
        elif self.recv_status == RecvStatus.READ_CTX:
            return self._recv_ctx(sock=sock)
        elif self.recv_status == RecvStatus.READ_BODY:
            return self._recv_body(sock=sock)

    def _recv_head(self, sock: socket.socket) -> RecvResult:
        data = sock.recv(4 - len(self.acc_bytes))
        if len(data) == 0:
            return RecvResult.SOCKET_CLOSED
        self.acc_bytes += data
        if len(self.acc_bytes) == 4:
            self.ctx_size = struct.unpack("!I", self.acc_bytes)[0]
            self.acc_bytes = b''
            self.recv_status = RecvStatus.READ_CTX
        return RecvResult.CONTINUE

    def _recv_ctx(self, sock: socket.socket) -> RecvResult:
        data = sock.recv(self.ctx_size - len(self.acc_bytes))
        if len(data) == 0:
            return RecvResult.SOCKET_CLOSED
        self.acc_bytes += data
        if len(self.acc_bytes) == self.ctx_size:
            self.ctx = self.coder.decode_context(self.acc_bytes)
            self.acc_bytes = b''
            self.recv_status = RecvStatus.READ_BODY
        return RecvResult.CONTINUE

    def _recv_body(self, sock: socket.socket) -> RecvResult:
        data = sock.recv(self.ctx.body_size - len(self.acc_bytes))
        if len(data) == 0:
            return RecvResult.SOCKET_CLOSED
        self.acc_bytes += data
        if len(self.acc_bytes) == self.ctx.body_size:
            self.recv_status = RecvStatus.READ_HEAD
            self.acc_bytes = b''
            self.body = self.coder.decode_body(self.acc_bytes)
            return RecvResult.COMPLETE
        return RecvResult.CONTINUE

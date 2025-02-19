import socket
import threading
import struct
import logging
from enum import Enum
from typing import Optional


class DuckHeadPackType(Enum):
    SHORT = "SHORT",
    INT = "INT"
    LONG = "LONG"


class RecvStatus(Enum):
    READ_HEAD = "READ_HEAD"
    READ_BODY = "READ_BODY"


class RecvResult(Enum):
    CONTINUE = "CONTINUE"
    COMPLETE = "COMPLETE"
    SOCKET_CLOSED = "SOCKET_CLOSED"


HEAD_SIZE_DICT = {
    DuckHeadPackType.SHORT: 2,
    DuckHeadPackType.INT: 4,
    DuckHeadPackType.LONG: 8
}


class DuckSocket(object):
    sock: socket.socket
    head_pack_type: DuckHeadPackType
    write_lock: threading.Lock
    read_lock: threading.Lock
    recv_status: RecvStatus
    acc_bytes: bytes = b''
    head_size: int = 0
    body_size: int = 0
    logger = None

    def __init__(self, sock: socket.socket, head_pack_type=DuckHeadPackType.INT, logger=None):
        self.sock = sock
        if head_pack_type is None:
            head_pack_type = DuckHeadPackType.INT
        self.head_pack_type = head_pack_type
        self.write_lock = threading.Lock()
        self.read_lock = threading.Lock()
        self.recv_status = RecvStatus.READ_HEAD
        self.acc_bytes = b''
        self.head_size = HEAD_SIZE_DICT[self.head_pack_type]
        self.body_size = 0
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def pack_size(self, size: int) -> bytes:
        if self.head_pack_type == DuckHeadPackType.SHORT:
            return struct.pack("!H", size)
        elif self.head_pack_type == DuckHeadPackType.INT:
            return struct.pack("!I", size)
        elif self.head_pack_type == DuckHeadPackType.LONG:
            return struct.pack("!Q", size)

    def unpack_size(self, data: bytes) -> int:
        if self.head_pack_type == DuckHeadPackType.SHORT:
            return struct.unpack("!H", data)[0]
        elif self.head_pack_type == DuckHeadPackType.INT:
            return struct.unpack("!I", data)[0]
        elif self.head_pack_type == DuckHeadPackType.LONG:
            return struct.unpack("!Q", data)[0]

    def send(self, data: bytes) -> None:
        with self.write_lock:
            self.sock.sendall(self.pack_size(len(data)))
            self.sock.sendall(data)

    def recv(self) -> tuple[RecvResult, Optional[bytes]]:
        if self.recv_status == RecvStatus.READ_HEAD:
            return self._recv_head()
        elif self.recv_status == RecvStatus.READ_BODY:
            return self._recv_body()

    def _recv_head(self) -> tuple[RecvResult, Optional[bytes]]:
        with self.read_lock:
            data = self.sock.recv(self.head_size - len(self.acc_bytes))
            self.logger.debug(f"recv {len(data)} bytes")
            if len(data) == 0:
                return RecvResult.SOCKET_CLOSED, None
            self.acc_bytes += data
            if len(self.acc_bytes) == self.head_size:
                self.body_size = self.unpack_size(self.acc_bytes)
                self.acc_bytes = b''
                self.recv_status = RecvStatus.READ_BODY
            return RecvResult.CONTINUE, None

    def _recv_body(self) -> tuple[RecvResult, Optional[bytes]]:
        with self.read_lock:
            data = self.sock.recv(self.body_size - len(self.acc_bytes))
            self.logger.debug(f"recv {len(data)} bytes")
            if len(data) == 0:
                return RecvResult.SOCKET_CLOSED, None
            self.acc_bytes += data
            if len(self.acc_bytes) == self.body_size:
                res = self.acc_bytes
                self.recv_status = RecvStatus.READ_HEAD
                self.acc_bytes = b''
                return RecvResult.COMPLETE, res
            return RecvResult.CONTINUE, None

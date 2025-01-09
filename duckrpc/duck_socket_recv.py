from enum import Enum
from typing import Optional
import struct
import logging

from .duck_socket_wrap import DuckSocketWrap


class RecvStatus(Enum):
    READ_HEAD = "READ_HEAD"
    READ_BODY = "READ_BODY"


class RecvResult(Enum):
    CONTINUE = "CONTINUE"
    COMPLETE = "COMPLETE"
    SOCKET_CLOSED = "SOCKET_CLOSED"


class DuckSocketReceiver(object):
    recv_status: RecvStatus
    acc_bytes: bytes = b''
    body_size: int = 0
    socket_wrap: DuckSocketWrap
    logger: logging.Logger

    def __init__(self, socket_wrap: DuckSocketWrap, logger=None):
        self.recv_status = RecvStatus.READ_HEAD
        self.acc_bytes = b''
        self.socket_wrap = socket_wrap
        self.body_size = 0
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def recv(self) -> tuple[RecvResult, Optional[bytes]]:
        if self.recv_status == RecvStatus.READ_HEAD:
            return self._recv_head()
        elif self.recv_status == RecvStatus.READ_BODY:
            return self._recv_body()

    def _recv_head(self) -> tuple[RecvResult, Optional[bytes]]:
        with self.socket_wrap.read_lock:
            data = self.socket_wrap.sock.recv(4 - len(self.acc_bytes))
            self.logger.debug(f"recv {len(data)} bytes")
            if len(data) == 0:
                return RecvResult.SOCKET_CLOSED, None
            self.acc_bytes += data
            if len(self.acc_bytes) == 4:
                self.body_size = struct.unpack("!I", self.acc_bytes)[0]
                self.acc_bytes = b''
                self.recv_status = RecvStatus.READ_BODY
            return RecvResult.CONTINUE, None

    def _recv_body(self) -> tuple[RecvResult, Optional[bytes]]:
        with self.socket_wrap.read_lock:
            data = self.socket_wrap.sock.recv(self.body_size - len(self.acc_bytes))
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

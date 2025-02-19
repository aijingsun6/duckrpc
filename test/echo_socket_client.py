import uuid
import logging

from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_packet import DuckPacket
from duckrpc.duck_socket import DuckSocket
import socket


class EchoSocketClient(object):
    socket_factory: DuckSocketFactory
    coder: DuckCoder
    logger: logging.Logger
    _sock: socket.socket

    def __init__(self,
                 socket_factory: DuckSocketFactory,
                 coder: DuckCoder,
                 port:int):
        self.socket_factory = socket_factory
        self.coder = coder
        self.logger = logging.getLogger(__name__)
        self._sock = self.socket_factory.create()
        self._sock.connect(('127.0.0.1', port))


    def rpc(self, value: any):
        packet = DuckPacket(name="echo-socket-client", iid=str(uuid.uuid4()), body=value)
        socket_wrap = DuckSocket(sock=self._sock)
        socket_wrap.send( self.coder.encode_packet(packet=packet))
        socket_wrap.recv()
        _, data = socket_wrap.recv()
        self.logger.info(f"{data}")
        recv:DuckPacket = self.coder.decode_packet(data)
        self.logger.info(f"{recv}")
        return recv.body

    def shutdown(self):
        self._sock.close()

import uuid
import logging

from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_socket_send import DuckSocketSender
from duckrpc.duck_packet import DuckPacket
from duckrpc.duck_socket_wrap import DuckSocketWrap
import socket


class EchoSocketClient(object):
    socket_factory: DuckSocketFactory
    socket_sender: DuckSocketSender
    logger: logging.Logger
    _sock: socket.socket

    def __init__(self,
                 socket_factory: DuckSocketFactory,
                 socket_sender: DuckSocketSender,
                 port:int):
        self.socket_factory = socket_factory
        self.socket_sender = socket_sender
        self.logger = logging.getLogger(__name__)
        self._sock = self.socket_factory.create()
        self._sock.connect(('127.0.0.1', port))


    def rpc(self, value: any):
        packet = DuckPacket(name="echo-socket-client", iid=str(uuid.uuid4()), body=value)
        socket_wrap = DuckSocketWrap(sock=self._sock)
        self.socket_sender.send(socket_wrap, packet)
        size = len(self.socket_sender.coder.encode_packet(packet))
        data = self._sock.recv(1000)
        self.logger.info(f"{data}")
        recv:DuckPacket = self.socket_sender.coder.decode_packet(data[4:])
        self.logger.info(f"{recv}")
        return recv.body

    def shutdown(self):
        self._sock.close()

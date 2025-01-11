import uuid
import logging

from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_socket_send import DuckSocketSender
from duckrpc.duck_packet import DuckPacket
from duckrpc.duck_socket_wrap import DuckSocketWrap
from duckrpc.duck_socket_recv import DuckSocketReceiver
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

        recv:DuckSocketReceiver = DuckSocketReceiver(socket_wrap=socket_wrap)
        recv.recv()# read head
        _, data = recv.recv()
        self.logger.info(f"{data}")
        recv:DuckPacket = self.socket_sender.coder.decode_packet(data)
        self.logger.info(f"{recv}")
        return recv.body

    def shutdown(self):
        self._sock.close()

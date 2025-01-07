import struct

from .duck_coder import DuckCoder
from .duck_socket_wrap import DuckSocketWrap
from .duck_packet import DuckPacket


class DuckSocketSender(object):
    coder: DuckCoder

    def __init__(self, coder: DuckCoder):
        self.coder = coder

    def send(self, socket_wrap: DuckSocketWrap, packet: DuckPacket):
        data = self.coder.encode_packet(packet=packet)
        with socket_wrap.write_lock:
            socket_wrap.sock.sendall(struct.pack("!I", len(data)))
            socket_wrap.sock.sendall(data)

from duckrpc.duck_rpc_server import DuckRpcPacketHandler
from duckrpc.duck_socket import DuckSocket
from duckrpc.duck_packet import DuckPacket

import logging
from json_coder import JsonCoder

class EchoHandler(DuckRpcPacketHandler):

    coder = JsonCoder()

    def handle_packet(self, sock: DuckSocket, packet: DuckPacket) -> None:
        logging.info(f"handle {packet.body}")
        data = self.coder.encode_packet(packet)
        sock.send(data)
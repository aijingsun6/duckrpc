import json
import socket
import unittest
import logging
import sys

from duckrpc.duck_common import DuckCoder, DuckPacket, DuckSocketFactory
from duckrpc.duck_rpc_server import DuckRpcHandler, DuckRpcServerConfig, DuckRpcServer

logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)s %(levelname)s %(threadName)s %(filename)s %(lineno)d %(message)s")
logger = logging.getLogger(__name__)



class EchoHandler(DuckRpcHandler):

    def handle(self, body: any) -> any:
        logging.info("handle {}".format(body))
        return body

class EchoCoder(DuckCoder):
    def encode_packet(self, packet: DuckPacket) -> bytes:
        return json.dumps(packet.__dict__).encode("utf-8")

    def decode_packet(self, data: bytes) -> DuckPacket:
        packet = DuckPacket()
        packet.__dict__ = json.loads(data.decode("utf-8"))
        return packet

class EchoSocketFactory(DuckSocketFactory):
    def create(self) -> socket.socket:
        return socket.socket()

    def destroy(self, value: socket.socket) -> None:
        pass


BIND_ADDR = "127.0.0.1"
BIND_PORT=30080
BACKLOG = 10

def build_rpc_server():
    config: DuckRpcServerConfig = DuckRpcServerConfig(name="rpc_server",
                                                      recv_thread_size=0,
                                                      dispatch_thread_size=0,
                                                      bind_addr=BIND_ADDR,
                                                      bind_port=BIND_PORT,
                                                      backlog=BACKLOG)
    rpc_server = DuckRpcServer(config=config,factory=EchoSocketFactory(),coder=EchoCoder(),handler=EchoHandler())
    return rpc_server


class EchoClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.info("setUpClass {}".format(cls))
        rpc_server = build_rpc_server()
        rpc_server.start()
        cls.rpc_server = rpc_server

    @classmethod
    def tearDownClass(cls):
        logging.info("tearDownClass {}".format(cls))
        cls.rpc_server.shutdown()

    def test(self):
        pass





if __name__ == '__main__':
    unittest.main()
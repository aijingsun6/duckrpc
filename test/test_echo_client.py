import json
import socket
import unittest
import logging
import sys

from duckrpc.duck_common import DuckCoder, DuckPacket, DuckSocketFactory
from duckrpc.duck_rpc_server import DuckRpcHandler, DuckRpcServerConfig, DuckRpcServer
from duckrpc.duck_rpc_client import DuckRpcClientConfig, DuckRpcClient

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
BIND_PORT = 30080
BACKLOG = 10


def build_rpc_server():
    config: DuckRpcServerConfig = DuckRpcServerConfig(name="rpc_server",
                                                      recv_thread_size=0,
                                                      dispatch_thread_size=0,
                                                      bind_addr=BIND_ADDR,
                                                      bind_port=BIND_PORT,
                                                      backlog=BACKLOG)
    rpc_server = DuckRpcServer(config=config, factory=EchoSocketFactory(), coder=EchoCoder(), handler=EchoHandler())
    return rpc_server


def build_rpc_client():
    config: DuckRpcClientConfig = DuckRpcClientConfig(
        name="rpc_client",
        core_conn_size=2,
        max_conn_size=4,
        recv_thread_size=4,
        dispatch_thread_size=4,
        timeout_default=600,
        timeout_interval=60,
        local_addr="",
        local_port=0,
        remote_addr=BIND_ADDR,
        remote_port=BIND_PORT
    )
    return DuckRpcClient(config=config, factory=EchoSocketFactory(), coder=EchoCoder())


class EchoClientTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        logging.info("setUpClass {}".format(cls))
        rpc_server = build_rpc_server()
        rpc_server.start()
        cls.rpc_server = rpc_server
        cls.rpc_client = build_rpc_client()

    @classmethod
    def tearDownClass(cls):
        logging.info("tearDownClass {}".format(cls))
        cls.rpc_server.shutdown()
        cls.rpc_client.shutdown()

    def get_client(self) -> DuckRpcClient:
        return type(self).rpc_client

    def test_get_client(self):
        self.assertIsNotNone(self.get_client())

    def test_simple(self):
        client: DuckRpcClient = self.get_client()
        req = "hello"
        res = client.rpc(body=req, timeout=5)
        print(res)
        self.assertEqual(req, res)


if __name__ == '__main__':
    unittest.main()

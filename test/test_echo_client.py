import json
import socket
import time
import unittest
import logging
import sys
from duckrpc.duck_rpc_server import DuckRpcBodyHandler
from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_packet import DuckPacket
from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_rpc_client import DuckRpcClientConfig, DuckRpcClient
from duckrpc.duck_rpc_context import DuckRpcContext
from duckrpc.duck_socket_send import DuckSocketSender
from duckrpc.duck_socket_event import DuckSocketEventDispatch
from echo_socket_server import EchoSocketServer

logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)s %(levelname)s %(threadName)s %(filename)s %(lineno)d %(message)s")
logger = logging.getLogger(__name__)


class EchoHandler(DuckRpcBodyHandler):

    def handle_body(self, body: any) -> any:
        logging.info(f"handle {body}")
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

    def destroy(self, sock: socket.socket) -> None:
        logger.info(f"close {sock}")
        sock.close()


BIND_ADDR = "127.0.0.1"
BIND_PORT = 30080
BACKLOG = 10


def build_context() -> DuckRpcContext:
    coder = EchoCoder()
    sender = DuckSocketSender(coder=coder)

    return DuckRpcContext(dispatch_packet_thread_size=4,
                          coder=coder,
                          socket_factory=EchoSocketFactory(),
                          socket_sender=sender)


def build_rpc_server():
    return EchoSocketServer(port=BIND_PORT)


def build_rpc_client(context: DuckRpcContext):
    config: DuckRpcClientConfig = DuckRpcClientConfig(
        name="rpc_client",
        core_conn_size=1,
        max_conn_size=1,
        timeout_default=600,
        timeout_interval=60,
        timeout_dispatch_thread_size=1,
        remote_addr=BIND_ADDR,
        remote_port=BIND_PORT
    )
    event_dispatch = DuckSocketEventDispatch(select_timeout=1, dispatch_thread_size=1)
    return DuckRpcClient(config=config, context=context, socket_event_dispatch=event_dispatch)


class EchoClientTest(unittest.TestCase):

    # def setUp(self):
    #     logger.info("setUp")
    #     ctx = build_context()
    #     rpc_server = build_rpc_server()
    #     rpc_server.start()
    #     self.rpc_server = rpc_server
    #     self.rpc_client = build_rpc_client(context=ctx)

    # def tearDown(self):
    #     logger.info("tearDown")
    #     self.rpc_server.shutdown()
    #     self.rpc_client.shutdown()

    @classmethod
    def setUpClass(cls):
        logging.info("setUpClass {}".format(cls))
        cls.rpc_server = build_rpc_server()
        cls.rpc_server.start()
        ctx = build_context()

        cls.rpc_client = build_rpc_client(context=ctx)

    @classmethod
    def tearDownClass(cls):
        logging.info("tearDownClass {}".format(cls))
        cls.rpc_server.shutdown()
        cls.rpc_client.shutdown()

    def get_client(self) -> DuckRpcClient:
        return type(self).rpc_client

    def test_simple(self):
        req = "hello"
        res = self.rpc_client.rpc(body=req, timeout=5)
        self.assertEqual(req, res)


if __name__ == '__main__':
    unittest.main()

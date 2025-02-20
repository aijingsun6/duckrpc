import socket
import unittest
import logging
import sys
import time

from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_rpc_server import DuckRpcServerConfig, DuckRpcServer, DuckRpcPacketHandler
from duckrpc.duck_event_dispatch import DuckEventDispatchConfig
from echo_socket_client import EchoSocketClient

from json_coder import JsonCoder

from echo_packet_handler import EchoHandler


logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)s %(levelname)s %(threadName)s %(filename)s %(lineno)d %(message)s")
logger = logging.getLogger(__name__)




class EchoSocketFactory(DuckSocketFactory):
    def create(self) -> socket.socket:
        return socket.socket()

    def destroy(self, sock: socket.socket) -> None:
        logger.info(f"close {sock}")
        sock.close()


BIND_ADDR = "127.0.0.1"
BIND_PORT = 30080
BACKLOG = 10





def build_rpc_server():

    config = DuckRpcServerConfig(
        name="rpc-server",
        bind_addr=BIND_ADDR,
        bind_port=BIND_PORT,
        backlog=BACKLOG,
        packet_dispatch_thread_size = 1,
        coder = JsonCoder(),
        socket_factory=EchoSocketFactory()
    )
    event_config = DuckEventDispatchConfig(select_timeout=1.0,
                                           dispatch_thread_size=1)
    return DuckRpcServer(config=config,event_config=event_config,handler=EchoHandler())




class EchoServerTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        logging.info("setUpClass {}".format(cls))
        cls.rpc_server = build_rpc_server()
        cls.rpc_server.start()
        coder = JsonCoder()
        cls.rpc_client = EchoSocketClient(socket_factory=EchoSocketFactory(),
                                          coder=coder,
                                          port=BIND_PORT)

    @classmethod
    def tearDownClass(cls):
        logging.info("tearDownClass {}".format(cls))
        cls.rpc_server.shutdown()
        cls.rpc_client.shutdown()

    def get_client(self) -> EchoSocketClient:
        return type(self).rpc_client

    def test_simple(self):
        req = "hello"
        start = time.time()
        res = self.rpc_client.rpc(req)
        cost = time.time() - start
        logger.info(f"rpc with code {cost}")
        self.assertEqual(req, res)


if __name__ == '__main__':
    unittest.main()
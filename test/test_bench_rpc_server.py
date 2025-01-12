import json
import queue
import socket
import unittest
import logging
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor

from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_packet import DuckPacket
from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_rpc_server import DuckRpcServerConfig, DuckRpcServer, DuckRpcBodyHandler
from duckrpc.duck_socket_event import DuckSocketEventDispatchConfig,EventDispatchMode
from duckrpc.duck_rpc_client import DuckRpcClientConfig, DuckRpcClient


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
        logger.info(f"{data}")
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





def build_rpc_server(event_dispatch_thread_size=4, packet_dispatch_thread_size=4):

    config = DuckRpcServerConfig(
        name="rpc-server",
        bind_addr=BIND_ADDR,
        bind_port=BIND_PORT,
        backlog=BACKLOG,
        packet_dispatch_thread_size = packet_dispatch_thread_size,
        coder = EchoCoder(),
        socket_factory=EchoSocketFactory()
    )
    event_config = DuckSocketEventDispatchConfig(select_timeout=0.2,
                                                 dispatch_thread_size=event_dispatch_thread_size,
                                                 dispatch_mode=EventDispatchMode.THREAD)
    return DuckRpcServer(config=config,event_config=event_config,handler=EchoHandler())

def build_rpc_client(event_dispatch_thread_size=4, packet_dispatch_thread_size=4):
    config: DuckRpcClientConfig = DuckRpcClientConfig(
        name="rpc_client",
        core_conn_size=1,
        max_conn_size=1,
        timeout_default=600,
        remote_addr=BIND_ADDR,
        remote_port=BIND_PORT,
        packet_dispatch_thread_size=packet_dispatch_thread_size,
        coder=EchoCoder(),
        socket_factory=DuckSocketFactory()
    )
    event_config = DuckSocketEventDispatchConfig(select_timeout=0.1, dispatch_thread_size=event_dispatch_thread_size)
    return DuckRpcClient(config=config, event_config=event_config)



class BenchRpcServerTest(unittest.TestCase):

    def rpc_one(self, rpc_client: DuckRpcClient, q: queue.Queue):
        q.get()
        rpc_client.rpc("hello")
        q.task_done()



    def test_bench_thread_100(self):

        rpc_server: DuckRpcServer = build_rpc_server()
        rpc_server.start()
        rpc_client: DuckRpcClient = build_rpc_client()

        executor = ThreadPoolExecutor(max_workers=100)
        q = queue.Queue()
        n = 1000
        for i in range(n):
            q.put(i)
        start = time.time()
        for i in range(n):
            executor.submit(self.rpc_one, rpc_client,q)
        q.join()
        cost = time.time()-start
        logger.info(f"run {n} rpc cost {cost}, avg {cost/n}")
        rpc_client.shutdown()
        rpc_server.shutdown()


if __name__ == '__main__':
    unittest.main()
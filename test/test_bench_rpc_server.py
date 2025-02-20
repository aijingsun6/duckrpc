import queue
import socket
import unittest
import logging
import sys
import time
from concurrent.futures.thread import ThreadPoolExecutor


from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_rpc_server import DuckRpcServerConfig, DuckRpcServer
from duckrpc.duck_event_dispatch import DuckEventDispatchConfig
from duckrpc.duck_rpc_client import DuckRpcClientConfig, DuckRpcClient
from json_coder import JsonCoder
from echo_packet_handler import EchoHandler


logging.basicConfig(stream=sys.stdout,
                    level=logging.WARNING,
                    format="%(message)s")
logger = logging.getLogger(__name__)







class EchoSocketFactory(DuckSocketFactory):
    def create(self) -> socket.socket:
        return socket.socket()

    def destroy(self, sock: socket.socket) -> None:
        # logger.info(f"close {sock}")
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
        packet_dispatch_thread_size=packet_dispatch_thread_size,
        coder=JsonCoder(),
        socket_factory=EchoSocketFactory()
    )
    event_config = DuckEventDispatchConfig(select_timeout=0.2,
                                           dispatch_thread_size=event_dispatch_thread_size)
    return DuckRpcServer(config=config, event_config=event_config, handler=EchoHandler())


def build_rpc_client(event_dispatch_thread_size=4, packet_dispatch_thread_size=4, core_conn_size=1, max_conn_size=1):
    config: DuckRpcClientConfig = DuckRpcClientConfig(
        name="rpc_client",
        core_conn_size=core_conn_size,
        max_conn_size=max_conn_size,
        timeout_default=600,
        remote_addr=BIND_ADDR,
        remote_port=BIND_PORT,
        packet_dispatch_thread_size=packet_dispatch_thread_size,
        coder=JsonCoder(),
        socket_factory=DuckSocketFactory()
    )
    event_config = DuckEventDispatchConfig(select_timeout=0.1, dispatch_thread_size=event_dispatch_thread_size)
    return DuckRpcClient(config=config, event_config=event_config)


def rpc_one(rpc_client: DuckRpcClient, q: queue.Queue):
    q.get()
    start = time.time()
    rpc_client.rpc("hello")
    end = time.time()
    q.task_done()
    return end - start


def bench(event_thread_size, packet_thread_size, core_conn_size, max_conn_size, rpc_thread_size, total_req):
    rpc_server: DuckRpcServer = build_rpc_server(event_dispatch_thread_size=event_thread_size,
                                                 packet_dispatch_thread_size=packet_thread_size)
    rpc_server.start()
    rpc_client: DuckRpcClient = build_rpc_client(event_dispatch_thread_size=event_thread_size,
                                                 packet_dispatch_thread_size=packet_thread_size,
                                                 core_conn_size=core_conn_size,
                                                 max_conn_size=max_conn_size)

    executor = ThreadPoolExecutor(max_workers=rpc_thread_size)
    q = queue.Queue()
    n = total_req
    for i in range(n):
        q.put(i)
    start = time.time()
    fu_acc = []
    for i in range(n):
        f = executor.submit(rpc_one, rpc_client, q)
        fu_acc.append(f)

    q.join()


    cost = time.time() - start

    cost_max = None
    cost_min = None
    cost_total = 0
    for f in fu_acc:
        r = f.result()
        cost_total += r
        if cost_max is None:
            cost_max = r
        else:
            cost_max = max(cost_max, r)
        if cost_min is None:
            cost_min = r
        else:
            cost_min = min(cost_min, r)
    logger.warning("------ bench mark info -------")
    logger.warning(f"event:{event_thread_size}, packet:{packet_thread_size}, rpc:{rpc_thread_size}")
    logger.warning(f"core_conn_size:{core_conn_size}, max_conn_size: {max_conn_size}")
    logger.warning(f"total:{n}, qps: {n / cost}, avg:{cost_total / n}, max:{cost_max}, min:{cost_min}")
    executor.shutdown()
    rpc_client.shutdown()
    rpc_server.shutdown()


class BenchRpcServerTest(unittest.TestCase):

    def test_bench_4_4_1_1_4_100(self):
        bench(4, 4, 1, 1, 4, 100)

    def test_bench_4_4_1_1_4_1000(self):
        bench(4, 4, 1, 1, 4, 1000)

    def test_bench_4_4_1_1_4_10000(self):
        bench(4, 4, 1, 1, 4, 10000)

    def test_bench_4_4_1_1_8_1000(self):
        bench(4, 4, 1, 1, 8, 1000)

    def test_bench_4_4_4_4_8_1000(self):
        bench(4, 4, 4, 4, 8, 1000)

    def test_bench_4_4_4_4_8_10000(self):
        bench(4, 4, 4, 4, 8, 10000)

    def test_bench_8_8_8_8_8_10000(self):
        bench(8, 8, 8, 8, 8, 10000)

    def test_bench_16_16_16_16_16_10000(self):
        bench(16, 16, 16, 16, 16, 10000)


if __name__ == '__main__':
    unittest.main()

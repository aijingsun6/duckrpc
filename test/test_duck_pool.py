import queue
import threading
import time

from duckrpc.duck_factory import DuckFactory
from duckrpc.duck_pool import DuckPool, DuckPoolConfig

import unittest
from concurrent.futures import ThreadPoolExecutor

import logging
import sys

logging.basicConfig(stream=sys.stdout,
                    level=logging.DEBUG,
                    format="%(asctime)s %(name)s %(levelname)s %(threadName)s %(filename)s %(lineno)d %(message)s")
logger = logging.getLogger(__name__)


class DuckPoolFactoryTest(DuckFactory):
    index = 0

    def __init__(self):
        self.index = 0

    def create(self) -> str:
        r = "name-{}".format(self.index)
        self.index += 1
        return r

    def destroy(self, value: str) -> None:
        del value


class DuckPoolTest(unittest.TestCase):
    pool: DuckPool
    core_size: int = 4
    max_size: int = 8
    thread_pool: ThreadPoolExecutor

    def setUp(self):
        config = DuckPoolConfig(core_size=self.core_size, max_size=self.max_size)
        self.pool = DuckPool(config=config, factory=DuckPoolFactoryTest(), logger=logger)
        self.thread_pool = ThreadPoolExecutor()

    def check_in_delay(self, item: any):
        logger.info(f"check_in {item}")
        self.pool.check_in(item)

    def test_create_pool(self):
        self.assertEqual(self.pool._idle_queue.qsize(), self.core_size)
        self.assertEqual(len(self.pool._all_set), self.core_size)

    def test_check_out_core(self):
        for i in range(100):
            item = self.pool.check_out()
            expect = "name-{}".format(i % 4)
            self.assertEqual(expect, item)
            self.pool.check_in(item)

    def test_check_out_max(self):
        for i in range(self.max_size):
            item = self.pool.check_out()
            expect = "name-{}".format(i)
            self.assertEqual(expect, item)

        for i in range(10):
            with self.assertRaises(queue.Empty):
                item = self.pool.check_out(timeout=0.01)

    def test_check_out_delay(self):
        timer = None
        for i in range(self.max_size):
            item = self.pool.check_out()
            expect = "name-{}".format(i)
            self.assertEqual(expect, item)
            if i == (self.max_size - 1):
                timer = threading.Timer(2.7, self.check_in_delay,args=[item])
                timer.start()

        start = time.time()
        self.pool.check_out(timeout=3.0)
        timer.cancel()
        cost = time.time() - start
        logger.info("check_out cost {}".format(cost))
        self.assertTrue(cost < 3.0)

    def tearDown(self):
        self.pool.shutdown()


if __name__ == '__main__':
    unittest.main()

from abc import ABC, abstractmethod
import logging
import socket


class DuckFactory(ABC):
    @abstractmethod
    def create(self) -> any:
        raise NotImplementedError()

    @abstractmethod
    def destroy(self, value: any) -> None:
        raise NotImplementedError()


class DuckSocketFactory(DuckFactory):
    logger: logging.Logger

    def __init__(self, logger=None):
        if logger is None:
            self.logger = logging.getLogger(__name__)
        else:
            self.logger = logger

    def create(self) -> socket.socket:
        sock = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
        logging.debug(f"create socket {sock}")
        return sock

    def destroy(self, sock: socket.socket) -> None:
        logging.debug("destroy {sock}")
        sock.close()

from abc import ABC, abstractmethod
import json

from .duck_packet import DuckPacket


class DuckCoder(ABC):

    @abstractmethod
    def encode_packet(self, packet: DuckPacket) -> bytes:
        raise NotImplementedError()

    @abstractmethod
    def decode_packet(self, data: bytes) -> DuckPacket:
        raise NotImplementedError()


class DefaultDuckCoder(DuckCoder):

    def decode_packet(self, data: bytes) -> DuckPacket:
        packet = DuckPacket()
        packet.__dict__ = json.loads(data.decode(encoding="utf-8"))
        return packet

    def encode_packet(self, packet: DuckPacket) -> bytes:
        return json.dumps(packet.__dict__).encode(encoding="utf-8")

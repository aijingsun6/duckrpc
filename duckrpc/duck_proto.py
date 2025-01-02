import json
from dataclasses import dataclass

@dataclass
class DuckHeader(object):
    name:str
    local_addr: str
    local_port: int
    req_id: str
    trace_id: str
    body_size: int

    def encode(self)-> bytes:
        return json.dumps(self.__dict__).encode("utf-8")

    def decode(self, data: bytes):
        self.__dict__ = json.loads(data)




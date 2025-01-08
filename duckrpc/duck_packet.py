from dataclasses import dataclass


@dataclass
class DuckPacket(object):
    name: str
    iid: str
    body: any

    def __init__(self, name=None, iid=None, body=None):
        self.name = name
        self.iid = iid
        self.body = body

    def __str__(self):
        return f"{self.name}, {self.iid}, {self.body}"
       
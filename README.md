# duckrpc
duckrpc is a mini rpc framework for python

## 1. quick guide
### 1.1.1 DuckCoder
```python
from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_packet import DuckPacket

import json
class JsonCoder(DuckCoder):
    def encode_packet(self, packet: DuckPacket) -> bytes:
        return json.dumps(packet.__dict__).encode("utf-8")

    def decode_packet(self, data: bytes) -> DuckPacket:
        packet = DuckPacket()
        packet.__dict__ = json.loads(data.decode("utf-8"))
        return packet
```
### 1.1.2 DuckSocketFactory
```python
from duckrpc.duck_factory import DuckSocketFactory
import socket

class SimpleSocketFactory(DuckSocketFactory):
    def create(self) -> socket.socket:
        return socket.socket()

    def destroy(self, sock: socket.socket) -> None:
        sock.close()
```


### 1.1.3 start rpc server

```python
from duckrpc.duck_rpc_server import DuckRpcServerConfig, DuckRpcServer, DuckRpcPacketHandler
from duckrpc.duck_event_dispatch import DuckEventDispatchConfig
from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_factory import DuckSocketFactory


class EchoHandler(DuckRpcPacketHandler):

    def handle_body(self, body: any) -> any:
        return body


bind_addr = "127.0.0.1"
bind_port = ...
backlog = 10
packet_dispatch_thread_size = 4
event_dispatch_thread_size = 4
coder: DuckCoder = ...
socket_factory: DuckSocketFactory = ...
config = DuckRpcServerConfig(
    name="rpc-server",
    bind_addr=bind_addr,
    bind_port=bind_port,
    backlog=backlog,
    packet_dispatch_thread_size=packet_dispatch_thread_size,
    coder=coder,
    socket_factory=socket_factory
)
event_config = DuckEventDispatchConfig(select_timeout=0.2,
                                       dispatch_thread_size=event_dispatch_thread_size)
rpc_server = DuckRpcServer(config=config, event_config=event_config, handler=EchoHandler())
rpc_server.start()
```
### 1.1.4 start rpc client

```python
from duckrpc.duck_rpc_client import DuckRpcClientConfig, DuckRpcClient
from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_event_dispatch import DuckEventDispatchConfig

packet_dispatch_thread_size = 4
event_dispatch_thread_size = 4
coder: DuckCoder = ...
socket_factory: DuckSocketFactory = ...
remote_addr = ...
remote_port = ...
config: DuckRpcClientConfig = DuckRpcClientConfig(
    name="rpc_client",
    core_conn_size=1,
    max_conn_size=1,
    timeout_default=600,
    remote_addr=remote_addr,
    remote_port=remote_port,
    packet_dispatch_thread_size=packet_dispatch_thread_size,
    coder=coder,
    socket_factory=socket_factory
)
event_config = DuckEventDispatchConfig(select_timeout=0.1,
                                       dispatch_thread_size=event_dispatch_thread_size)
rpc_client = DuckRpcClient(config=config, event_config=event_config)
```
### 1.1.4 rpc call
```python
rpc_client = ...
req = ...
reply = rpc_client.rpc(req=...)
```

## 2. design
### 2.1 rpc server
```
| ----------- |              | -------------- |                       | ------------------------------------ |               
| select loop | - dispatch - | event-dispatch | - read data(socket) - | packet-dispatch(handle body & reply) |
| ----------- |              | -------------- |                       | ------------------------------------ |             

```

## 3. bench mark
see ``` test_bench_rpc_server.py ```
```
------ bench mark info -------
event:8, packet:8, rpc:8
core_conn_size:8, max_conn_size: 8
total:10000, qps: 1182.4083359553138, avg:0.00675000057220459, max:0.12443304061889648, min:0.0029528141021728516

```








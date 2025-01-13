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
from duckrpc.duck_rpc_server import DuckRpcServerConfig, DuckRpcServer, DuckRpcBodyHandler
from duckrpc.duck_socket_event import DuckSocketEventDispatchConfig, EventDispatchMode
from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_factory import DuckSocketFactory

class EchoHandler(DuckRpcBodyHandler):

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
event_config = DuckSocketEventDispatchConfig(select_timeout=0.2,
                                                 dispatch_thread_size=event_dispatch_thread_size,
                                                 dispatch_mode=EventDispatchMode.THREAD)
rpc_server = DuckRpcServer(config=config, event_config=event_config, handler=EchoHandler())
rpc_server.start()
```
### 1.1.4 start rpc client
```python
from duckrpc.duck_rpc_client import DuckRpcClientConfig, DuckRpcClient
from duckrpc.duck_coder import DuckCoder
from duckrpc.duck_factory import DuckSocketFactory
from duckrpc.duck_socket_event import DuckSocketEventDispatchConfig, EventDispatchMode

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
event_config = DuckSocketEventDispatchConfig(select_timeout=0.1, 
                                             dispatch_thread_size=event_dispatch_thread_size,
                                             dispatch_mode=EventDispatchMode.THREAD)
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
------ dispatch thread info -------
event:4, packet:4, rpc:4, num: 100
total:0.13124990463256836, qps: 761.9053155120235, avg:0.005098330974578858, max:0.009862899780273438, min:0.0025680065155029297
start rpc server, sock=<socket.socket fd=6, family=2, type=1, proto=0, laddr=('127.0.0.1', 30080)>, bind=('127.0.0.1', 30080)
 <socket.socket fd=10, family=2, type=1, proto=0, laddr=('0.0.0.0', 0)> connect remote 127.0.0.1  30080
------ dispatch thread info -------
event:4, packet:4, rpc:4, num: 1000
total:1.063072919845581, qps: 940.6692441617807, avg:0.004234939098358154, max:0.03983783721923828, min:0.002289295196533203
start rpc server, sock=<socket.socket fd=6, family=2, type=1, proto=0, laddr=('127.0.0.1', 30080)>, bind=('127.0.0.1', 30080)
 <socket.socket fd=10, family=2, type=1, proto=0, laddr=('0.0.0.0', 0)> connect remote 127.0.0.1  30080
------ dispatch thread info -------
event:4, packet:4, rpc:4, num: 10000
total:11.97170090675354, qps: 835.3031935803497, avg:0.004775405097007752, max:0.2888331413269043, min:0.0020918846130371094
start rpc server, sock=<socket.socket fd=6, family=2, type=1, proto=0, laddr=('127.0.0.1', 30080)>, bind=('127.0.0.1', 30080)
 <socket.socket fd=10, family=2, type=1, proto=0, laddr=('0.0.0.0', 0)> connect remote 127.0.0.1  30080
------ dispatch thread info -------
event:4, packet:4, rpc:8, num: 1000
total:0.8520209789276123, qps: 1173.6800204833453, avg:0.006767379999160767, max:0.028744935989379883, min:0.00479578971862793
start rpc server, sock=<socket.socket fd=6, family=2, type=1, proto=0, laddr=('127.0.0.1', 30080)>, bind=('127.0.0.1', 30080)
 <socket.socket fd=10, family=2, type=1, proto=0, laddr=('0.0.0.0', 0)> connect remote 127.0.0.1  30080
------ dispatch thread info -------
event:8, packet:8, rpc:8, num: 1000
total:0.8631699085235596, qps: 1158.5204606014202, avg:0.006850845098495483, max:0.027443885803222656, min:0.004606962203979492
start rpc server, sock=<socket.socket fd=7, family=2, type=1, proto=0, laddr=('127.0.0.1', 30080)>, bind=('127.0.0.1', 30080)
 <socket.socket fd=12, family=2, type=1, proto=0, laddr=('0.0.0.0', 0)> connect remote 127.0.0.1  30080
------ dispatch thread info -------
event:8, packet:8, rpc:8, num: 10000
total:8.435243844985962, qps: 1185.502183904755, avg:0.006735598921775818, max:0.12670493125915527, min:0.0047130584716796875

```








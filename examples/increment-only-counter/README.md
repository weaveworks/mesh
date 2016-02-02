# Increment-only counter

This example implements an in-memory key-value cache.
Keys are strings, and values are increment-only counters.
This is a state-based CRDT, so the write operation is set(key, value).

## Demo

Start several peers on the same host.
Tell the second and subsequent peers to connect to the first one.

```
$ ./increment-only-counter -hwaddr 00:00:00:00:00:01 -nickname a -mesh :6001 -http :8001 &
$ ./increment-only-counter -hwaddr 00:00:00:00:00:02 -nickname b -mesh :6002 -http :8002 -peer 127.0.0.1:6001 &
$ ./increment-only-counter -hwaddr 00:00:00:00:00:03 -nickname c -mesh :6003 -http :8003 -peer 127.0.0.1:6001 &
```

Set a value using the HTTP API of any peer.

```
$ curl -Ss -XPOST "http://localhost:8003/?key=a&value=123"
set(a, 123) => 123
```

Get the value from any other peer.

```
$ curl -Ss -XGET "http://localhost:8001/?key=a"
get(a) => 123
```

## Implementation

- [The state object](/examples/increment-only-counter/state.go) implements GossipData.
- [The peer object](/examples/increment-only-counter/peer.go) implements Gossiper.
- [The func main](/examples/increment-only-counter/main.go) wires the components together.

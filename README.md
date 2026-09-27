# phasync/net

> Formerly `phasync/server`, which stays available for 1.x.

TCP, UDP and Unix socket networking for PHP using phasync coroutines.

The API follows Go's `net` package: `listen()` gives you a `Listener` that hands you
connections, `dial()` connects, and `listenPacket()` gives you a UDP `PacketConn`. You start
one coroutine per connection, and connections are plain PHP stream resources, so every
stream function and library works with them.

## Installation

```bash
composer require phasync/net
```

For production, also see [the phasync extension](#the-phasync-extension) and
[JIT](#jit).

## Listening: `listen()`

```php
use function phasync\Net\listen;

phasync::run(function () {
    $listener = listen('0.0.0.0:8080');

    foreach ($listener as $peer => $conn) {
        phasync::go(function () use ($conn, $peer) {
            $request = fread(phasync::readable($conn), 65536);

            fwrite(phasync::writable($conn),
                "HTTP/1.1 200 OK\r\n" .
                "Connection: close\r\n" .
                "Content-Length: " . strlen("Hello, $peer!") . "\r\n" .
                "\r\n" .
                "Hello, $peer!"
            );
            fclose($conn);
        });
    }
});
```

Connections are plain non-blocking stream resources. Call `phasync::readable()` /
`phasync::writable()` before reading or writing, so the coroutine waits instead of getting
an empty read. This is also the fastest pattern for a server, with or without
[the phasync extension](#the-phasync-extension).

`foreach` over the listener accepts connections until it is closed (`$listener->close()`
from any coroutine). To take one connection at a time, as with Go's `Accept()`:

```php
[$conn, $peer] = $listener->accept();
```

Either way, a connection already waiting in the kernel's queue is taken without waiting for
the event loop, so a burst of new connections is admitted at once.

### Several addresses

A listener listens on one address. For several, run one accept loop per address:

```php
foreach (['0.0.0.0:80', '0.0.0.0:8080'] as $address) {
    phasync::go(function () use ($address) {
        foreach (listen($address) as $peer => $conn) {
            phasync::go(fn () => handle($conn, $peer));
        }
    });
}
```

To handle connections from all of them in one place, have each loop write to a shared
channel instead:

```php
phasync::channel($connections, $newConnection);

foreach (['0.0.0.0:80', '0.0.0.0:8080'] as $address) {
    phasync::go(function () use ($address, $newConnection) {
        foreach (listen($address) as $peer => $conn) {
            $newConnection->write([$peer, $conn]);
        }
    });
}

phasync::go(function () use ($connections) {
    foreach ($connections as [$peer, $conn]) {
        phasync::go(fn () => handle($conn, $peer));
    }
});
```

Read the channel from its own coroutine, as above, not from the coroutine that created it:
phasync currently treats a channel's creator waiting on it for more than 100 ms as a likely
deadlock and throws.

### Unix domain sockets

```php
$listener = listen('unix:///run/myapp.sock');
```

`close()` removes the socket file, so the path can be bound again after a restart.

### Port 0

Listening on port 0 picks a free port; `addr()` returns the real one:

```php
$listener = listen('127.0.0.1:0');
echo $listener->addr(); // 127.0.0.1:43127
```

### API

```php
phasync\Net\listen(string $address, array $context = []): Listener

$listener->accept(): array{resource, string}  // [connection, peer]; IOException once closed
foreach ($listener as $peer => $conn)         // until the listener is closed
$listener->close(): void
$listener->addr(): string
```

## Serving connections: `serve()` and `Duplex`

`serve()` is a server that hands out connections as `Duplex` objects instead of stream
resources. A `Duplex` reads and writes, waiting in the event loop, and behaves the same however
the bytes travel: this is the API for protocol code (an HTTP server, a WebSocket).

```php
use function phasync\Net\serve;

phasync::run(function () {
    foreach (serve('0.0.0.0:8080') as $peer => $conn) {
        phasync::go(function () use ($conn) {
            while ('' !== ($data = $conn->read())) {
                $conn->write($data); // echo
            }
            $conn->close();
        });
    }
});
```

With the [phasync extension](#the-phasync-extension)'s `tcp_server()`, a TCP server is **one
stream** for the listening socket and all its connections: the event loop waits on one file
descriptor however many connections are open, and one read collects new connections and data
from many clients. Without it (and for Unix sockets) each connection is a socket of its own.
Connections behave the same either way; `Server::isMultiplexed()` says which it is.

| `Duplex` | |
|---|---|
| `read(int $max = 65536, ?float $timeout = null): string` | The next bytes, waiting for them; `''` only at the end (the peer finished, or the connection closed) |
| `write(string $bytes, ?float $timeout = null): void` | All of `$bytes`; writes from several coroutines never interleave; waits while the peer is slow |
| `eof(): bool` | The peer finished sending and everything was read |
| `end(): void` | Finish our side; we can still read |
| `close(): void` | Close once what was written is sent; waiting reads end with `''`, writes throw `IOException` |
| `peer()`, `local()` | The two ends' addresses |

`serve()` options: `backlog`, `reuseport` (default true), `nodelay` (default true),
`max_connections` (new connections wait in the kernel's backlog meanwhile; `isFull()`), and with
the extension `read_chunk` and `high_water`. A slow reader pauses its client: data it hasn't read
stays in the client's socket.

`new StreamDuplex($stream)` makes any stream resource a `Duplex`, such as a `dial()` connection.

## Connecting: `dial()`

Connect without blocking other coroutines, including the DNS lookup and a TLS handshake:

```php
use function phasync\Net\dial;

phasync::run(function () {
    $conn = dial('example.com:80');

    fwrite(phasync::writable($conn), "GET / HTTP/1.0\r\nHost: example.com\r\n\r\n");
    while (!feof($conn)) {
        echo fread(phasync::readable($conn), 65536);
    }
    fclose($conn);
});
```

```php
dial('tls://example.com:443');   // TLS; the certificate is checked against the host name
dial('unix:///run/app.sock');    // Unix domain socket
dial('[::1]:8080', timeout: 5);  // the timeout covers lookup, connect and TLS handshake
```

```php
phasync\Net\dial(string $address, float $timeout = 30, array $context = []): resource
```

## UDP: `listenPacket()`

```php
use function phasync\Net\listenPacket;

phasync::run(function () {
    $conn = listenPacket('0.0.0.0:9000');

    foreach ($conn as $peer => $data) {
        $conn->writeTo("Echo: $data", $peer);
    }
});
```

A `PacketConn` is one UDP socket, for a server or a client (bind port 0 for a client).
Datagrams already waiting are taken before waiting again. For several addresses, run one
loop per address as with `listen()`.

### API

```php
phasync\Net\listenPacket(string $address, array $context = []): PacketConn

$conn->readFrom(int $maxLength = 65536): array{string, string}  // [datagram, peer]; IOException once closed
foreach ($conn as $peer => $data)                               // until the socket is closed
$conn->writeTo(string $data, string $address): int|false
$conn->close(): void
$conn->addr(): string
```

## Dns

Async DNS resolution (used automatically by `dial()`):

```php
use phasync\Net\Dns;
use phasync\Net\DnsRecordType;

phasync::run(function () {
    // Default: tries A (IPv4) first, then AAAA (IPv6).
    // Returns a random IP when there are several records.
    $ip = Dns::resolve('example.com');

    $allIps = Dns::resolveAll('example.com', DnsRecordType::A);
    $ipv4   = Dns::resolve('example.com', DnsRecordType::A);
    $ipv6   = Dns::resolve('example.com', DnsRecordType::AAAA);
});
```

- Non-blocking UDP queries to the system nameserver
- IPv4 (A) and IPv6 (AAAA)
- Reads `/etc/hosts` first
- Caches results using the DNS TTL
- Falls back to 8.8.8.8 if no nameserver is configured

```php
Dns::resolve(string $hostname, DnsRecordType $type = DnsRecordType::ANY, float $timeout = 2.0): ?string
Dns::resolveAll(string $hostname, DnsRecordType $type = DnsRecordType::ANY, float $timeout = 2.0): array
Dns::clearCache(): void
```

## Socket options

| Option | Default | Why |
|--------|---------|-----|
| `backlog` | `65535` | Room for connection bursts. The kernel caps it at its own limit (`net.core.somaxconn` on Linux), so a high value is safe. |
| `so_reuseport` | `true` | Several processes can listen on the same port, for multi-process scaling and graceful restarts. |
| `tcp_nodelay` | `true` | Small responses are sent immediately instead of waiting to be batched. |

Override them with `$context`:

```php
$listener = listen('0.0.0.0:8080', [
    'socket' => ['tcp_nodelay' => false],
]);
```

## Production

### The phasync extension

phasync waits for sockets with `stream_select()`, which in PHP fails for any file descriptor
numbered 1024 or higher. A server with more than roughly 1,000 open connections hits that.
The [phasync extension](https://github.com/phasync/phasync-ext) replaces it with a version
without that limit. It is CLI only.

Code written for phasync, like the examples here, works the same with the extension. Its
other job is making ordinary blocking code cooperate: inside `phasync::run()`, a library's
`fread()` / `fwrite()` on a blocking stream, file reads, DNS lookups and `usleep()` suspend
the coroutine instead of stalling every connection. I/O outside PHP's streams, such as curl
or a database client library, still blocks.

```bash
composer require phasync/phasync-ext
```

```php
phasync\try_enable_ext(); // first line of your script; may restart the process once
```

### JIT

PHP's JIT is off by default on the command line. For a server, turn it on:

```bash
php -d opcache.enable_cli=1 -d opcache.jit=tracing -d opcache.jit_buffer_size=128M server.php
```

In the benchmark below it added 8–18% throughput.

## Benchmark

Hello-world keep-alive HTTP responder on raw sockets (no HTTP library), one process each:
phasync/net 2.0.0-alpha1 with phasync-ext 0.4.0-alpha11 and JIT on PHP 8.3, and node v18 with the `net` module.
`wrk -t8 -d10s` on a Ryzen 9 9950X3D:

| Connections | phasync req/s | node req/s | phasync p99 | node p99 |
|---|---|---|---|---|
| 100 | 254k | 262k | 0.47 ms | 0.50 ms |
| 900 | 230k | 252k | 5.0 ms | 376 ms |
| 10,000 | 105k | 155k* | 98 ms | 30 ms* |

\* At 10,000 connections node had only accepted about 8,350 of them after 5 seconds, with
287 timeouts, so it was serving fewer clients. phasync had all 10,000 connections accepted
with no errors.

This handler does almost nothing per request, which favours node: its event loop runs in C
while phasync's runs in PHP. With more work per request, phasync's per-request overhead is
lower: waiting on I/O suspends a fiber, where node allocates promises for every `await`.

The scripts are in [`benchmarking/`](benchmarking/).

## Upgrading from 1.x

The package is renamed from `phasync/server` to `phasync/net`:

```bash
composer remove phasync/server
composer require phasync/net
```

2.0 requires phasync 2.0 and has a new API, modelled on Go's `net` package:

| 1.x | 2.0 |
|---|---|
| `Server::serve($address, $handler)` | `foreach (listen($address) as $peer => $conn)`, starting a coroutine per connection |
| `new TcpServer([...addresses])`, `foreach ($server->accept() ...)` | one `listen()` per address, `foreach ($listener ...)`; see [Several addresses](#several-addresses) |
| `TcpServer::getAddresses(): array` | `$listener->addr(): string` |
| `isClosed()` | gone: `accept()` throws `IOException` once the listener is closed |
| `TcpClient::connect()` / `connectUnix()` | `dial('host:port')` / `dial('unix:///path')` |
| `UdpServer::receive()` yielding `peer => [data, socket]` | `foreach (listenPacket($address) as $peer => $data)`; reply with `$conn->writeTo($data, $peer)` |

```php
// 1.x
Server::serve('tcp://127.0.0.1:8080', function ($stream, $peer) {
    // handle connection
});

// 2.0
foreach (listen('127.0.0.1:8080') as $peer => $conn) {
    phasync::go(fn () => /* handle connection */);
}
```

Connections are no longer wrapped in AsyncStream: they are plain non-blocking streams. Wait
with `phasync::readable()` / `phasync::writable()` before reading or writing. The
`$wrapStreams`, `$readBuffer` and `$writeBuffer` options are gone.

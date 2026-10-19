# Changelog

## 2.0.0 (2026-10-19)

Breaking changes, preparing phasync/net for release. No compatibility shims: upgrade in one
pass.

- **`SO_REUSEPORT` is off by default everywhere** (`listen()`, `serve()`/`Server`,
  `listenPacket()`/`PacketConn`). A server that runs in several processes or workers sharing
  one port must now opt in: pass `so_reuseport => true`. Previously `listen()`/`serve()`
  defaulted it on (`listenPacket()` always enabled it); a forgotten second process could
  silently end up sharing a port it meant to own exclusively. This matches Go, Node, tokio,
  nginx, Swoole, ReactPHP and Amp, which all default it off.
- **One flat option shape, PHP's own stream-context names, for every function.**
  `listen()`/`Listener` took a nested stream context (`['socket' => ['so_reuseport' => …,
  'tcp_nodelay' => …, 'backlog' => …]]`); `serve()`/`Server` took flat but differently-named
  options (`reuseport`, `nodelay`, `backlog`, `keepalive`, `max_connections`). All four
  functions — `listen()`, `serve()`, `listenPacket()` and `dial()` — now take the same flat
  array with PHP's own names: `so_reuseport`, `tcp_nodelay`, `backlog`, plus phasync/net's own
  `keepalive` and `max_connections` (`serve()` only). Anything not named above (such as `ssl`
  options for TLS) goes under a `context` key, passed through to the stream context as-is.
  Example: `listen($addr, ['socket' => ['so_reuseport' => true]])` is now
  `listen($addr, ['so_reuseport' => true])`; `serve($addr, ['reuseport' => true])` is now
  `serve($addr, ['so_reuseport' => true])`.
- **Unknown option names are rejected**, with `InvalidArgumentException` naming the bad key and
  listing the valid ones, instead of being silently ignored. This also means passing the *old*
  nested `['socket' => [...]]` shape, or `serve()`'s old `reuseport`/`nodelay` names, now throws
  rather than doing nothing.
- **`dial()` returns a `Duplex`** (a `StreamDuplex`, the same type `serve()` hands out) instead
  of a raw stream resource, so client and server connections share the same read/write API, as
  Go's `net.Conn`, Node's `net.Socket` and tokio's `TcpStream` do. Code calling `fread()`/
  `fwrite()`/`fclose()`/`feof()` on a `dial()` result must switch to `$conn->read()`,
  `$conn->write()`, `$conn->close()`, `$conn->eof()`. `dial()`'s third parameter is renamed
  `$options`, same shape as above (`tcp_nodelay`, `context`); it no longer accepts a raw stream
  context directly.
- **The `backlog` default changed** from a flat `65535` to `net.core.somaxconn` on Linux (read
  once per process, capped at 65535 — older kernels store the backlog in a 16-bit field, so a
  larger value wraps), falling back to `511` where that file cannot be read (macOS, BSD,
  Windows). An explicit `backlog` option always wins.

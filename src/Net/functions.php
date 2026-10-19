<?php

namespace phasync\Net;

use phasync;

/**
 * Listen for connections on a TCP or Unix domain socket, like Go's net.Listen().
 *
 * @param string $address Address to listen on, such as '0.0.0.0:8080', '[::]:8080' or
 *                         'unix:///run/app.sock'. Port 0 picks a free port; see
 *                         Listener::addr().
 * @param array  $options so_reuseport (default false: opt in for several processes to share
 *                         the port, for multi-process scaling and graceful restarts), tcp_nodelay
 *                         (default true), backlog (default: net.core.somaxconn on Linux, capped
 *                         at 65535; else 511), keepalive (seconds, default 15; 0 or false: off;
 *                         needs ext-sockets and is otherwise not applied) — the last three don't
 *                         apply to a Unix socket. `context` escapes to raw stream context
 *                         options (such as 'ssl') for anything not named above.
 *
 * @throws \RuntimeException if the address cannot be bound
 */
function listen(string $address, array $options = []): Listener
{
    return new Listener($address, $options);
}

/**
 * Serve TCP (or Unix socket) connections as Duplex objects: see Server.
 *
 * @param string|Listener $address Such as '0.0.0.0:8080', '[::]:8080' or 'unix:///run/app.sock'
 *                                 (port 0 picks a free port; see Server::addr()), or a Listener
 *                                 to serve, such as Listener::fromStream($socket)
 * @param array           $options so_reuseport (default false), tcp_nodelay (default true),
 *                                 backlog (default: net.core.somaxconn on Linux, capped at
 *                                 65535; else 511), keepalive (seconds, default 15; 0 or false:
 *                                 off), max_connections (0: no limit), context (raw stream
 *                                 context options, such as 'ssl'). Not used with a Listener,
 *                                 except max_connections.
 *
 * @throws \RuntimeException if the address cannot be bound
 */
function serve(string|Listener $address, array $options = []): Server
{
    return new Server($address, $options);
}

/**
 * Bind a UDP socket, like Go's net.ListenPacket().
 *
 * @param string $address Address to bind, such as '0.0.0.0:9000'. Port 0 picks a free port;
 *                        see PacketConn::addr().
 * @param array  $options so_reuseport (default false). `context` escapes to raw stream context
 *                        options for anything not named above.
 *
 * @throws \RuntimeException if the address cannot be bound
 */
function listenPacket(string $address, array $options = []): PacketConn
{
    return new PacketConn($address, $options);
}

/**
 * Connect to an address, like Go's net.Dial(), without blocking other coroutines: the DNS
 * lookup, the connect and a TLS handshake all wait in the event loop.
 *
 * @param string $address Such as 'example.com:80', 'tcp://[::1]:8080', 'unix:///run/app.sock'
 *                        or 'tls://example.com:443' (the certificate is checked against the
 *                        host name)
 * @param float  $timeout Seconds for all of it: lookup, connect and TLS handshake
 * @param array  $context Stream context options. Default for TCP and TLS: tcp_nodelay.
 *
 * @return resource a connected, non-blocking stream
 *
 * @throws \RuntimeException    when the lookup, the connection or the TLS handshake fails
 * @throws phasync\TimeoutException when it takes longer than $timeout
 */
function dial(string $address, float $timeout = 30, array $context = []): mixed
{
    if (!\str_contains($address, '://')) {
        $address = 'tcp://' . $address;
    }
    $scheme    = \strstr($address, '://', true);
    $tls       = 'tls' === $scheme || 'ssl' === $scheme;
    $deadline  = \microtime(true) + $timeout;
    $remaining = static fn (): float => \max(0.0, $deadline - \microtime(true));

    $target = $address;
    if ('unix' !== $scheme) {
        $parts = \parse_url($address);
        $host  = $parts['host'] ?? throw new \InvalidArgumentException("No host in $address");
        $port  = $parts['port'] ?? throw new \InvalidArgumentException("No port in $address");
        $ip    = \trim($host, '[]');
        if (false === \filter_var($ip, \FILTER_VALIDATE_IP)) {
            $ip = Dns::resolve($host, DnsRecordType::ANY, $remaining())
                ?? throw new \RuntimeException("DNS lookup failed for $host");
        }
        // Connect as TCP and do the TLS handshake after, so it can wait without blocking
        $target = ($tls ? 'tcp' : $scheme) . '://' . (\str_contains($ip, ':') ? "[$ip]" : $ip) . ':' . $port;
        $context['socket']['tcp_nodelay'] ??= true;
        if ($tls) {
            $context['ssl']['peer_name'] ??= \trim($host, '[]');
        }
    }

    $socket = @\stream_socket_client(
        $target,
        $errno,
        $errstr,
        $remaining(),
        \STREAM_CLIENT_CONNECT | \STREAM_CLIENT_ASYNC_CONNECT,
        \stream_context_create($context)
    );
    if (!$socket) {
        throw new \RuntimeException("Connection failed to $address: $errstr", $errno);
    }
    \stream_set_blocking($socket, false);

    try {
        // Writable once the handshake completes or fails; the peer name tells which
        phasync::writable($socket, $remaining());
        if (false === @\stream_socket_get_name($socket, true)) {
            throw new \RuntimeException("Connection refused or reset: $address");
        }
        if ($tls) {
            // A non-blocking handshake returns 0 while it waits for the server
            while (0 === ($done = @\stream_socket_enable_crypto($socket, true, \STREAM_CRYPTO_METHOD_TLS_CLIENT))) {
                phasync::readable($socket, $remaining());
            }
            if (true !== $done) {
                throw new \RuntimeException("TLS handshake failed with $address: " . (\error_get_last()['message'] ?? 'unknown error'));
            }
        }
    } catch (\Throwable $e) {
        \fclose($socket);
        throw $e;
    }

    return $socket;
}

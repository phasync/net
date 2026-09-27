<?php

namespace phasync\Net;

use Generator;
use IteratorAggregate;
use phasync;
use phasync\IOException;

/**
 * A listening TCP or Unix domain socket, like Go's net.Listener. Create one with listen().
 *
 * One listener is one socket. To listen on several addresses, create one listener per
 * address and run each accept loop in its own coroutine; to handle all of them in one
 * place, have each loop write its connections to a shared channel.
 *
 * Connections are plain non-blocking stream resources: wait with phasync::readable() /
 * phasync::writable() before reading or writing.
 *
 * ```php
 * phasync::run(function () {
 *     $listener = phasync\Net\listen('0.0.0.0:8080');
 *     foreach ($listener as $peer => $conn) {
 *         phasync::go(function () use ($conn) {
 *             $request = fread(phasync::readable($conn), 65536);
 *             fwrite(phasync::writable($conn), "HTTP/1.0 200 OK\r\n\r\nHello\n");
 *             fclose($conn);
 *         });
 *     }
 * });
 * ```
 *
 * @implements IteratorAggregate<string, resource>
 */
final class Listener implements IteratorAggregate
{
    /** @var resource|null */
    private $socket;

    private string $address;

    /** Path of the socket file for a unix:// listener, removed again on close(). */
    private ?string $unixPath = null;

    /**
     * @see listen()
     *
     * @throws \RuntimeException if the address cannot be bound
     */
    public function __construct(string $address, array $context = [])
    {
        if (!\str_contains($address, '://')) {
            $address = 'tcp://' . $address;
        }
        $protocol = \strstr($address, '://', true);
        $context  = self::applyDefaultContext($context, $protocol);

        $warning = null;
        \set_error_handler(static function (int $code, string $message) use (&$warning): bool {
            $warning = $message;

            return true;
        });
        try {
            $socket = 'tcp' === $protocol ? self::closeOnExec($address, $context) : null;
            $socket ??= \stream_socket_server(
                $address,
                $errno,
                $errstr,
                \STREAM_SERVER_BIND | \STREAM_SERVER_LISTEN,
                \stream_context_create($context)
            );
        } finally {
            \restore_error_handler();
        }
        if (!$socket) {
            throw new \RuntimeException("Failed to bind to $address: " . ($errstr ?: $warning ?? 'unknown error'), $errno);
        }

        \stream_set_blocking($socket, false);
        $this->socket  = $socket;
        $this->address = (string) \stream_socket_get_name($socket, false);
        if ('unix' === $protocol) {
            $this->unixPath = \substr($address, \strlen('unix://'));
        }
    }

    /**
     * Wait for the next connection and return it, like Go's Listener.Accept(). A connection
     * already waiting in the kernel's queue is returned without waiting for the event loop,
     * so a burst of connections is admitted at once.
     *
     * @return array{0: resource, 1: string} the connection stream and the peer address
     *
     * @throws IOException when the listener is closed, also while waiting
     */
    public function accept(): array
    {
        while (\is_resource($this->socket)) {
            if (!$this->hasPendingConnection()) {
                try {
                    phasync::readable($this->socket, \PHP_FLOAT_MAX);
                } catch (IOException $e) {
                    if (\is_resource($this->socket)) {
                        throw $e;
                    }
                    break; // closed while waiting
                }
                if (!\is_resource($this->socket)) {
                    break;
                }
            }

            // Can still fail when several processes share this socket (so_reuseport) and
            // another one took the connection first; or for lack of file descriptors, while
            // the connection keeps waiting: then wait a little instead of trying again at once
            $stream = @\stream_socket_accept($this->socket, 0, $peer);
            if (false === $stream) {
                if ($this->hasPendingConnection()) {
                    phasync::sleep(0.1);
                }
                continue;
            }
            \stream_set_blocking($stream, false);

            return [$stream, (string) $peer];
        }

        throw new IOException('The listener is closed');
    }

    /**
     * Accept connections in a foreach loop: peer address => connection stream. The loop ends
     * when the listener is closed.
     *
     * @return Generator<string, resource>
     */
    public function getIterator(): Generator
    {
        while (\is_resource($this->socket)) {
            try {
                [$stream, $peer] = $this->accept();
            } catch (IOException $e) {
                if (\is_resource($this->socket)) {
                    throw $e;
                }

                return;
            }
            yield $peer => $stream;
        }
    }

    /**
     * Stop listening. A coroutine waiting in accept() gets IOException, and a foreach loop
     * over the listener ends. A Unix socket's file is removed.
     */
    public function close(): void
    {
        if (\is_resource($this->socket)) {
            \fclose($this->socket);
        }
        $this->socket = null;
        if (null !== $this->unixPath && \file_exists($this->unixPath)) {
            @\unlink($this->unixPath);
        }
        $this->unixPath = null;
    }

    /**
     * The address the listener is bound to, with the real port when it listened on port 0
     * (for example '127.0.0.1:43127'), or the socket path for a Unix socket.
     */
    public function addr(): string
    {
        return $this->address;
    }

    /**
     * Wait until a connection waits to be accepted, without accepting it.
     *
     * @throws IOException when the listener is closed
     * @throws phasync\TimeoutException
     */
    public function awaitPending(float $timeout = \PHP_FLOAT_MAX): void
    {
        while (\is_resource($this->socket) && !$this->hasPendingConnection()) {
            phasync::readable($this->socket, $timeout);
        }
        if (!\is_resource($this->socket)) {
            throw new IOException('The listener is closed');
        }
    }

    /**
     * Stop listening, but hand out the connections already waiting in the kernel's queue first:
     * accept() returns them, then throws IOException. The socket leaves its SO_REUSEPORT group
     * at once, so the kernel gives new connections to the other listeners.
     *
     * @return list<array{0: resource, 1: string}> the connections that were waiting
     */
    public function drain(): array
    {
        $waiting = [];
        while (\is_resource($this->socket) && $this->hasPendingConnection() && false !== ($stream = @\stream_socket_accept($this->socket, 0, $peer))) {
            \stream_set_blocking($stream, false);
            $waiting[] = [$stream, (string) $peer];
        }
        $this->close();

        return $waiting;
    }

    /**
     * Whether a connection is waiting in the accept queue right now, without blocking.
     * Checking first avoids calling stream_socket_accept() on an empty queue, which always
     * emits a warning. Uses the phasync extension's stream_select() when loaded, since the
     * native one fails for file descriptor numbers at or above FD_SETSIZE.
     */
    private function hasPendingConnection(): bool
    {
        $read   = [$this->socket];
        $write  = null;
        $except = null;
        $ready  = \function_exists('phasync\ext\stream_select')
            ? \phasync\ext\stream_select($read, $write, $except, 0, 0)
            : @\stream_select($read, $write, $except, 0, 0);

        return $ready > 0;
    }

    /**
     * A close-on-exec TCP listener, so that processes the application starts (exec(),
     * proc_open(), mail()) don't inherit it: an inherited listener stays in its SO_REUSEPORT
     * group after this process ended, and the kernel keeps giving it connections nobody
     * accepts. PHP's streams are never close-on-exec; the sockets extension's are with
     * SOCK_CLOEXEC. Null when that is not available, or the context asks for more than
     * backlog, so_reuseport and tcp_nodelay.
     *
     * @return resource|null
     */
    private static function closeOnExec(string $address, array $context): mixed
    {
        if (!\defined('SOCK_CLOEXEC') || \array_diff(\array_keys($context), ['socket']) || \array_diff(\array_keys($context['socket'] ?? []), ['backlog', 'so_reuseport', 'tcp_nodelay'])) {
            return null;
        }
        if (!\preg_match('/^tcp:\/\/\[?(.*?)\]?:(\d+)$/D', $address, $m) || false === \filter_var($m[1], \FILTER_VALIDATE_IP) || (int) $m[2] > 65535) {
            return null; // socket_bind() takes an IP address only: stream_socket_server() says what's wrong
        }
        $socket = \socket_create(\str_contains($m[1], ':') ? \AF_INET6 : \AF_INET, \SOCK_STREAM | \SOCK_CLOEXEC, \SOL_TCP);
        if (false === $socket) {
            return null;
        }
        // SO_REUSEADDR as stream_socket_server() sets it: connections in TIME_WAIT don't block a restart
        if (!@\socket_set_option($socket, \SOL_SOCKET, \SO_REUSEADDR, 1)
            || (($context['socket']['so_reuseport'] ?? false) && !@\socket_set_option($socket, \SOL_SOCKET, \SO_REUSEPORT, 1))
            || !@\socket_bind($socket, $m[1], (int) $m[2]) || !@\socket_listen($socket, (int) ($context['socket']['backlog'] ?? 128))) {
            $errno = \socket_last_error($socket);
            \socket_close($socket);
            throw new \RuntimeException("Failed to bind to $address: " . \socket_strerror($errno), $errno);
        }
        $stream = \socket_export_stream($socket);
        if ($context['socket']['tcp_nodelay'] ?? false) {
            // Read by stream_socket_accept() from the listener's context
            \stream_context_set_option($stream, 'socket', 'tcp_nodelay', true);
        }

        return $stream;
    }

    private static function applyDefaultContext(array $context, string $protocol): array
    {
        $context['socket']['backlog'] ??= 65535;

        if ('unix' === $protocol) {
            // Unix sockets don't support these options
            unset($context['socket']['so_reuseport'], $context['socket']['tcp_nodelay']);
        } else {
            $context['socket']['so_reuseport'] ??= true;
            $context['socket']['tcp_nodelay']  ??= true;
        }

        return $context;
    }
}

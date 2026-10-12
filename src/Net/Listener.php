<?php

namespace phasync\Net;

use Generator;
use IteratorAggregate;
use phasync;
use phasync\IOException;

/**
 * A listening TCP or Unix domain socket, like Go's net.Listener. Create one with listen(), or
 * wrap a socket that is already listening with fromStream().
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
    /**
     * Connections accepted from the queue without waiting, after which accept() lets the event
     * loop run once: while clients keep connecting, the queue never empties, and the other
     * coroutines (the connections' own waits, timers, garbage collection) must still get turns.
     */
    public const ACCEPT_BURST = 64;

    /** @var resource|null */
    private $socket;

    /** The listener as ext-sockets' object, when it was created with it: connections are accepted with it, see closeOnExec(). */
    private ?\Socket $sock = null;

    private string $address;

    /** Path of the socket file for a unix:// listener this listener created, removed again on close(). */
    private ?string $unixPath = null;

    /** A Unix socket's clients have no address. */
    private bool $unix = false;

    /** Connections accepted since accept() last waited or let the loop run, see ACCEPT_BURST. */
    private int $burst = 0;

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
            $socket = $this->closeOnExec($address, $protocol, $context);
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

        // A stream_socket_server() listener: PHP's socket context has no keepalive option
        if (null === $this->sock && 'tcp' === $protocol && \function_exists('socket_import_stream')
            && ($keepalive = (int) ($context['socket']['keepalive'] ?? 0)) > 0
            && !self::setKeepalive(\socket_import_stream($socket), $keepalive)) {
            \fclose($socket);
            throw new \RuntimeException("Failed to set keepalive on $address");
        }

        \stream_set_blocking($socket, false);
        $this->socket  = $socket;
        $this->address = (string) \stream_socket_get_name($socket, false);
        if ($this->unix = 'unix' === $protocol) {
            $this->unixPath = \substr($address, \strlen('unix://'));
        }
    }

    /**
     * Accept from a socket that is already listening and that this process did not create: one
     * the application made before forking its workers, which all accept from it. The socket is
     * made non-blocking. Whoever created it owns it: close() closes only this process's
     * descriptor, and never removes a Unix socket's file. Options set on the socket, such as
     * keepalive, are those it was created with.
     *
     * @param resource $stream a listening stream socket, TCP or Unix
     */
    public static function fromStream($stream): self
    {
        $listener = (new \ReflectionClass(self::class))->newInstanceWithoutConstructor();
        \stream_set_blocking($stream, false);
        $listener->socket  = $stream;
        $listener->sock    = \function_exists('socket_import_stream') ? \socket_import_stream($stream) : null;
        $listener->address = (string) \stream_socket_get_name($stream, false);
        $listener->unix    = \str_starts_with(\stream_get_meta_data($stream)['stream_type'], 'unix');

        return $listener;
    }

    /**
     * Wait for the next connection and return it, like Go's Listener.Accept(). A connection
     * already waiting in the kernel's queue is returned without waiting for the event loop,
     * so a burst of connections is admitted at once, up to ACCEPT_BURST of them.
     *
     * @param string|null $peer set to the peer address, like stream_socket_accept()
     *
     * @return resource the connection stream
     *
     * @throws IOException when the listener is closed, also while waiting
     */
    public function accept(?string &$peer = null)
    {
        while (\is_resource($this->socket)) {
            if ($this->hasPendingConnection()) {
                if (++$this->burst > self::ACCEPT_BURST) {
                    $this->burst = 0;
                    phasync::sleep(0);
                    continue;
                }
            } else {
                $this->burst = 0;
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
            $stream = $this->acceptNow($peer);
            if (false === $stream) {
                if ($this->hasPendingConnection()) {
                    phasync::sleep(0.1);
                }
                continue;
            }

            return $stream;
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
                $stream = $this->accept($peer);
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
     * over the listener ends. The file of a Unix socket this listener created is removed.
     */
    public function close(): void
    {
        if (\is_resource($this->socket)) {
            \fclose($this->socket);
        }
        $this->socket = $this->sock = null;
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
        while (\is_resource($this->socket) && $this->hasPendingConnection() && false !== ($stream = $this->acceptNow($peer))) {
            $waiting[] = [$stream, $peer];
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
     * Take a connection from the queue, or false when there is none (or accepting failed).
     * With ext-sockets, the connection is close-on-exec, see closeOnExec().
     *
     * @return resource|false a non-blocking stream
     */
    private function acceptNow(?string &$peer): mixed
    {
        if (null === $this->sock) {
            $stream = @\stream_socket_accept($this->socket, 0, $peer);
            if (false !== $stream) {
                \stream_set_blocking($stream, false);
            }
            $peer = (string) $peer;

            return $stream;
        }
        $accepted = @\socket_accept($this->sock);
        if (false === $accepted) {
            return false;
        }
        \socket_set_nonblock($accepted);
        $peer = '';
        if (!$this->unix) {
            \socket_getpeername($accepted, $ip, $port);
            $peer = (\str_contains($ip, ':') ? "[$ip]" : $ip) . ":$port";
        }

        return \socket_export_stream($accepted);
    }

    /**
     * A close-on-exec listener, so that processes the application starts (exec(), proc_open(),
     * mail()) don't inherit it, nor the connections accepted from it (accepted with
     * socket_accept(), see acceptNow()). An inherited TCP listener stays in its SO_REUSEPORT
     * group after this process ended, and the kernel keeps giving it connections nobody
     * accepts; an inherited connection stays open after we closed it. PHP's streams are never
     * close-on-exec; the sockets extension's are with SOCK_CLOEXEC. Null when that is not
     * available, or the context asks for more than backlog, so_reuseport, tcp_nodelay and keepalive.
     *
     * @return resource|null
     */
    private function closeOnExec(string $address, string $protocol, array $context): mixed
    {
        if (!\defined('SOCK_CLOEXEC') || \array_diff(\array_keys($context), ['socket']) || \array_diff(\array_keys($context['socket'] ?? []), ['backlog', 'so_reuseport', 'tcp_nodelay', 'keepalive'])) {
            return null;
        }
        if ('unix' === $protocol) {
            $host   = \substr($address, \strlen('unix://'));
            $port   = 0;
            $socket = \socket_create(\AF_UNIX, \SOCK_STREAM | \SOCK_CLOEXEC, 0);
        } elseif ('tcp' === $protocol && \preg_match('/^tcp:\/\/\[?(.*?)\]?:(\d+)$/D', $address, $m) && false !== \filter_var($m[1], \FILTER_VALIDATE_IP) && (int) $m[2] <= 65535) {
            [, $host, $port] = $m;
            $socket          = \socket_create(\str_contains($host, ':') ? \AF_INET6 : \AF_INET, \SOCK_STREAM | \SOCK_CLOEXEC, \SOL_TCP);
        } else {
            return null; // socket_bind() takes an IP address only: stream_socket_server() says what's wrong
        }
        if (false === $socket) {
            return null;
        }
        // SO_REUSEADDR as stream_socket_server() sets it: connections in TIME_WAIT don't block a restart
        if ('unix' !== $protocol && (!@\socket_set_option($socket, \SOL_SOCKET, \SO_REUSEADDR, 1)
            || (($context['socket']['so_reuseport'] ?? false) && !@\socket_set_option($socket, \SOL_SOCKET, \SO_REUSEPORT, 1))
            // Linux gives the listener's TCP_NODELAY to the connections it accepts
            || (($context['socket']['tcp_nodelay'] ?? false) && !@\socket_set_option($socket, \SOL_TCP, \TCP_NODELAY, 1))
            // so does its keepalive: first probe after $keepalive seconds idle, then every $keepalive, dropped after 9 unanswered
            || (($keepalive = (int) ($context['socket']['keepalive'] ?? 0)) > 0 && !self::setKeepalive($socket, $keepalive)))
            || !@\socket_bind($socket, $host, (int) $port) || !@\socket_listen($socket, (int) ($context['socket']['backlog'] ?? 128))) {
            $errno = \socket_last_error($socket);
            \socket_close($socket);
            throw new \RuntimeException("Failed to bind to $address: " . \socket_strerror($errno), $errno);
        }
        $stream     = \socket_export_stream($socket);
        $this->sock = \socket_import_stream($stream);

        return $stream;
    }

    /** Keepalive as in Go: first probe after $seconds idle, then every $seconds, dropped after 9 unanswered. */
    private static function setKeepalive(\Socket $socket, int $seconds): bool
    {
        return @\socket_set_option($socket, \SOL_SOCKET, \SO_KEEPALIVE, 1) && @\socket_set_option($socket, \SOL_TCP, \TCP_KEEPIDLE, $seconds)
            && @\socket_set_option($socket, \SOL_TCP, \TCP_KEEPINTVL, $seconds) && @\socket_set_option($socket, \SOL_TCP, \TCP_KEEPCNT, 9);
    }

    private static function applyDefaultContext(array $context, string $protocol): array
    {
        $context['socket']['backlog'] ??= 65535;

        if ('unix' === $protocol) {
            // Unix sockets don't support these options
            unset($context['socket']['so_reuseport'], $context['socket']['tcp_nodelay'], $context['socket']['keepalive']);
        } else {
            $context['socket']['so_reuseport'] ??= true;
            $context['socket']['tcp_nodelay']  ??= true;
            $context['socket']['keepalive']    ??= 15;
        }

        return $context;
    }
}

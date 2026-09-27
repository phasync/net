<?php

namespace phasync\Net;

use Generator;
use IteratorAggregate;
use phasync\IOException;

/**
 * A TCP (or Unix socket) server that hands out Duplex connections. Create one with serve().
 *
 * With the phasync extension's tcp_server(), a TCP server is one stream for the listening
 * socket and every connection: the event loop waits on one file descriptor however many
 * connections are open, and one read collects new connections and data from many clients.
 * Without it (and for Unix sockets), each connection is a socket of its own (a StreamDuplex).
 * Connections behave the same either way.
 *
 * ```php
 * phasync::run(function () {
 *     foreach (phasync\Net\serve('0.0.0.0:8080') as $conn) {
 *         phasync::go(function () use ($conn) {
 *             while ('' !== ($data = $conn->read())) {
 *                 $conn->write($data); // echo
 *             }
 *             $conn->close();
 *         });
 *     }
 * });
 * ```
 *
 * @implements IteratorAggregate<string, Duplex>
 */
final class Server implements IteratorAggregate
{
    private ?Multiplexer $mux = null;

    private ?Listener $listener = null;

    /** Open connections, without the extension, for max_connections. */
    private int $open = 0;

    /** Raised when a connection opens or closes, without the extension. */
    private readonly object $room;

    /** @var list<array{0: resource, 1: string}> connections waiting when close() was called, without the extension */
    private array $drained = [];

    /** @var \WeakMap<StreamDuplex, true> open connections, without the extension, for shutdown() */
    private \WeakMap $streams;

    /**
     * @see serve()
     *
     * @throws \RuntimeException if the address cannot be bound
     */
    public function __construct(string $address, private readonly array $options = [])
    {
        $this->room    = new \stdClass();
        $this->streams = new \WeakMap();
        if (!\str_contains($address, '://')) {
            $address = 'tcp://' . $address;
        }
        if (\str_starts_with($address, 'tcp://') && \function_exists('phasync\ext\tcp_server')) {
            $parts     = \parse_url($address);
            $this->mux = new Multiplexer(\trim($parts['host'] ?? '', '[]'), (int) ($parts['port'] ?? 0), [
                'backlog'         => $options['backlog'] ?? 65535,
                'reuseport'       => $options['reuseport'] ?? true,
                'nodelay'         => $options['nodelay'] ?? true,
                'max_connections' => $options['max_connections'] ?? 0,
            ] + \array_intersect_key($options, ['read_chunk' => true, 'high_water' => true]));

            return;
        }
        $this->listener = new Listener($address, ['socket' => [
            'backlog'      => $options['backlog'] ?? 65535,
            'so_reuseport' => $options['reuseport'] ?? true,
            'tcp_nodelay'  => $options['nodelay'] ?? true,
        ]]);
    }

    /**
     * The next connection: waits for one.
     *
     * @throws IOException when the server is closed, also while waiting
     */
    public function accept(): Duplex
    {
        if (null !== $this->mux) {
            return $this->mux->accept();
        }
        if ($this->drained) {
            [$stream, $peer] = \array_shift($this->drained);

            return $this->adopt($stream, $peer);
        }
        $max = $this->options['max_connections'] ?? 0;
        while ($max > 0 && $this->open >= $max && null !== $this->listener) {
            \phasync::awaitFlag($this->room, \PHP_FLOAT_MAX); // new connections wait in the backlog
        }
        if (null === $this->listener) {
            throw new IOException('The server is closed');
        }
        [$stream, $peer] = $this->listener->accept();

        return $this->adopt($stream, $peer);
    }

    /**
     * Wait until the server is full with a client waiting to be accepted: at max_connections,
     * or (with the extension) accepting failed for lack of resources. A server can make room
     * then, closing a connection that only waits on its client, as nginx does with its idle
     * keep-alive connections. (With the extension, "a client waiting" is not known: only that
     * accepting stopped.)
     *
     * @throws IOException when the server is closed, also while waiting
     */
    public function awaitFull(): void
    {
        if (null !== $this->mux) {
            while (!$this->mux->isFull()) {
                \phasync::awaitFlag($this->mux->fullFlag, \PHP_FLOAT_MAX);
            }

            return;
        }
        while (true) {
            while (!$this->isFull() && null !== $this->listener) {
                \phasync::awaitFlag($this->room, \PHP_FLOAT_MAX);
            }
            if (null === $this->listener) {
                throw new IOException('The server is closed');
            }
            $this->listener->awaitPending();
            if ($this->isFull()) {
                return;
            }
        }
    }

    /** @param resource $stream */
    private function adopt($stream, string $peer): StreamDuplex
    {
        ++$this->open;
        \phasync::raiseFlag($this->room);
        $duplex                 = new StreamDuplex($stream, $peer, function (StreamDuplex $duplex) {
            --$this->open;
            unset($this->streams[$duplex]);
            \phasync::raiseFlag($this->room);
        });
        $this->streams[$duplex] = true;

        return $duplex;
    }

    /**
     * Accept connections in a foreach loop: peer address => connection. The loop ends when the
     * server is closed.
     *
     * @return Generator<string, Duplex>
     */
    public function getIterator(): Generator
    {
        while (true) {
            try {
                $connection = $this->accept();
            } catch (IOException) {
                return;
            }
            yield $connection->peer() => $connection;
        }
    }

    /**
     * Whether accepting stopped: at max_connections, or (with the extension) accept() failing
     * for lack of resources. New connections wait in the kernel's backlog meanwhile.
     */
    public function isFull(): bool
    {
        if (null !== $this->mux) {
            return $this->mux->isFull();
        }
        $max = $this->options['max_connections'] ?? 0;

        return $max > 0 && $this->open >= $max;
    }

    /**
     * Stop listening: accept() hands out the connections that were already waiting, then
     * throws IOException, and the loop over the server ends. Open connections go on. The
     * socket leaves its SO_REUSEPORT group at once, so the kernel gives new connections to the
     * other listeners on the address.
     */
    public function close(): void
    {
        $this->mux?->stopListening();
        if (null !== $this->listener) {
            $this->drained  = $this->listener->drain();
            $this->listener = null;
        }
        \phasync::raiseFlag($this->room);
    }

    /**
     * Stop listening and close every connection now; reads and writes waiting on them end.
     * With the extension, the server's stream closes; without it, the last connection's close
     * ends it all the same.
     */
    public function shutdown(): void
    {
        $this->close();
        $this->mux?->close();
        foreach ($this->drained as [$stream]) {
            \fclose($stream);
        }
        $this->drained = [];
        foreach ($this->streams as $duplex => $_) {
            $duplex->close();
        }
    }

    /** The address the server is bound to, with the real port when it listened on port 0. */
    public function addr(): string
    {
        return null !== $this->mux ? $this->mux->address() : (string) $this->listener?->addr();
    }

    /** Whether connections are multiplexed through the phasync extension's tcp_server(). */
    public function isMultiplexed(): bool
    {
        return null !== $this->mux;
    }
}

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

    private readonly object $room;

    /**
     * @see serve()
     *
     * @throws \RuntimeException if the address cannot be bound
     */
    public function __construct(string $address, private readonly array $options = [])
    {
        $this->room = new \stdClass();
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
        $max = $this->options['max_connections'] ?? 0;
        while ($max > 0 && $this->open >= $max && null !== $this->listener) {
            \phasync::awaitFlag($this->room, \PHP_FLOAT_MAX); // new connections wait in the backlog
        }
        if (null === $this->listener) {
            throw new IOException('The server is closed');
        }
        [$stream, $peer] = $this->listener->accept();
        ++$this->open;

        return new StreamDuplex($stream, $peer, function () {
            --$this->open;
            \phasync::raiseFlag($this->room);
        });
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

    /** Stop listening. With the extension, every connection closes too. */
    public function close(): void
    {
        $this->mux?->close();
        $this->listener?->close();
        $this->listener = null;
        \phasync::raiseFlag($this->room);
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

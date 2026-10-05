<?php

namespace phasync\Net;

use Generator;
use IteratorAggregate;
use phasync\IOException;

/**
 * A TCP (or Unix socket) server that hands out Duplex connections, each a socket of its own
 * (a StreamDuplex). Create one with serve().
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
    private ?Listener $listener = null;

    /** Open connections, for max_connections. */
    private int $open = 0;

    /** Raised when a connection opens or closes. */
    private readonly object $room;

    /** @var list<array{0: resource, 1: string}> connections waiting when close() was called */
    private array $drained = [];

    /** @var \WeakMap<StreamDuplex, true> open connections, for shutdown() */
    private \WeakMap $streams;

    /** At max_connections with a client waiting, see awaitFull(). */
    private bool $fullAndWaiting = false;

    /** Raised when $fullAndWaiting becomes true. */
    private readonly object $full;

    /**
     * @see serve()
     *
     * @throws \RuntimeException if the address cannot be bound
     */
    public function __construct(string $address, private readonly array $options = [])
    {
        $this->room    = new \stdClass();
        $this->full    = new \stdClass();
        $this->streams = new \WeakMap();
        if (!\str_contains($address, '://')) {
            $address = 'tcp://' . $address;
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
        while (true) {
            if ($this->drained) {
                [$stream, $peer] = \array_shift($this->drained);

                return $this->adopt($stream, $peer);
            }
            if (null === $this->listener) {
                throw new IOException('The server is closed');
            }
            try {
                $max = $this->options['max_connections'] ?? 0;
                while ($max > 0 && $this->open >= $max && null !== $this->listener) {
                    // New connections wait in the backlog. Only this coroutine waits on the
                    // listener (phasync allows one waiter per stream): once a client waits while
                    // there is no room, awaitFull() is told
                    $this->listener->awaitPending();
                    if ($this->open >= $max) {
                        $this->fullAndWaiting = true;
                        \phasync::raiseFlag($this->full);
                        \phasync::awaitFlag($this->room, \PHP_FLOAT_MAX);
                        $this->fullAndWaiting = false;
                    }
                }
                if (null === $this->listener) {
                    continue;
                }
                $stream = $this->listener->accept($peer);

                return $this->adopt($stream, $peer);
            } catch (IOException $e) {
                if (null !== $this->listener) {
                    throw $e;
                }
                // close() while waiting: the connections it took from the queue come next
            }
        }
    }

    /**
     * Wait until the server is at max_connections with a client waiting to be accepted. A
     * server can make room then, closing a connection that only waits on its client, as nginx
     * does with its idle keep-alive connections. Returns at once while it stays full, so wait
     * for room (a connection closing) before calling it again.
     *
     * @throws IOException when the server is closed, also while waiting
     */
    public function awaitFull(): void
    {
        while (!$this->fullAndWaiting) {
            if (null === $this->listener) {
                throw new IOException('The server is closed');
            }
            \phasync::awaitFlag($this->full, \PHP_FLOAT_MAX);
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

    /** Whether accepting stopped, at max_connections. New connections wait in the kernel's backlog meanwhile. */
    public function isFull(): bool
    {
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
        if (null !== $this->listener) {
            $this->drained  = $this->listener->drain();
            $this->listener = null;
        }
        \phasync::raiseFlag($this->room);
        \phasync::raiseFlag($this->full);
    }

    /**
     * Stop listening and close every connection now; reads and writes waiting on them end.
     */
    public function shutdown(): void
    {
        $this->close();
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
        return (string) $this->listener?->addr();
    }
}

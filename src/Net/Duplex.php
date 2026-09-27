<?php

namespace phasync\Net;

use phasync\IOException;
use phasync\TimeoutException;

/**
 * A full-duplex byte connection, such as a TCP connection: what a Server hands out, and what a
 * protocol (HTTP, a WebSocket) reads and writes, without knowing how the bytes travel.
 *
 * Reads and writes wait in the event loop, never blocking other coroutines, and behave the same
 * whether the connection is a socket of its own (StreamDuplex) or one of many multiplexed
 * through the phasync extension's tcp_server().
 */
interface Duplex
{
    /**
     * The next bytes the peer sent, at most $max: waits until there are some. '' only once the
     * peer has finished sending and everything was read, or the connection is closed.
     *
     * @param float|null $timeout seconds to wait; null: no limit
     *
     * @throws TimeoutException when nothing came within $timeout
     */
    public function read(int $max = 65536, ?float $timeout = null): string;

    /**
     * Send all of $bytes, in order: concurrent writes from other coroutines don't interleave
     * with it. Returns once the bytes are on their way; waits while the peer is slow to take
     * what was sent before.
     *
     * @param float|null $timeout seconds to wait for a slow peer; null: no limit
     *
     * @throws IOException      when the connection is closed or broke
     * @throws TimeoutException when the peer took nothing within $timeout
     */
    public function write(string $bytes, ?float $timeout = null): void;

    /**
     * Whether the peer has finished sending, and everything it sent was read. Noticed without a
     * read: a peer that closed its side is seen here while nothing is being read.
     */
    public function eof(): bool;

    /** Whether bytes the peer sent are waiting: read() would return them at once. */
    public function pending(): bool;

    /** Finish our side: the peer reads the end after what was written; we can still read. */
    public function end(): void;

    /**
     * Close the connection once what was written is sent. A read() or write() waiting on it,
     * in any coroutine, returns '' or throws IOException.
     */
    public function close(): void;

    public function isClosed(): bool;

    /** The peer's address, such as '203.0.113.7:51234' or '[::1]:51234'. */
    public function peer(): string;

    /** Our address on the connection, such as '0.0.0.0:8080'. */
    public function local(): string;
}

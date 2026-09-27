<?php

namespace phasync\Net;

use phasync;
use phasync\IOException;

/**
 * One connection of a Multiplexer (the phasync extension's tcp_server()): its input is buffered
 * here as the pump delivers it; its output is frames to the server.
 *
 * Reading is paused (the client's data stays in its socket) while more than PAUSE_AT bytes are
 * unread, and resumed when they are read. A write waits while the server reports the
 * connection's output backed up past its high_water (H), until it drained (W).
 *
 * @internal made by Multiplexer, used as a Duplex
 */
final class MuxDuplex implements Duplex
{
    /** Unread input that pauses reading from the client. */
    public const PAUSE_AT = 1 << 20;

    private string $in = '';

    /** The client finished sending (E). */
    private bool $finished = false;

    /** Closed by us, or gone (X). */
    private bool $closed = false;

    private bool $paused = false;

    /** Output backed up past high_water (H), until drained (W). */
    private bool $backlogged = false;

    private readonly object $readable;

    private readonly object $writable;

    public function __construct(private readonly Multiplexer $server, public readonly int $id, private readonly string $peer, private readonly string $local)
    {
        $this->readable = new \stdClass();
        $this->writable = new \stdClass();
    }

    public function read(int $max = 65536, ?float $timeout = null): string
    {
        $deadline = null === $timeout ? null : \microtime(true) + $timeout;
        while ('' === $this->in && !$this->finished && !$this->closed) {
            phasync::awaitFlag($this->readable, null === $deadline ? \PHP_FLOAT_MAX : \max(0.0, $deadline - \microtime(true)));
        }
        if ('' === $this->in) {
            return '';
        }
        $data     = \substr($this->in, 0, $max);
        $this->in = (string) \substr($this->in, \strlen($data));
        if ($this->paused && '' === $this->in && !$this->closed) {
            $this->paused = false;
            $this->server->send('R', $this->id);
        }

        return $data;
    }

    public function write(string $bytes, ?float $timeout = null): void
    {
        $deadline = null === $timeout ? null : \microtime(true) + $timeout;
        // One frame per write: a write never interleaves with another
        while ($this->backlogged && !$this->closed) {
            phasync::awaitFlag($this->writable, null === $deadline ? \PHP_FLOAT_MAX : \max(0.0, $deadline - \microtime(true)));
        }
        if ($this->closed) {
            throw new IOException('The connection is closed');
        }
        if ('' !== $bytes) {
            $this->server->send('D', $this->id, $bytes);
        }
    }

    public function eof(): bool
    {
        return '' === $this->in && ($this->finished || $this->closed);
    }

    public function pending(): bool
    {
        return '' !== $this->in;
    }

    public function end(): void
    {
        if (!$this->closed) {
            $this->server->send('E', $this->id);
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->server->send('X', $this->id);
        $this->gone();
    }

    public function isClosed(): bool
    {
        return $this->closed;
    }

    public function peer(): string
    {
        return $this->peer;
    }

    public function local(): string
    {
        return $this->local;
    }

    /** @internal Data from the client (D). */
    public function received(string $data): void
    {
        $this->in .= $data;
        if (!$this->paused && \strlen($this->in) >= self::PAUSE_AT) {
            $this->paused = true;
            $this->server->send('P', $this->id);
        }
        phasync::raiseFlag($this->readable);
    }

    /** @internal The client finished sending (E). */
    public function finished(): void
    {
        $this->finished = true;
        phasync::raiseFlag($this->readable);
    }

    /** @internal Output backed up past high_water (H). */
    public function backlogged(): void
    {
        $this->backlogged = true;
    }

    /** @internal Backed-up output drained (W). */
    public function drained(): void
    {
        $this->backlogged = false;
        phasync::raiseFlag($this->writable);
    }

    /** @internal The connection is gone (X), or closed by us or the server. */
    public function gone(): void
    {
        $this->closed = true;
        phasync::raiseFlag($this->readable);
        phasync::raiseFlag($this->writable);
    }
}

<?php

namespace phasync\Net;

use phasync;
use phasync\IOException;

/**
 * A Duplex over a stream resource (a socket from a Listener, dial() or anywhere): waits with
 * phasync::readable() and phasync::writable(). The stream is made non-blocking.
 */
final class StreamDuplex implements Duplex
{
    private bool $eof = false;

    private bool $closed = false;

    /** A write is under way: the next waits, so writes don't interleave. */
    private bool $writing = false;

    private string $peer;

    private string $local;

    /**
     * @param resource                  $stream
     * @param \Closure(self): void|null $onClose told when close() closes it
     */
    public function __construct(private $stream, ?string $peer = null, private readonly ?\Closure $onClose = null)
    {
        \stream_set_blocking($stream, false);
        // Without PHP's own read buffer, read($max) takes at most $max from the kernel, and
        // pending() and eof() see what the kernel holds
        \stream_set_read_buffer($stream, 0);
        $this->peer  = $peer ?? (string) @\stream_socket_get_name($stream, true);
        $this->local = (string) @\stream_socket_get_name($stream, false);
    }

    public function read(int $max = 65536, ?float $timeout = null): string
    {
        $deadline = null === $timeout ? null : \microtime(true) + $timeout;
        while (!$this->closed && !$this->eof) {
            $data = @\fread($this->stream, $max);
            if (false === $data) {
                break; // the connection broke: its end, for the reader
            }
            if ('' !== $data) {
                return $data;
            }
            if (\feof($this->stream)) {
                break;
            }
            try {
                phasync::readable($this->stream, null === $deadline ? \PHP_FLOAT_MAX : \max(0.0, $deadline - \microtime(true)));
            } catch (IOException $e) {
                if ($this->closed) {
                    break; // closed while waiting
                }
                throw $e;
            }
        }
        $this->eof = true;

        return '';
    }

    public function write(string $bytes, ?float $timeout = null): void
    {
        $deadline = null === $timeout ? null : \microtime(true) + $timeout;
        $remaining = static fn () => null === $deadline ? \PHP_FLOAT_MAX : \max(0.0, $deadline - \microtime(true));
        while ($this->writing) {
            phasync::awaitFlag($this, $remaining());
        }
        $this->writing = true;
        try {
            while ('' !== $bytes) {
                if ($this->closed) {
                    throw new IOException('The connection is closed');
                }
                $n = @\fwrite($this->stream, $bytes);
                if (false === $n) {
                    throw new IOException('The connection broke');
                }
                $bytes = \substr($bytes, $n);
                if ('' !== $bytes) {
                    phasync::writable($this->stream, $remaining());
                }
            }
        } finally {
            $this->writing = false;
            phasync::raiseFlag($this);
        }
    }

    public function eof(): bool
    {
        if (!$this->eof && !$this->closed) {
            // A socket's end, without reading: a peek finds nothing to read and no more to come
            $peek = @\stream_socket_recvfrom($this->stream, 1, \STREAM_PEEK);
            $this->eof = false === $peek ? \feof($this->stream) : '' === $peek;
        }

        return $this->eof || $this->closed;
    }

    public function pending(): bool
    {
        if ($this->closed) {
            return false;
        }
        $peek = @\stream_socket_recvfrom($this->stream, 1, \STREAM_PEEK);

        return \is_string($peek) && '' !== $peek;
    }

    public function end(): void
    {
        if (!$this->closed) {
            @\stream_socket_shutdown($this->stream, \STREAM_SHUT_WR);
        }
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }
        $this->closed = true;
        @\fclose($this->stream);
        phasync::raiseFlag($this);
        if (null !== $this->onClose) {
            ($this->onClose)($this);
        }
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
}

<?php

namespace phasync\Net;

use phasync;
use phasync\CancelledException;
use phasync\IOException;

/**
 * The phasync extension's tcp_server(): one stream for the listening socket and every
 * connection. One coroutine (the pump) reads its frames and hands them to the connections; a
 * connection's writes are frames written to the same stream.
 *
 * Frames: a 13-byte header, type:u8 id:u64 len:u32 little-endian, then len bytes. See the
 * extension's README for the protocol.
 *
 * @internal used by Server
 */
final class Multiplexer
{
    /** @var resource|null */
    private $fp;

    /** Bytes of a frame whose rest comes with the next read. */
    private string $carry = '';

    /** @var array<int, MuxDuplex> by id */
    private array $connections = [];

    /** @var \SplQueue<MuxDuplex> accepted, for accept() */
    private \SplQueue $accepted;

    private ?\Fiber $pump = null;

    /** Still accepting; see stopListening(). */
    private bool $listening = true;

    /** Raised when accepting stops for lack of room (F). */
    public readonly object $fullFlag;

    /** Accepting stopped (F): at max_connections, or accept() failed for lack of resources. */
    private bool $full = false;

    private readonly string $address;

    /**
     * @param array<string, mixed> $options the extension's: backlog, reuseport, nodelay,
     *                                      max_connections, read_chunk, high_water
     */
    public function __construct(string $host, int $port, array $options)
    {
        $warning = null;
        \set_error_handler(static function (int $code, string $message) use (&$warning): bool {
            $warning = $message;

            return true;
        });
        try {
            $fp = \phasync\ext\tcp_server($host, $port, $options);
        } finally {
            \restore_error_handler();
        }
        if (!\is_resource($fp)) {
            throw new \RuntimeException("Failed to bind to $host:$port: " . ($warning ?? 'unknown error'));
        }
        \stream_set_blocking($fp, false);
        $this->fp       = $fp;
        $this->address  = (string) \stream_socket_get_name($fp, false);
        $this->accepted = new \SplQueue();
        $this->fullFlag = new \stdClass();
        $this->pump     = phasync::go($this->pump(...));
    }

    public function address(): string
    {
        return $this->address;
    }

    /**
     * The next connection: waits for one.
     *
     * @throws IOException when the server is closed, also while waiting
     */
    public function accept(): MuxDuplex
    {
        while ($this->accepted->isEmpty()) {
            if (null === $this->fp || !$this->listening) {
                throw new IOException('The server is closed');
            }
            phasync::awaitFlag($this->accepted, \PHP_FLOAT_MAX);
        }

        return $this->accepted->dequeue();
    }

    /** Whether accepting stopped: new connections wait in the kernel's backlog. */
    public function isFull(): bool
    {
        return $this->full;
    }

    /** @internal A frame for the server: whole, so frames from coroutines never interleave. */
    public function send(string $type, int $id, string $payload = ''): void
    {
        if (null !== $this->fp) {
            \fwrite($this->fp, $type . \pack('PV', $id, \strlen($payload)) . $payload);
        }
    }

    /**
     * Stop accepting: accept() hands out the connections accepted so far, then throws; open
     * connections go on, and the server's stream is closed when the last one ended.
     *
     * The extension can't stop listening alone yet (phasync/phasync-ext#4): until it can, a
     * connection that arrives after this is closed at once, and the listener stays in its
     * SO_REUSEPORT group.
     */
    public function stopListening(): void
    {
        $this->listening = false;
        phasync::raiseFlag($this->accepted);
        if (!$this->connections) {
            $this->close();
        }
    }

    /** Close the server and every connection; accept() and waiting reads end. */
    public function close(): void
    {
        if (null === $this->fp) {
            return;
        }
        $fp       = $this->fp;
        $this->fp = null;
        if (null !== $this->pump && !$this->pump->isTerminated() && $this->pump !== \Fiber::getCurrent()) {
            phasync::cancel($this->pump);
        }
        \fclose($fp);
        foreach ($this->connections as $connection) {
            $connection->gone();
        }
        $this->connections = [];
        phasync::raiseFlag($this->accepted);
    }

    private function pump(): void
    {
        try {
            while (null !== $this->fp) {
                phasync::readable($this->fp, \PHP_FLOAT_MAX);
                if (null === $this->fp) {
                    return;
                }
                $buffer = $this->carry . \fread($this->fp, 262144);
                $length = \strlen($buffer);
                $offset = 0;
                while ($length - $offset >= 13) {
                    ['type' => $type, 'id' => $id, 'len' => $len] = \unpack('atype/Pid/Vlen', $buffer, $offset);
                    if ($length - $offset - 13 < $len) {
                        break; // the rest comes with the next read
                    }
                    $this->frame($type, $id, $len > 0 ? \substr($buffer, $offset + 13, $len) : '');
                    $offset += 13 + $len;
                }
                $this->carry = \substr($buffer, $offset);
            }
        } catch (CancelledException) {
            // close()
        } catch (IOException) {
            // The stream was closed under us: close() ends the rest
        }
    }

    private function frame(string $type, int $id, string $payload): void
    {
        switch ($type) {
            case 'D':
                ($this->connections[$id] ?? null)?->received($payload);
                break;
            case 'C':
                if (!$this->listening) {
                    $this->send('X', $id); // see stopListening()
                    break;
                }
                [$peer, $local]         = \explode("\0", $payload, 2) + ['', ''];
                $connection             = new MuxDuplex($this, $id, $peer, $local);
                $this->connections[$id] = $connection;
                $this->accepted->enqueue($connection);
                phasync::raiseFlag($this->accepted);
                break;
            case 'E':
                ($this->connections[$id] ?? null)?->finished();
                break;
            case 'H':
                ($this->connections[$id] ?? null)?->backlogged();
                break;
            case 'W':
                ($this->connections[$id] ?? null)?->drained();
                break;
            case 'X':
                ($this->connections[$id] ?? null)?->gone();
                unset($this->connections[$id]);
                if (!$this->listening && !$this->connections) {
                    $this->close();
                }
                break;
            case 'F':
                $this->full = true;
                phasync::raiseFlag($this->fullFlag);
                break;
            case 'A':
                $this->full = false;
                break;
        }
    }
}

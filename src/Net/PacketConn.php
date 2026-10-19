<?php

namespace phasync\Net;

use Generator;
use IteratorAggregate;
use phasync;
use phasync\IOException;

/**
 * A UDP socket bound to one address, like Go's net.PacketConn. Create one with
 * listenPacket(). Port 0 binds a free port, for a client.
 *
 * ```php
 * phasync::run(function () {
 *     $conn = phasync\Net\listenPacket('0.0.0.0:9000');
 *     foreach ($conn as $peer => $data) {
 *         $conn->writeTo("Echo: $data", $peer);
 *     }
 * });
 * ```
 *
 * @implements IteratorAggregate<string, string>
 */
final class PacketConn implements IteratorAggregate
{
    /** @var resource|null */
    private $socket;

    private string $address;

    /**
     * @param array $options so_reuseport (default false). `context` escapes to raw stream
     *                        context options for anything not named above.
     *
     * @see listenPacket()
     *
     * @throws \RuntimeException if the address cannot be bound
     */
    public function __construct(string $address, array $options = [])
    {
        if (!\str_contains($address, '://')) {
            $address = 'udp://' . $address;
        }
        [$socketOptions, $context] = SocketOptions::normalize($options, ['so_reuseport'], ['so_reuseport' => false]);
        $context['socket']         = $socketOptions + ($context['socket'] ?? []);

        $warning = null;
        \set_error_handler(static function (int $code, string $message) use (&$warning): bool {
            $warning = $message;

            return true;
        });
        try {
            // UDP binds only, there is no listen step
            $socket = \stream_socket_server($address, $errno, $errstr, \STREAM_SERVER_BIND, \stream_context_create($context));
        } finally {
            \restore_error_handler();
        }
        if (!$socket) {
            throw new \RuntimeException("Failed to bind to $address: " . ($errstr ?: $warning ?? 'unknown error'), $errno);
        }

        \stream_set_blocking($socket, false);
        $this->socket  = $socket;
        $this->address = (string) \stream_socket_get_name($socket, false);
    }

    /**
     * Wait for the next datagram, like Go's PacketConn.ReadFrom(). A datagram already waiting
     * is returned without waiting for the event loop.
     *
     * @param int $maxLength Maximum bytes per datagram; longer datagrams are truncated
     *
     * @return array{0: string, 1: string} the datagram and the peer address
     *
     * @throws IOException when the socket is closed, also while waiting
     */
    public function readFrom(int $maxLength = 65536): array
    {
        while (\is_resource($this->socket)) {
            // Returns false, without a warning, when nothing is waiting.
            $data = \stream_socket_recvfrom($this->socket, $maxLength, 0, $peer);
            if (false !== $data && null !== $peer) {
                return [$data, $peer];
            }
            try {
                phasync::readable($this->socket, \PHP_FLOAT_MAX);
            } catch (IOException $e) {
                if (\is_resource($this->socket)) {
                    throw $e;
                }
                break; // closed while waiting
            }
        }

        throw new IOException('The socket is closed');
    }

    /**
     * Receive datagrams in a foreach loop: peer address => datagram. The loop ends when the
     * socket is closed.
     *
     * @return Generator<string, string>
     */
    public function getIterator(): Generator
    {
        while (\is_resource($this->socket)) {
            try {
                [$data, $peer] = $this->readFrom();
            } catch (IOException $e) {
                if (\is_resource($this->socket)) {
                    throw $e;
                }

                return;
            }
            yield $peer => $data;
        }
    }

    /**
     * Send a datagram to an address, like Go's PacketConn.WriteTo(), such as a peer address
     * from readFrom().
     *
     * @return int|false bytes sent, or false on failure
     *
     * @throws IOException when the socket is closed
     */
    public function writeTo(string $data, string $address): int|false
    {
        if (!\is_resource($this->socket)) {
            throw new IOException('The socket is closed');
        }
        phasync::writable($this->socket, \PHP_FLOAT_MAX);

        return \stream_socket_sendto($this->socket, $data, 0, $address);
    }

    /**
     * Close the socket. A coroutine waiting in readFrom() gets IOException, and a foreach
     * loop over the socket ends.
     */
    public function close(): void
    {
        if (\is_resource($this->socket)) {
            \fclose($this->socket);
        }
        $this->socket = null;
    }

    /**
     * The address the socket is bound to, with the real port when it was bound to port 0
     * (for example '127.0.0.1:43127').
     */
    public function addr(): string
    {
        return $this->address;
    }
}

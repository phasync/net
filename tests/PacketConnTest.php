<?php

test('PacketConn binds to port 0 and reports the real address', function () {
    $server = phasync\Net\listenPacket('127.0.0.1:0');
    expect($server->addr())->toMatch('/^127\.0\.0\.1:[1-9][0-9]*$/');
    $server->close();
});

test('PacketConn yields peer => data and can reply with writeTo()', function () {
    $response = phasync::run(function () {
        $server = phasync\Net\listenPacket('127.0.0.1:0');

        phasync::go(function () use ($server) {
            foreach ($server as $peer => $data) {
                $server->writeTo("Echo: $data", $peer);
                $server->close();
            }
        });

        $client = stream_socket_client('udp://' . $server->addr());
        stream_set_blocking($client, false);
        fwrite($client, 'Hello UDP');
        $response = fread(phasync::readable($client), 65536);
        fclose($client);

        return $response;
    });

    expect($response)->toBe('Echo: Hello UDP');
});

test('PacketConn takes datagrams already waiting without suspending', function () {
    $result = phasync::run(function () {
        $server = phasync\Net\listenPacket('127.0.0.1:0');
        $client = stream_socket_client('udp://' . $server->addr());
        for ($i = 0; $i < 20; ++$i) {
            fwrite($client, "msg$i");
        }
        phasync::sleep(0.05);

        $turns   = 0;
        $stop    = false;
        $sibling = phasync::go(function () use (&$turns, &$stop) {
            while (!$stop) {
                ++$turns;
                phasync::sleep(0);
            }
        });

        $before   = $turns;
        $received = [];
        foreach ($server as $data) {
            $received[] = $data;
            if (20 === count($received)) {
                break;
            }
        }
        $siblingTurns = $turns - $before;

        $stop = true;
        phasync::await($sibling);
        fclose($client);
        $server->close();

        return [count($received), $received[0], $received[19], $siblingTurns];
    });

    expect($result)->toBe([20, 'msg0', 'msg19', 0]);
});

test('closing the socket ends a waiting foreach loop without an exception', function () {
    $result = phasync::run(function () {
        $server = phasync\Net\listenPacket('127.0.0.1:0');
        $loop   = phasync::go(function () use ($server) {
            foreach ($server as $data) {
            }

            return 'loop ended';
        });
        phasync::sleep(0.05);
        $server->close();

        return phasync::await($loop);
    });

    expect($result)->toBe('loop ended');
});

test('PacketConn throws on an address it cannot bind', function () {
    expect(fn () => phasync\Net\listenPacket('invalid:99999'))->toThrow(RuntimeException::class);
});

test('readFrom() returns one datagram and its peer address at a time, like Go', function () {
    $result = phasync::run(function () {
        $conn   = phasync\Net\listenPacket('127.0.0.1:0');
        $client = phasync\Net\listenPacket('127.0.0.1:0');
        $client->writeTo('ping', $conn->addr());
        [$data, $peer] = $conn->readFrom();
        $conn->writeTo('pong', $peer);
        [$reply] = $client->readFrom();
        $conn->close();
        $client->close();

        return [$data, $peer === $client->addr(), $reply];
    });

    expect($result)->toBe(['ping', true, 'pong']);
});

test('readFrom() and writeTo() throw IOException on a closed socket', function () {
    $conn = phasync\Net\listenPacket('127.0.0.1:0');
    $conn->close();

    expect(fn () => phasync::run(fn () => $conn->readFrom()))->toThrow(phasync\IOException::class);
    expect(fn () => $conn->writeTo('x', '127.0.0.1:9'))->toThrow(phasync\IOException::class);
});

/** Whether SO_REUSEPORT is set on a PacketConn's underlying socket. */
function packetconn_reuseport(phasync\Net\PacketConn $conn): bool
{
    $stream = (new ReflectionProperty($conn, 'socket'))->getValue($conn);

    return (bool) socket_get_option(socket_import_stream($stream), SOL_SOCKET, SO_REUSEPORT);
}

test('PacketConn defaults so_reuseport to off, and the option reaches the socket', function () {
    if (!extension_loaded('sockets')) {
        $this->markTestSkipped('Needs ext-sockets');
    }
    $default = phasync\Net\listenPacket('127.0.0.1:0');
    expect(packetconn_reuseport($default))->toBeFalse();
    $default->close();

    $withOption = phasync\Net\listenPacket('127.0.0.1:0', ['so_reuseport' => true]);
    expect(packetconn_reuseport($withOption))->toBeTrue();
    $withOption->close();
});

test('listenPacket() rejects unknown option names loudly', function () {
    expect(fn () => phasync\Net\listenPacket('127.0.0.1:0', ['reuseport' => true]))
        ->toThrow(InvalidArgumentException::class, 'Unknown option reuseport');
});

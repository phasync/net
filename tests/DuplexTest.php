<?php

/*
 * Server and Duplex. The same tests run with the phasync extension (the server multiplexes
 * connections through tcp_server()) and without it (a socket per connection): connections must
 * behave the same. Clients are StreamDuplex over dial().
 */

use phasync\IOException;
use phasync\Net\Duplex;
use phasync\Net\Server;
use phasync\Net\StreamDuplex;
use phasync\TimeoutException;

use function phasync\Net\dial;
use function phasync\Net\serve;

/** Run $test with a server on a free port, whose connections $handle handles. */
function with_server(Closure $handle, Closure $test, array $options = []): mixed
{
    return phasync::run(function () use ($handle, $test, $options) {
        $server = serve('127.0.0.1:0', $options);
        phasync::go(function () use ($server, $handle) {
            foreach ($server as $conn) {
                phasync::go(fn () => $handle($conn));
            }
        });
        try {
            return $test($server);
        } finally {
            $server->shutdown();
        }
    });
}

function client(Server $server): StreamDuplex
{
    return new StreamDuplex(dial($server->addr()));
}

/** Everything the peer sends until it finishes. */
function read_all(Duplex $conn, ?float $timeout = 5): string
{
    $all = '';
    while ('' !== ($data = $conn->read(65536, $timeout))) {
        $all .= $data;
    }

    return $all;
}

$echo = static function (Duplex $conn) {
    while ('' !== ($data = $conn->read())) {
        $conn->write($data);
    }
    $conn->close();
};

test('the server multiplexes through tcp_server() exactly when the extension has it', function () {
    phasync::run(function () {
        $server = serve('127.0.0.1:0');
        expect($server->isMultiplexed())->toBe(function_exists('phasync\ext\tcp_server'));
        $server->close();
    });
});

test('echo: what a client writes comes back', function () use ($echo) {
    expect(with_server($echo, function (Server $server) {
        $client = client($server);
        $client->write('hello');
        $reply = $client->read(5, 5);
        $client->close();

        return $reply;
    }))->toBe('hello');
});

test('peer() and local() are the two ends\' addresses', function () {
    $seen = with_server(function (Duplex $conn) {
        $conn->write($conn->peer() . ' ' . $conn->local());
        $conn->close();
    }, function (Server $server) {
        $client = client($server);

        return [read_all($client), $client->local(), $server->addr()];
    });
    [$reported, $clientAddress, $serverAddress] = $seen;
    expect($reported)->toBe("$clientAddress $serverAddress");
});

test('a half-close: the server reads the end, and can still answer', function () {
    expect(with_server(function (Duplex $conn) {
        $request = read_all($conn);
        $conn->write(strrev($request));
        $conn->close();
    }, function (Server $server) {
        $client = client($server);
        $client->write('abc');
        $client->write('def');
        $client->end();

        return [read_all($client), $client->eof()];
    }))->toBe(['fedcba', true]);
});

test('many connections at once, each getting its own bytes back', function () use ($echo) {
    $replies = with_server($echo, function (Server $server) {
        $fibers = [];
        for ($i = 0; $i < 200; ++$i) {
            $fibers[] = phasync::go(function () use ($server, $i) {
                $client = client($server);
                $client->write("message $i");
                $client->end();

                return read_all($client);
            });
        }

        return array_map(static fn ($f) => phasync::await($f), $fibers);
    });
    expect($replies)->toBe(array_map(static fn ($i) => "message $i", range(0, 199)));
});

test('megabytes both ways, through a server that reads slowly (pausing the client)', function () {
    $payload = random_bytes(5 << 20);
    $result  = with_server(function (Duplex $conn) {
        phasync::sleep(0.2); // let the input back up
        $all = read_all($conn);
        $conn->write(hash('sha256', $all) . strlen($all));
        $conn->write($all);
        $conn->close();
    }, function (Server $server) use ($payload) {
        $client = client($server);
        phasync::go(function () use ($client, $payload) {
            $client->write($payload, 10);
            $client->end();
        });
        $reply = read_all($client, 10);

        return [substr($reply, 0, 64 + strlen((string) strlen($payload))), substr($reply, 64 + strlen((string) strlen($payload))) === $payload];
    });
    expect($result)->toBe([hash('sha256', $payload) . strlen($payload), true]);
});

test('writes from several coroutines never interleave', function () {
    $records = with_server(function (Duplex $conn) {
        $writers = [];
        foreach (['A', 'B', 'C'] as $letter) {
            $writers[] = phasync::go(function () use ($conn, $letter) {
                for ($i = 0; $i < 300; ++$i) {
                    $conn->write(str_repeat($letter, 1000));
                    if (0 === $i % 50) {
                        phasync::sleep(0);
                    }
                }
            });
        }
        foreach ($writers as $writer) {
            phasync::await($writer);
        }
        $conn->close();
    }, fn (Server $server) => str_split(read_all(client($server)), 1000));
    expect(count($records))->toBe(900);
    foreach ($records as $record) {
        expect(count_chars($record, 3))->toHaveLength(1);
    }
});

test('a client that leaves ends the server\'s read with \'\'; eof() says so', function () {
    $seen = with_server(function (Duplex $conn) use (&$seen) {
        $seen = [read_all($conn), $conn->eof()];
    }, function (Server $server) use (&$seen) {
        $client = client($server);
        $client->write('bye');
        $client->close();
        $deadline = microtime(true) + 5;
        while (null === $seen && microtime(true) < $deadline) {
            phasync::sleep(0.01);
        }

        return $seen;
    });
    expect($seen)->toBe(['bye', true]);
});

test('close() ends a read waiting in another coroutine', function () {
    expect(with_server(function (Duplex $conn) {
        $reader = phasync::go(fn () => $conn->read());
        phasync::sleep(0.05);
        $conn->close();

        return phasync::await($reader);
    }, function (Server $server) {
        $client = client($server);

        return [read_all($client), $client->eof()];
    }))->toBe(['', true]);
});

test('a write after close() throws IOException', function () {
    expect(fn () => with_server(function (Duplex $conn) {}, function (Server $server) {
        $client = client($server);
        $client->close();
        $client->write('late');
    }))->toThrow(IOException::class);
});

test('read() gives up after its timeout', function () {
    expect(fn () => with_server(function (Duplex $conn) {
        phasync::sleep(1);
        $conn->close();
    }, fn (Server $server) => client($server)->read(10, 0.1)))->toThrow(TimeoutException::class);
});

test('at max_connections, the next connection waits until one closes', function () {
    $log = with_server(function (Duplex $conn) {
        $conn->write('hi');
        $conn->read(); // until the client closes
        $conn->close();
    }, function (Server $server) {
        $log = [];
        $a   = client($server);
        $b   = client($server);
        $log[] = $a->read(2, 5) . $b->read(2, 5);
        $c     = client($server);                    // connected in the kernel's backlog
        try {
            $c->read(2, 0.3);
        } catch (TimeoutException) {
            $log[] = 'third waits';
        }
        $log[] = $server->isFull() ? 'full' : 'not full';
        $a->close();
        $log[] = $c->read(2, 5);                     // accepted now

        return $log;
    }, ['max_connections' => 2]);
    expect($log)->toBe(['hihi', 'third waits', 'full', 'hi']);
});

test('close() on the server ends the accept loop', function () {
    expect(phasync::run(function () {
        $server = serve('127.0.0.1:0');
        $loop   = phasync::go(function () use ($server) {
            foreach ($server as $conn) {
            }

            return 'ended';
        });
        phasync::sleep(0.05);
        $server->close();

        return phasync::await($loop);
    }))->toBe('ended');
});

test('pending() and eof() see what arrived without reading it', function () {
    expect(with_server(function (Duplex $conn) {
        phasync::sleep(0.05); // the client wrote and finished meanwhile
        $seen = [$conn->pending(), $conn->eof(), $conn->read(), $conn->pending(), $conn->eof()];
        $conn->write(json_encode($seen));
        $conn->close();
    }, function (Server $server) {
        $client = client($server);
        $client->write('x');
        $client->end();

        return json_decode(read_all($client));
    }))->toBe([true, false, 'x', false, true]);
});

test('close() stops listening; open connections go on', function () use ($echo) {
    expect(with_server($echo, function (Server $server) {
        $client = client($server);
        $client->write('before');
        $first = $client->read(6, 5);
        $server->close();
        $client->write('after');

        return [$first, $client->read(5, 5)];
    }))->toBe(['before', 'after']);
});

test('awaitFull() returns once the server is at max_connections with a client waiting', function () {
    expect(with_server(function (Duplex $conn) {
        $conn->read(); // until the client closes
        $conn->close();
    }, function (Server $server) {
        $full = phasync::go(function () use ($server) {
            $server->awaitFull();

            return 'full';
        });
        $a = client($server);
        phasync::sleep(0.05);
        $early = $full->isTerminated();
        $b     = client($server); // waits in the backlog
        $result = phasync::await($full, 3);
        $a->close();
        $b->close();

        return [$early, $result];
    }, ['max_connections' => 1]))->toBe([
        // The extension reports full at the limit, before anyone waits (phasync/phasync-ext#4)
        function_exists('phasync\ext\tcp_server'),
        'full',
    ]);
});

test('close() while the accept loop waits: a connection that was queued is still handed out and served', function () use ($echo) {
    expect(phasync::run(function () use ($echo) {
        $server = serve('127.0.0.1:0');
        phasync::go(function () use ($server, $echo) {
            foreach ($server as $conn) {
                phasync::go(fn () => $echo($conn));
            }
        });
        phasync::sleep(0.01); // the loop waits for a connection
        // A blocking connect doesn't yield: the connection is queued, the loop still waits
        $client = new StreamDuplex(stream_socket_client('tcp://' . $server->addr()));
        $server->close();
        $client->write('queued');
        $reply = $client->read(6, 2);
        $client->close();
        $server->shutdown();

        return $reply;
    }))->toBe('queued');
});

test('a client that stops reading makes writes wait, and time out', function () {
    $outcome = null;
    with_server(function (Duplex $conn) use (&$outcome) {
        try {
            for ($i = 0; $i < 200; ++$i) {
                $conn->write(str_repeat('x', 1 << 20), 0.3); // 200 MiB in all: far past any buffer
            }
            $outcome = 'wrote everything';
        } catch (TimeoutException) {
            $outcome = 'timed out';
        } finally {
            $conn->close();
        }
    }, function (Server $server) use (&$outcome) {
        $client   = client($server); // never reads
        $deadline = microtime(true) + 10;
        while (null === $outcome && microtime(true) < $deadline) {
            phasync::sleep(0.05);
        }
    }, ['high_water' => 1 << 20]);
    expect($outcome)->toBe('timed out');
});

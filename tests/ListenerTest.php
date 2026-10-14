<?php

use phasync\Util\WaitGroup;

test('Listener binds to port 0 and reports the real address', function () {
    $server = phasync\Net\listen('127.0.0.1:0');
    expect($server->addr())->toMatch('/^127\.0\.0\.1:[1-9][0-9]*$/');
    $server->close();
});

test('Listener accepts a connection with foreach and yields peer => stream', function () {
    $result = phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');
        $got    = null;

        phasync::go(function () use ($server, &$got) {
            foreach ($server as $peer => $stream) {
                $got  = [$peer, fread(phasync::readable($stream), 65536)];
                fwrite(phasync::writable($stream), 'Got: ' . $got[1]);
                fclose($stream);
                $server->close();
            }
        });

        $client = stream_socket_client('tcp://' . $server->addr());
        stream_set_blocking($client, false);
        fwrite(phasync::writable($client), 'Hello');
        $response = fread(phasync::readable($client), 65536);
        fclose($client);

        return [$got, $response];
    });

    [[$peer, $data], $response] = $result;
    expect($peer)->toMatch('/^127\.0\.0\.1:[0-9]+$/');
    expect($data)->toBe('Hello');
    expect($response)->toBe('Got: Hello');
});

test('Listener yields plain stream resources, not AsyncStream-wrapped ones', function () {
    $meta = phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');
        $client = stream_socket_client('tcp://' . $server->addr());
        foreach ($server as $stream) {
            $meta = stream_get_meta_data($stream);
            fclose($stream);
            break;
        }
        fclose($client);
        $server->close();

        return $meta;
    });

    expect($meta['wrapper_type'] ?? null)->not->toBe('user-space');
    expect($meta['blocked'])->toBeFalse();
});

test('Listener takes connections already waiting in the queue without suspending', function () {
    // With N connections queued, accepting all of them must not wait for the event loop
    // between connections: a sibling coroutine counting its turns must get none.
    $result = phasync::run(function () {
        $server  = phasync\Net\listen('127.0.0.1:0');
        $clients = [];
        for ($i = 0; $i < 20; ++$i) {
            $clients[] = stream_socket_client('tcp://' . $server->addr());
        }
        phasync::sleep(0.05); // let the kernel finish the handshakes into the accept queue

        $turns   = 0;
        $stop    = false;
        $sibling = phasync::go(function () use (&$turns, &$stop) {
            while (!$stop) {
                ++$turns;
                phasync::sleep(0);
            }
        });

        $before   = $turns;
        $accepted = [];
        foreach ($server as $stream) {
            $accepted[] = $stream;
            if (20 === count($accepted)) {
                break;
            }
        }
        $siblingTurns = $turns - $before;

        $stop = true;
        phasync::await($sibling);
        foreach (array_merge($clients, $accepted) as $s) {
            fclose($s);
        }
        $server->close();

        return [count($accepted), $siblingTurns];
    });

    expect($result)->toBe([20, 0]);
});

test('Listener lets the event loop run after every ACCEPT_BURST connections taken from a queue that does not empty', function () {
    // Clients that keep connecting would otherwise keep accept() from ever waiting, and the
    // other coroutines (the connections' own) from ever running.
    $n      = 2 * phasync\Net\Listener::ACCEPT_BURST + 10;
    $result = phasync::run(function () use ($n) {
        $server  = phasync\Net\listen('127.0.0.1:0', ['socket' => ['backlog' => 1024]]);
        $clients = [];
        for ($i = 0; $i < $n; ++$i) {
            $clients[] = stream_socket_client('tcp://' . $server->addr());
        }
        phasync::sleep(0.05); // let the kernel finish the handshakes into the accept queue

        $turns   = 0;
        $stop    = false;
        $sibling = phasync::go(function () use (&$turns, &$stop) {
            while (!$stop) {
                ++$turns;
                phasync::sleep(0);
            }
        });

        $before   = $turns;
        $accepted = [];
        foreach ($server as $stream) {
            $accepted[] = $stream;
            if ($n === count($accepted)) {
                break;
            }
        }
        $siblingTurns = $turns - $before;

        $stop = true;
        phasync::await($sibling);
        foreach (array_merge($clients, $accepted) as $s) {
            fclose($s);
        }
        $server->close();

        return [count($accepted), $siblingTurns];
    });

    expect($result)->toBe([$n, 2]);
});

test('closing the listener ends a waiting foreach loop without an exception', function () {
    $result = phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');
        $loop   = phasync::go(function () use ($server) {
            foreach ($server as $stream) {
                fclose($stream);
            }

            return 'loop ended';
        });
        phasync::sleep(0.05); // the loop is now waiting for a connection
        $server->close();

        return phasync::await($loop);
    });

    expect($result)->toBe('loop ended');
});

test('Listener throws on an address it cannot bind', function () {
    expect(fn () => phasync\Net\listen('invalid:99999'))->toThrow(RuntimeException::class);
});

test('Listener handles concurrent connections', function () {
    phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');
        $wg     = new WaitGroup();
        $count  = 10;

        // Echo with a delay, to prove the connections are handled concurrently.
        phasync::go(function () use ($server, $count) {
            $handled = 0;
            foreach ($server as $conn) {
                phasync::go(function () use ($conn) {
                    phasync::sleep(0.05);
                    fwrite(phasync::writable($conn), 'Done');
                    fclose($conn);
                });
                if (++$handled >= $count) {
                    $server->close();
                }
            }
        });

        $start = microtime(true);
        for ($i = 0; $i < $count; ++$i) {
            $wg->add();
            phasync::go(function () use ($server, $wg) {
                try {
                    $client = stream_socket_client('tcp://' . $server->addr());
                    stream_set_blocking($client, false);
                    expect(fread(phasync::readable($client), 1024))->toBe('Done');
                    fclose($client);
                } finally {
                    $wg->done();
                }
            });
        }
        $wg->await();

        // Sequential would take 10 * 0.05 s = 0.5 s.
        expect(microtime(true) - $start)->toBeLessThan(0.3);
    });
});

test('Listener handles large payloads', function () {
    phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');

        // Read 1 MB, reply with its hash.
        phasync::go(function () use ($server) {
            foreach ($server as $conn) {
                $buffer = '';
                while (true) {
                    $chunk = fread(phasync::readable($conn), 65536);
                    if ('' === $chunk || false === $chunk) {
                        if (feof($conn)) {
                            break;
                        }
                        continue;
                    }
                    $buffer .= $chunk;
                }
                fwrite(phasync::writable($conn), md5($buffer));
                fclose($conn);
                $server->close();
            }
        });

        $client = stream_socket_client('tcp://' . $server->addr());
        stream_set_blocking($client, false);
        $payload = str_repeat('X', 1024 * 1024);
        $offset  = 0;
        while ($offset < strlen($payload)) {
            $written = fwrite(phasync::writable($client), substr($payload, $offset, 65536));
            if (false === $written) {
                break;
            }
            $offset += $written;
        }
        stream_socket_shutdown($client, STREAM_SHUT_WR);
        $response = fread(phasync::readable($client), 1024);
        fclose($client);

        expect($response)->toBe(md5($payload));
    });
});

test('accept() returns one connection and its peer address at a time, like Go', function () {
    $result = phasync::run(function () {
        $listener = phasync\Net\listen('127.0.0.1:0');
        $client   = stream_socket_client('tcp://' . $listener->addr());
        $conn     = $listener->accept($peer);
        fwrite($client, 'ping');
        $data = fread(phasync::readable($conn), 10);
        fclose($conn);
        fclose($client);
        $listener->close();

        return [is_resource($conn) || 'resource (closed)' === get_debug_type($conn), $peer, $data];
    });

    expect($result[0])->toBeTrue();
    expect($result[1])->toMatch('/^127\.0\.0\.1:[0-9]+$/');
    expect($result[2])->toBe('ping');
});

test('accept() throws IOException on a closed listener, also in a coroutine already waiting in it', function () {
    $result = phasync::run(function () {
        $listener = phasync\Net\listen('127.0.0.1:0');
        $waiting  = phasync::go(function () use ($listener) {
            try {
                $listener->accept();

                return 'accepted';
            } catch (phasync\IOException $e) {
                return $e->getMessage();
            }
        });
        phasync::sleep(0.05);
        $listener->close();
        $afterwards = null;
        try {
            $listener->accept();
        } catch (phasync\IOException $e) {
            $afterwards = $e->getMessage();
        }

        return [phasync::await($waiting), $afterwards];
    });

    expect($result)->toBe(['The listener is closed', 'The listener is closed']);
});

/** The soft and hard open-file limits as posix_setrlimit() takes them. */
function nofile_limits(): array
{
    $r = posix_getrlimit();

    return array_map(fn ($v) => 'unlimited' === $v ? POSIX_RLIM_INFINITY : (int) $v, [$r['soft openfiles'], $r['hard openfiles']]);
}

/** The socket file descriptors this process has open: number => close-on-exec. */
function socket_fd_flags(): array
{
    $fds = [];
    foreach (glob('/proc/self/fdinfo/*') as $info) {
        $fd = '/proc/self/fd/' . basename($info); // glob()'s own, closed again, is gone by now
        if (is_link($fd) && str_starts_with(readlink($fd), 'socket:')) {
            preg_match('/^flags:\s+(\d+)/m', file_get_contents($info), $m);
            $fds[basename($info)] = (bool) (octdec($m[1]) & 02000000); // O_CLOEXEC
        }
    }

    return $fds;
}

test('the listener and the connections it accepts are close-on-exec, so processes the application starts do not inherit them', function (string $address) {
    if (!is_dir('/proc/self/fdinfo') || !defined('SOCK_CLOEXEC')) {
        $this->markTestSkipped('Needs Linux and ext-sockets');
    }
    $new = phasync::run(function () use ($address) {
        $before   = socket_fd_flags();
        $listener = phasync\Net\listen($address);
        $client   = stream_socket_client(str_starts_with($address, 'unix:') ? $address : 'tcp://' . $listener->addr());
        $conn     = $listener->accept();
        $new      = array_diff_key(socket_fd_flags(), $before);
        fclose($client);
        fclose($conn);
        $listener->close();

        return $new;
    });

    // The listener and the accepted connection; the client, which PHP creates, is the one that is not
    $counts = array_count_values(array_map(intval(...), $new));
    ksort($counts); // in the order the descriptors were numbered, which varies
    expect($counts)->toBe([0 => 1, 1 => 2]);
})->with([
    'tcp'  => ['127.0.0.1:0'],
    'unix' => ['unix://' . sys_get_temp_dir() . '/phasync_cloexec.sock'],
]);

test('accept() backs off while there are no file descriptors, instead of retrying at once', function () {
    [$soft, $hard] = nofile_limits();
    $result        = phasync::run(function () use ($soft, $hard) {
        $listener = phasync\Net\listen('127.0.0.1:0');
        $client   = stream_socket_client('tcp://' . $listener->addr());
        phasync::sleep(0.05); // the handshake finishes into the accept queue

        // Use up the descriptors, up to a lowered limit
        posix_setrlimit(POSIX_RLIMIT_NOFILE, (int) max(array_map('basename', glob('/proc/self/fd/*'))) + 3, $hard);
        set_error_handler(fn () => true); // PHPUnit's would need to autoload a class
        $filler = [];
        while ($f = @fopen('/dev/null', 'r')) {
            $filler[] = $f;
        }

        $accepting = phasync::go(fn () => $listener->accept());
        phasync::sleep(0.3); // a listener that retried at once would never give this coroutine its turn
        $stillWaiting = !$accepting->isTerminated();

        foreach ($filler as $f) {
            fclose($f);
        }
        posix_setrlimit(POSIX_RLIMIT_NOFILE, $soft, $hard);
        restore_error_handler();
        $conn   = phasync::await($accepting, 3);
        $result = [$stillWaiting, is_resource($conn)];
        fclose($client);
        fclose($conn);
        $listener->close();

        return $result;
    });

    expect($result)->toBe([true, true]);
});

/** The keepalive options of a connection: SO_KEEPALIVE, and idle, interval and count when it is on. */
function keepalive_of($stream): array
{
    $sock = socket_import_stream($stream);
    $on   = (bool) socket_get_option($sock, SOL_SOCKET, SO_KEEPALIVE);

    return $on ? [true, ...array_map(fn ($o) => socket_get_option($sock, SOL_TCP, $o), [TCP_KEEPIDLE, TCP_KEEPINTVL, TCP_KEEPCNT])] : [false];
}

test('accepted TCP connections have keepalive set as in Go (15 s idle, 15 s interval, 9 probes) unless it is switched off', function (array $context, array $expected) {
    if (!extension_loaded('sockets')) {
        $this->markTestSkipped('Needs ext-sockets');
    }
    $result = phasync::run(function () use ($context) {
        $listener = phasync\Net\listen('127.0.0.1:0', $context);
        $client   = stream_socket_client('tcp://' . $listener->addr());
        $conn     = $listener->accept();
        $result   = keepalive_of($conn);
        fclose($client);
        fclose($conn);
        $listener->close();

        return $result;
    });

    expect($result)->toBe($expected);
})->with([
    'default'  => [[], [true, 15, 15, 9]],
    '30 s'     => [['socket' => ['keepalive' => 30]], [true, 30, 30, 9]],
    'zero'     => [['socket' => ['keepalive' => 0]], [false]],
    'false'    => [['socket' => ['keepalive' => false]], [false]],
    // an option only stream_socket_server() takes: the listener is made by it, not by ext-sockets
    'streams'  => [['socket' => ['ipv6_v6only' => false]], [true, 15, 15, 9]],
]);

test('the Server option keepalive reaches the listener', function (array $options, array $expected) {
    if (!extension_loaded('sockets')) {
        $this->markTestSkipped('Needs ext-sockets');
    }
    $result = phasync::run(function () use ($options) {
        $server = phasync\Net\serve('127.0.0.1:0', $options);
        $client = stream_socket_client('tcp://' . $server->addr());
        $conn   = $server->accept();
        $stream = (new ReflectionProperty($conn, 'stream'))->getValue($conn);
        $result = keepalive_of($stream);
        fclose($client);
        $conn->close();
        $server->close();

        return $result;
    });

    expect($result)->toBe($expected);
})->with([
    'default' => [[], [true, 15, 15, 9]],
    '20 s'    => [['keepalive' => 20], [true, 20, 20, 9]],
    'off'     => [['keepalive' => 0], [false]],
]);

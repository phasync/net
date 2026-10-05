<?php

beforeEach(function () {
    if ('WIN' === strtoupper(substr(PHP_OS, 0, 3))) {
        $this->markTestSkipped('Unix sockets not supported on Windows');
    }
});

test('Listener supports Unix domain sockets', function () {
    $socketPath = sys_get_temp_dir() . '/phasync_test_' . uniqid() . '.sock';

    $response = phasync::run(function () use ($socketPath) {
        $server = phasync\Net\listen("unix://$socketPath");

        phasync::go(function () use ($server) {
            foreach ($server as $conn) {
                fwrite(phasync::writable($conn), 'Hello Unix');
                fclose($conn);
                $server->close();
            }
        });

        $client = stream_socket_client("unix://$socketPath");
        stream_set_blocking($client, false);
        $response = fread(phasync::readable($client), 65536);
        fclose($client);

        return $response;
    });

    expect($response)->toBe('Hello Unix');
});

test('closing a Unix socket server removes the socket file, so the path can be bound again', function () {
    $socketPath = sys_get_temp_dir() . '/phasync_test_' . uniqid() . '.sock';

    $server = phasync\Net\listen("unix://$socketPath");
    expect(file_exists($socketPath))->toBeTrue();
    $server->close();
    expect(file_exists($socketPath))->toBeFalse();

    $again = phasync\Net\listen("unix://$socketPath");
    $again->close();
    expect(file_exists($socketPath))->toBeFalse();
});

/** A listening Unix socket made as an application does before forking workers. */
function inherited_listener(string $path)
{
    $stream = stream_socket_server("unix://$path", $errno, $errstr);
    stream_set_blocking($stream, false);

    return $stream;
}

test('Listener::fromStream() accepts from a listening socket it did not create, and close() leaves the file alone', function () {
    $path   = sys_get_temp_dir() . '/phasync_inherited_' . uniqid() . '.sock';
    $stream = inherited_listener($path);

    $reply = phasync::run(function () use ($path, $stream) {
        $listener = phasync\Net\Listener::fromStream($stream);
        expect($listener->addr())->toBe($path);
        $client = stream_socket_client("unix://$path");
        $conn   = $listener->accept($peer);
        expect($peer)->toBe('');
        fwrite($conn, 'hi');
        fclose($conn);
        $reply = fread($client, 10);
        fclose($client);

        // Waiting in accept() when close() is called
        $waiting = phasync::go(fn () => $listener->accept());
        phasync::sleep(0.01);
        $listener->close();
        try {
            phasync::await($waiting);
        } catch (phasync\IOException) {
            $reply .= ' closed';
        }

        return $reply;
    });

    expect($reply)->toBe('hi closed');
    expect(file_exists($path))->toBeTrue();
    unlink($path);
});

test('Server over an inherited listener: max_connections, awaitFull() and the lingering drain work as for its own listener', function () {
    $path   = sys_get_temp_dir() . '/phasync_inherited_' . uniqid() . '.sock';
    $stream = inherited_listener($path);

    $result = phasync::run(function () use ($path, $stream) {
        $server = phasync\Net\serve(phasync\Net\Listener::fromStream($stream), ['max_connections' => 1]);
        $first  = stream_socket_client("unix://$path");
        $conn   = $server->accept();
        $second = stream_socket_client("unix://$path"); // waits in the backlog: the server is full
        $result = [$server->isFull()];
        $next   = phasync::go(fn () => $server->accept());
        $server->awaitFull();
        $result[] = $next->isTerminated();
        $conn->close();
        $server->close(); // drains: the queued connection is handed out first
        $accepted = phasync::await($next);
        $result[] = $accepted->peer();
        $accepted->close();
        fclose($first);
        fclose($second);

        return $result;
    });

    expect($result)->toBe([true, false, '']);
    expect(file_exists($path))->toBeTrue();
    unlink($path);
});

test('workers forked after the listener was made accept from the same socket, and one closing it neither removes the file nor stops the others', function () {
    if (!function_exists('pcntl_fork') || !function_exists('posix_kill')) {
        $this->markTestSkipped('Needs pcntl and posix');
    }
    $path   = sys_get_temp_dir() . '/phasync_inherited_' . uniqid() . '.sock';
    $stream = inherited_listener($path);

    $pids = [];
    for ($i = 0; $i < 2; ++$i) {
        $pid = pcntl_fork();
        if (0 === $pid) {
            pcntl_alarm(10);
            phasync::run(function () use ($stream, $path) {
                $server = phasync\Net\serve(phasync\Net\Listener::fromStream($stream));
                $conn   = $server->accept();
                $server->close(); // this worker's descriptor only
                $conn->write(getmypid() . ':' . (int) file_exists($path));
                $conn->close();
            });
            posix_kill(posix_getpid(), SIGKILL); // not exit(): the test runner's shutdown must not run in a child
        }
        $pids[] = $pid;
    }
    fclose($stream);

    // A worker takes one connection and closes: the second connection can only go to the other
    $replies = [];
    for ($i = 0; $i < 2; ++$i) {
        $client = stream_socket_client("unix://$path");
        stream_set_timeout($client, 5);
        $replies[] = stream_get_contents($client);
        fclose($client);
    }
    foreach ($pids as $pid) {
        pcntl_waitpid($pid, $status);
    }

    expect(array_map(fn ($r) => (int) $r, $replies))->toEqualCanonicalizing($pids);
    expect(array_map(fn ($r) => substr($r, strpos($r, ':') + 1), $replies))->toBe(['1', '1']);
    expect(file_exists($path))->toBeTrue();
    unlink($path);
});

<?php

test('dial() connects and exchanges data', function () {
    $response = phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');

        phasync::go(function () use ($server) {
            foreach ($server as $stream) {
                $data = fread(phasync::readable($stream), 65536);
                fwrite(phasync::writable($stream), "Echo: $data");
                fclose($stream);
                $server->close();
            }
        });

        $client = phasync\Net\dial($server->addr());
        $client->write('Hello');
        $response = $client->read(65536, 5);
        $client->close();

        return $response;
    });

    expect($response)->toBe('Echo: Hello');
});

test('dial() returns a Duplex (the same type serve() hands out), not a raw resource', function () {
    $meta = phasync::run(function () {
        $server = phasync\Net\listen('127.0.0.1:0');
        $client = phasync\Net\dial($server->addr());
        expect($client)->toBeInstanceOf(phasync\Net\Duplex::class);
        expect($client)->toBeInstanceOf(phasync\Net\StreamDuplex::class);
        $stream = (new ReflectionProperty($client, 'stream'))->getValue($client);
        $meta   = stream_get_meta_data($stream);
        $client->close();
        $server->close();

        return $meta;
    });

    expect($meta['wrapper_type'] ?? null)->not->toBe('user-space');
    expect($meta['blocked'])->toBeFalse();
});

test('dial() throws on refused connection', function () {
    phasync::run(function () {
        // Port 1 requires root and is almost certainly not listening
        expect(fn () => phasync\Net\dial('127.0.0.1:1', timeout: 0.5))
            ->toThrow(RuntimeException::class);
    });
});

test('dial() connects to a Unix socket', function () {
    if ('WIN' === strtoupper(substr(PHP_OS, 0, 3))) {
        $this->markTestSkipped('Unix sockets not supported on Windows');
    }
    $socketPath = sys_get_temp_dir() . '/phasync_client_test_' . uniqid() . '.sock';

    $response = phasync::run(function () use ($socketPath) {
        $server = phasync\Net\listen("unix://$socketPath");

        phasync::go(function () use ($server) {
            foreach ($server as $stream) {
                fwrite(phasync::writable($stream), 'Unix OK');
                fclose($stream);
                $server->close();
            }
        });

        $client   = phasync\Net\dial('unix://' . $socketPath);
        $response = $client->read(65536, 5);
        $client->close();

        return $response;
    });

    expect($response)->toBe('Unix OK');
});

test('dial() resolves a host name', function () {
    $response = phasync::run(function () {
        $listener = phasync\Net\listen('127.0.0.1:0');
        $port     = substr($listener->addr(), strrpos($listener->addr(), ':') + 1);
        phasync::go(function () use ($listener) {
            $conn = $listener->accept();
            fwrite(phasync::writable($conn), 'hello');
            fclose($conn);
            $listener->close();
        });
        $conn = phasync\Net\dial("localhost:$port", 5);
        $data = $conn->read(10, 5);
        $conn->close();

        return $data;
    });

    expect($response)->toBe('hello');
});

test('dial() with tls:// does a TLS handshake without blocking other coroutines', function () {
    // A self-signed certificate for "localhost", written to a temporary file for the server
    $key  = openssl_pkey_new(['private_key_bits' => 2048, 'private_key_type' => OPENSSL_KEYTYPE_RSA]);
    $cert = openssl_csr_sign(openssl_csr_new(['commonName' => 'localhost'], $key), null, $key, 1, ['digest_alg' => 'sha256']);
    openssl_x509_export($cert, $certPem);
    openssl_pkey_export($key, $keyPem);
    $pemFile = tempnam(sys_get_temp_dir(), 'phasync-net-tls');
    file_put_contents($pemFile, $certPem . $keyPem);

    try {
        $result = phasync::run(function () use ($pemFile) {
            $server = stream_socket_server('tcp://127.0.0.1:0', $errno, $errstr, STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
                stream_context_create(['ssl' => ['local_cert' => $pemFile]]));
            $port   = substr(stream_socket_get_name($server, false), strrpos(stream_socket_get_name($server, false), ':') + 1);
            phasync::go(function () use ($server) {
                $conn = stream_socket_accept(phasync::readable($server));
                stream_set_blocking($conn, false);
                while (0 === ($done = stream_socket_enable_crypto($conn, true, STREAM_CRYPTO_METHOD_TLS_SERVER))) {
                    phasync::readable($conn);
                }
                fwrite(phasync::writable($conn), 'secret');
                fclose($conn);
                fclose($server);
            });

            $ticks = 0;
            $stop  = false;
            $ticker = phasync::go(function () use (&$ticks, &$stop) {
                while (!$stop) {
                    ++$ticks;
                    phasync::sleep(0);
                }
            });

            // The test certificate is self-signed, so trust it explicitly
            $conn   = phasync\Net\dial("tls://localhost:$port", 5, ['context' => ['ssl' => ['cafile' => $pemFile]]]);
            $stream = (new ReflectionProperty($conn, 'stream'))->getValue($conn);
            // TLS 1.3 session tickets make the socket readable without application data, so
            // a non-blocking read can return '' before the data arrives
            $data = '';
            while ('' === $data && !$conn->eof()) {
                $data = $conn->read(10, 5);
            }
            $crypto = stream_get_meta_data($stream)['crypto']['protocol'] ?? null;
            $conn->close();
            $stop = true;
            phasync::await($ticker);

            return [$data, $crypto !== null, $ticks > 0];
        });
    } finally {
        unlink($pemFile);
    }

    expect($result)->toBe(['secret', true, true]);
});

test('dial() rejects unknown option names loudly', function () {
    expect(fn () => phasync::run(fn () => phasync\Net\dial('127.0.0.1:1', timeout: 0.1, options: ['nodelay' => true])))
        ->toThrow(InvalidArgumentException::class, 'Unknown option nodelay');
});

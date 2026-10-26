# Changelog

## Unreleased

- A large `StreamDuplex::write()` the kernel takes in parts goes out in 64 KB slices from an offset; each partial write copied the whole rest (a 16 MB write: about 1.2 s of copying).

## 2.0.1 (2026-10-19)

Documentation corrections.

## 2.0.0 (2026-10-19)

The 2.0 release, the successor of phasync/server 1.x: TCP, UDP and Unix socket servers and
clients on phasync 2.0. Moving from phasync/server 1.x: see the README's "Upgrading from 1.x".

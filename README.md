# One Billion Row Challenge in Rust

## Introduction

This project is an implementation of the [One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/) in Rust, using only the standard library.

## Run the challenge

```bash
cargo run --release -- measurements.txt
```

## Result

On Pop!\_OS 22.04 LTS Ryzen 5 1400, 16GB

```bash
make bench

Benchmark 1: ./target/release-with-debug/rust1brc measurements.txt
  Time (mean ± σ):     21.655 s ±  5.583 s    [User: 115.802 s, System: 4.748 s]
  Range (min … max):   16.621 s … 31.136 s    5 runs
```

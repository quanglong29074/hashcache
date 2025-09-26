#!/bin/bash

echo "Starting HashCache server..."
cargo run &
SERVER_PID=$!

echo "Waiting for server to start..."
sleep 3

echo "Running benchmark..."
cargo run --bin benchmark

echo "Stopping server..."
kill $SERVER_PID

echo "Benchmark complete!"

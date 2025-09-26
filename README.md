# HashCache

A high-performance, in-memory key-value cache server built with Rust and Hyper.rs.

## Features

- **High Performance**: Built on Hyper.rs with async/await for maximum throughput
- **Thread-Safe**: Uses `Arc<RwLock<HashMap>>` for concurrent read/write access
- **Simple API**: RESTful HTTP interface for PUT/GET operations
- **Comprehensive Benchmarking**: Built-in benchmark tool to measure performance
- **Real-time Stats**: Monitor cache statistics via HTTP endpoint

## Quick Start

### Prerequisites

- Rust 1.70+ with Cargo
- `curl` (for testing)

### Installation

1. Clone or download the project
2. Navigate to the project directory:
   ```bash
   cd hashcache
   ```

3. Build the project:
   ```bash
   cargo build --release
   ```

### Running the Server

Start the HashCache server:
```bash
cargo run
```

The server will start on `http://localhost:3000` and display usage instructions.

## API Reference

### Store a Key-Value Pair (PUT)

```bash
curl -X PUT http://localhost:3000/{key} --data-binary "value"
```

**Example:**
```bash
curl -X PUT http://localhost:3000/mykey --data-binary "Hello, World!"
```

**Response:**
```
Stored key: mykey
```

### Retrieve a Value (GET)

```bash
curl http://localhost:3000/{key}
```

**Example:**
```bash
curl http://localhost:3000/mykey
```

**Response:**
```
Hello, World!
```

**Note:** Returns HTTP 404 if key not found.

### Get Cache Statistics (GET)

```bash
curl http://localhost:3000/stats
```

**Response:**
```json
{"total_keys": 42}
```

## Benchmarking

### Running Benchmarks

The project includes a comprehensive benchmark tool that tests:
- PUT operations (1000 requests)
- GET operations (1000 requests)  
- Mixed workload (50% PUT, 50% GET)

#### Option 1: Manual Benchmark

1. Start the server in one terminal:
   ```bash
   cargo run
   ```

2. Run benchmark in another terminal:
   ```bash
   cargo run --bin benchmark
   ```

#### Option 2: Automated Benchmark

Use the provided script that starts the server, runs benchmarks, and stops the server:
```bash
./run_benchmark.sh
```

### Sample Benchmark Output

```
HashCache Benchmark Tool
========================
✓ Server is reachable

Benchmarking PUT operations...
  Completed 100 PUT operations...
  Completed 200 PUT operations...
  ...

Benchmarking GET operations...
  Completed 100 GET operations...
  ...

Benchmarking mixed workload (50% PUT, 50% GET)...
  Completed 100 mixed operations...
  ...

BENCHMARK SUMMARY
=================
PUT Operations:
  Total: 1000 ops
  Duration: 0.85s
  Throughput: 1176.47 ops/sec
  Average latency: 0.85ms

GET Operations:
  Total: 1000 ops
  Duration: 0.72s
  Throughput: 1388.89 ops/sec
  Average latency: 0.72ms

Mixed Workload:
  Total: 1000 ops
  Duration: 0.78s
  Throughput: 1282.05 ops/sec
  Average latency: 0.78ms
```

## Architecture

### Core Components

- **HTTP Server**: Hyper.rs-based async server handling HTTP requests
- **Storage**: Thread-safe HashMap with Arc<RwLock> for concurrent access
- **Benchmark Tool**: Separate binary for performance testing
- **Request Handler**: Pattern matching for PUT/GET/Stats endpoints

### Threading Model

- **Main Thread**: Accepts incoming connections
- **Task Pool**: Each connection spawned as async task
- **Shared State**: HashMap protected by RwLock for concurrent access

### Performance Characteristics

- **Concurrent Reads**: Multiple readers can access data simultaneously
- **Exclusive Writes**: Write operations have exclusive access
- **Memory Storage**: All data stored in RAM for maximum speed
- **Zero-Copy**: Efficient byte handling with Hyper's body utilities

## Project Structure

```
hashcache/
├── src/
│   ├── main.rs              # Main server implementation
│   └── bin/
│       └── benchmark.rs     # Benchmark tool
├── Cargo.toml              # Project dependencies
├── run_benchmark.sh        # Automated benchmark script
└── README.md              # This file
```

## Dependencies

- **hyper 1.0**: HTTP server framework
- **hyper-util**: Hyper utilities  
- **http-body-util**: HTTP body handling
- **tokio**: Async runtime
- **reqwest**: HTTP client (for benchmarks)
- **serde_json**: JSON serialization

## Usage Examples

### Basic Operations

```bash
# Store some data
curl -X PUT http://localhost:3000/user:123 --data-binary '{"name": "John", "age": 30}'
curl -X PUT http://localhost:3000/counter --data-binary "42"

# Retrieve data
curl http://localhost:3000/user:123
curl http://localhost:3000/counter

# Check stats
curl http://localhost:3000/stats
```

### Testing with Binary Data

```bash
# Store binary data
curl -X PUT http://localhost:3000/image --data-binary @image.jpg

# Retrieve binary data
curl http://localhost:3000/image > retrieved_image.jpg
```

## Development

### Building for Release

```bash
cargo build --release
```

### Running Tests

```bash
cargo test
```

### Code Formatting

```bash
cargo fmt
```

### Linting

```bash
cargo clippy
```

## Limitations

- **Memory Only**: Data is not persisted to disk
- **No Authentication**: No built-in security mechanisms
- **Single Process**: No clustering or distribution
- **No TTL**: Keys don't expire automatically
- **No Eviction**: No automatic cleanup when memory is full

## License

This project is provided as-is for educational and testing purposes.

## Contributing

Feel free to submit issues and enhancement requests!

## Performance Tips

1. **Use Release Builds**: Always use `--release` for production
2. **Concurrent Clients**: The server handles multiple clients efficiently  
3. **Key Design**: Use meaningful, consistent key naming schemes
4. **Value Size**: Smaller values generally perform better
5. **Network**: Run on localhost for maximum performance during testing

## Troubleshooting

### Server Won't Start

- Check if port 3000 is already in use: `lsof -i :3000`
- Try a different port by modifying the source code

### Benchmark Connection Failed

- Ensure the server is running before starting benchmarks
- Check firewall settings if running on different machines

### Build Errors

- Update Rust: `rustup update`
- Clean build: `cargo clean && cargo build`

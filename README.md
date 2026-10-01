# BalET - Asynchronous Load Balancer with Lazy Logging

BalET is a high-performance, asynchronous load balancer written in Go that distributes incoming HTTP requests across multiple backend servers using various load balancing algorithms. It features concurrent request processing with goroutines, thread-safe operations, and a lazy logging system for efficient request/response tracking.

## Features
- **Asynchronous Request Processing**: Uses goroutines for non-blocking, concurrent request handling
- **Multiple Load Balancing Algorithms**:
  - Round Robin
  - Least Connections
  - Weighted Connections
- **Health Checking**: Automatic health checks via `/health` endpoint before routing
- **Lazy Logging**: Efficient buffered logging with configurable flush frequency and buffer size
- **Connection Tracking**: Real-time connection counting per server using B-tree data structure

## Why BalET?
BalET was designed to provide a lightweight, efficient load balancing solution for Go applications. Unlike heavy-weight load balancers like Nginx or HAProxy, BalET can be embedded directly into your Go application, giving you programmatic control over load balancing logic while maintaining high performance through asynchronous processing.

## Installation

```bash
go get github.com/IhitplayzYT/BalET
```

## Usage

### Basic Example

```go
package main

import (
    "github.com/IhitplayzYT/BalET/lib"
)

func main() {
    // Create a new load balancer with:
    // - timeout: 30 seconds
    // - buffer length: 1000 requests
    // - server root prefix: "http://localhost:3000"
    lb := lib.NewBalET(30, 1000, "http://localhost:3000")
    
    // Set the load balancing algorithm
    lb.algo = lib.RoundRobin // or lib.LeastConnections, lib.WeightedConnections
    
    // Add backend servers
    lb.add_server(lib.Server{ip: "localhost:8001", _w: 1.0})
    lb.add_server(lib.Server{ip: "localhost:8002", _w: 1.0})
    lb.add_server(lib.Server{ip: "localhost:8003", _w: 2.0}) // Higher weight for weighted algorithms
    
    // Start the load balancer (runs in a goroutine)
    go lb.run()
    
    // Submit requests
    req := lib.Request{
        method:    "GET",
        param_url: "/api/users",
        body:      "",
    }
    lb.submit(req)
    
    // Keep main function running
    select {}
}
```

### Load Balancing Algorithms

#### Round Robin
Distributes requests sequentially across all servers. Each server gets an equal share of requests.

```go
lb.algo = lib.RoundRobin
```

#### Least Connections
Routes requests to the server with the fewest active connections, ideal for varying request processing times.

```go
lb.algo = lib.LeastConnections
```

#### Weighted Connections
Routes requests based on server capacity (weight). Servers with higher weights receive more requests relative to their connection count.

```go
lb.algo = lib.WeightedConnections
lb.add_server(lib.Server{ip: "localhost:8001", _w: 1.0})  // Lower capacity
lb.add_server(lib.Server{ip: "localhost:8002", _w: 3.0})  // Higher capacity
```

### Lazy Logging Configuration

```go
// Configure the log manager
lb.log_mnger = lib.NewLogManager(
    "requests.log",  // File path
    100,             // Flush frequency (flush after 100 logs)
    4096,            // Max buffer size in bytes
)
```

## API Reference

### Types

#### `Server`
```go
type Server struct {
    ip string    // Server IP address
    _w float64   // Weight for weighted algorithms
}
```

#### `Request`
```go
type Request struct {
    method    string // HTTP method: GET, POST, PUT, DELETE
    param_url string // URL path
    body      string // Request body
}
```

#### `Algo`
```go
const (
    RoundRobin Algo = iota
    LeastConnections
    WeightedConnections
)
```

### Functions

#### `NewBalET(timeout, buff_l uint64, srvr_root_pfx string) BalEt`
Creates a new load balancer instance.

- `timeout`: Request timeout in seconds
- `buff_l`: Channel buffer size for requests
- `srvr_root_pfx`: Default server URL prefix

#### `add_server(s Server)`
Adds a backend server to the load balancer.

#### `submit(r Request)`
Submits a request to the load balancer for processing.

#### `run()`
Starts the load balancer's request processing loop. Should be run in a goroutine.

## Pros

- **Lightweight**: Minimal dependencies, can be embedded in Go applications
- **Asynchronous**: Non-blocking request processing with goroutines
- **Flexible**: Multiple load balancing algorithms for different use cases
- **Efficient Logging**: Lazy logging reduces I/O operations with buffering
- **Thread-Safe**: Mutex protection ensures safe concurrent operations
- **Health Checks**: Automatic health verification before routing requests
- **Programmatic Control**: Full control over load balancing logic from your code

## Cons

- **Limited Protocol Support**: Only supports HTTP (no HTTPS, TCP, etc.)
- **No Circuit Breaker**: Lacks built-in circuit breaker pattern for failing servers

## Requirements

- Go 1.26.5 or higher
- `github.com/tidwall/btree` v1.8.2

## Backend Server Requirements

Backend servers must implement a `/health` endpoint that returns:
- HTTP status code 200
- Response body containing "ok" (case-insensitive)

Example:
```go
func healthHandler(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
    w.Write([]byte("ok"))
}
```

## License

This project is provided as-is for educational and development purposes.

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

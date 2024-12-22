# Redis Server Implementation in Go

This project is a lightweight implementation of a Redis-like in-memory data store, written in Go. It supports core Redis functionalities like key-value storage, replication, and basic client-server communication.

## Features

- **Key-Value Store**: Supports `SET`, `GET`, and other basic Redis commands.
- **Replication**: Implements master-replica communication using the `REPLCONF` and `PSYNC` protocols.
- **Persistence**: Data persistence is supported for reliable storage between sessions.
- **Concurrency**: Efficient handling of multiple clients using Go’s goroutines and synchronization primitives.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/prkbuilds/redis-go.git
   cd redis-go
   ```

2. Build the server:
   ```bash
   go build -o redis-server
   ```

3. Run the server:
   ```bash
   ./redis-server
   ```

## Usage

### Start the Server
Run the server with the default configuration:
```bash
./redis-server
```

### Interact with the Server
Use a Redis CLI or any Redis client library to interact with the server:
```bash
redis-cli -p 6379
```

### Supported Commands
- `PING`: Check server connectivity.
- `SET <key> <value>`: Store a key-value pair.
- `GET <key>`: Retrieve the value for a key.
- `REPLCONF`: Handle replica configuration.
- `PSYNC`: Synchronize data between master and replica.

## Replication Workflow
1. The replica sends a `REPLCONF` command with parameters like `listening-port`.
2. The master acknowledges the replica and initiates synchronization using `PSYNC`.
3. The master sends the RDB snapshot to the replica for full resynchronization.

## Directory Structure
```
redis-go/
├── main.go           # Entry point for the server
├── server.go         # Core server implementation
├── handler.go        # Client request handler
├── store.go          # In-memory data store implementation
└── README.md         # Project documentation
```

## Future Enhancements
- Support for advanced Redis data types (e.g., lists, sets, and hashes).
- More robust error handling and logging.
- Optimization for large-scale deployment.

## Contributing
Contributions are welcome! Please fork the repository and create a pull request for any improvements or new features.

## License
This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.
```


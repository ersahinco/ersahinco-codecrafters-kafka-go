# Kafka Broker - Clean Architecture

## Project Structure

```
app/
├── main.go                    # Entry point - minimal, delegates to server
├── server/
│   └── server.go             # Connection handling & request routing
├── handlers/
│   ├── apiversion.go         # ApiVersions request handler
│   ├── fetchtopic.go         # Fetch v16 request handler
│   ├── producetopic.go       # Produce v11 request handler
│   └── describetopic.go      # DescribeTopicPartitions v0 handler
├── topic/
│   └── topic.go              # Topic metadata & broker state management
├── partition/
│   └── partition.go          # Partition I/O operations (read/write records)
├── parser/
│   └── elements.go           # Binary protocol parsing & encoding utilities
├── errors/
│   └── custom.go             # Kafka error codes & custom error types
└── logger/
    └── colored.go            # Colored console logging utilities
```

## Architecture Benefits

- **Separation of Concerns**: Each package has a single, well-defined responsibility
- **Testability**: Isolated components are easier to unit test
- **Maintainability**: Changes to one handler don't affect others
- **Scalability**: Easy to add new API handlers or extend existing ones
- **Readability**: Clear structure makes navigation intuitive

## Key Components

### Server Layer
Handles TCP connections, request parsing, and routing to appropriate handlers.

### Handlers
Each Kafka API request type has its own handler with request parsing and response building logic.

### Topic & Partition
Domain logic for managing broker state and partition data persistence.

### Parser
Reusable utilities for Kafka binary protocol encoding/decoding (varints, compact strings, etc.).

### Errors
Centralized Kafka error code definitions.

### Logger
Colored output for better debugging and monitoring.

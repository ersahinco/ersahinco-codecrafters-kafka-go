# Clean Architecture Refactoring

## Before: Monolithic Structure
```
app/
└── main.go (900+ lines)
    ├── Connection handling
    ├── Request parsing
    ├── All API handlers
    ├── Topic management
    ├── Partition I/O
    ├── Binary parsing
    └── Error handling
```

## After: Layered Architecture
```
┌─────────────────────────────────────────────────────────┐
│                       main.go                           │
│              (Entry point - 38 lines)                   │
└────────────────────┬────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────┐
│                   server/server.go                      │
│         (Connection & Request Routing)                  │
│  • TCP connection handling                              │
│  • Request frame parsing                                │
│  • API key routing                                      │
└──────┬──────────────────────────────────────────────────┘
       │
       ├──────────────────────────────────────────────┐
       │                                              │
       ▼                                              ▼
┌─────────────────────┐                    ┌──────────────────────┐
│  handlers/          │                    │  topic/topic.go      │
│  ├── apiversion.go  │◄───────────────────┤  • BrokerState       │
│  ├── fetchtopic.go  │                    │  • Topic metadata    │
│  ├── producetopic.go│                    │  • Properties loader │
│  └── describetopic  │                    └──────────────────────┘
│                     │                              │
│  Each handler:      │                              │
│  • Parses request   │                              ▼
│  • Validates data   │                    ┌──────────────────────┐
│  • Builds response  │                    │ partition/           │
└──────┬──────────────┘                    │ partition.go         │
       │                                   │  • ReadRecords()     │
       │                                   │  • WriteRecords()    │
       │                                   └──────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────┐
│              parser/elements.go                         │
│         (Binary Protocol Utilities)                     │
│  • BytesReader                                          │
│  • ReadInt8/16/32/64, ReadVarInt, ReadUVarInt          │
│  • AppendInt8/16/32/64, AppendVarInt, AppendUVarInt    │
│  • ReadCompactString, AppendCompactString              │
│  • UUID parsing                                         │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│              errors/custom.go                           │
│         (Kafka Error Codes)                             │
│  • ErrNone = 0                                          │
│  • ErrUnknownTopicOrPartition = 3                       │
│  • ErrUnsupportedVersion = 35                           │
│  • ErrUnknownTopicID = 100                              │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│              logger/colored.go                          │
│         (Colored Console Output)                        │
│  • Info()    - Cyan                                     │
│  • Success() - Green                                    │
│  • Warn()    - Yellow                                   │
│  • Error()   - Red                                      │
│  • Debug()   - Blue                                     │
└─────────────────────────────────────────────────────────┘
```

## Data Flow Example: Produce Request

```
1. Client sends Produce request
   ↓
2. server.HandleConnection() reads TCP frame
   ↓
3. server.readRequest() parses header (API key, version, correlation ID)
   ↓
4. Routes to handlers.HandleProduceV11()
   ↓
5. Handler parses request body → ProduceTopicRequest
   ↓
6. Validates topic exists in topic.BrokerState
   ↓
7. Calls partition.WriteRecords() to persist data
   ↓
8. Builds response using parser.Append*() functions
   ↓
9. Returns framed response to client
```

## Key Improvements

### 1. Single Responsibility Principle
Each package has one clear purpose:
- `server`: Network I/O
- `handlers`: Request/response logic
- `topic`: Domain state
- `partition`: Data persistence
- `parser`: Protocol encoding/decoding
- `errors`: Error definitions
- `logger`: Output formatting

### 2. Dependency Inversion
- High-level modules (handlers) don't depend on low-level details
- All modules depend on abstractions (interfaces via function calls)

### 3. Open/Closed Principle
- Easy to add new handlers without modifying existing code
- New API versions can be added as separate functions

### 4. DRY (Don't Repeat Yourself)
- Binary parsing logic centralized in `parser` package
- Error codes defined once in `errors` package
- Logging utilities reused across all packages

## Testing Strategy

With this structure, you can now easily:

```go
// Unit test a handler in isolation
func TestHandleProduceV11(t *testing.T) {
    state := &topic.BrokerState{
        Topics: map[string]topic.Meta{
            "test-topic": {ID: [16]byte{1,2,3}, Partitions: 1},
        },
    }
    
    reqBody := buildMockProduceRequest()
    resp := handlers.HandleProduceV11(123, reqBody, state)
    
    // Assert response structure
}

// Mock partition I/O for testing
type MockPartition struct{}
func (m *MockPartition) WriteRecords(...) error { return nil }
```

## Performance Considerations

- **Memory**: Reduced allocations by reusing byte slices
- **Concurrency**: Each connection handled in separate goroutine
- **I/O**: Direct file operations for partition data
- **Parsing**: Zero-copy reads where possible using BytesReader

## Future Extensions

Easy to add:
- Metrics collection (add to server layer)
- Request tracing (add to handlers)
- Caching layer (add between handlers and partition)
- Alternative storage backends (implement partition interface)
- Protocol versioning (add version-specific handlers)

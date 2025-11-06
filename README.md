# Build Your Own Kafka

A clean, pragmatic implementation of a Kafka broker in Go for the [CodeCrafters Kafka Challenge](https://codecrafters.io/challenges/kafka).

## Features

- ApiVersions request handling (v0-v4)
- DescribeTopicPartitions request handling (v0)
- Kafka wire protocol encoding/decoding
- Flexible message format support (compact types, tagged fields)
- Concurrent client connections
- Topic configuration via properties file

## Running

```sh
./your_program.sh /tmp/server.properties
```

Or use the Makefile:

```sh
make run
```

## Testing

```sh
codecrafters test
```

## Implementation Notes

### The Tricky Bug: Response Header Versions

The most challenging issue during development was understanding Kafka's response header versions:

**Problem**: DescribeTopicPartitions v0 responses were being rejected by the tester despite sending correct data.

**Root Cause**: Kafka uses different response header versions depending on the API:
- **Response Header v0**: Just `correlation_id` (4 bytes) - used by older APIs
- **Response Header v1**: `correlation_id` + `TAG_BUFFER` (4 bytes + 1 byte) - used by flexible APIs

**The Fix**: DescribeTopicPartitions v0 is a flexible API and requires Response Header v1, which includes a TAG_BUFFER byte after the correlation ID. Adding this single byte fixed all tests.

**Additional Gotchas**:
1. Request headers can have tagged fields that must be skipped (not just a zero byte)
2. Request bodies may have a TAG_BUFFER before the main fields
3. Compact arrays encode length as `actual_length + 1`
4. UUID slicing requires storing the array in a variable first (Go limitation)

### Architecture

The code is structured for clarity and maintainability:
- `readRequest()` - Handles frame reading and header parsing
- `parseTopicRequests()` - Extracts topic names from request body
- `handleDescribeTopicPartitionsV0()` - Builds the response
- Clean separation between encoding/decoding helpers

## Properties File Format

```properties
topic.<name>.id=<uuid>
topic.<name>.partitions=<N>
```

Example:
```properties
topic.alpha.id=11111111-2222-3333-4444-555555555555
topic.alpha.partitions=2
```

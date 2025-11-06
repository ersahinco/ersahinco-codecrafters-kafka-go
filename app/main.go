package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	apiKeyApiVersions = 18
	errorNone         = int16(0)
	errorUnsupported  = int16(35) // UNSUPPORTED_VERSION
)

func main() {
	fmt.Println("Kafka-ish broker (ApiVersions v4 body) on :9092")

	ln, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConn(conn)
	}
}

func handleConn(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		// 1) read 4-byte message_size
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, sizeBuf); err != nil {
			return
		}
		msgSize := int32(binary.BigEndian.Uint32(sizeBuf))
		if msgSize <= 0 {
			return
		}

		// 2) read payload
		payload := make([]byte, msgSize)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}

		// 3) parse request header v2 (first 8 bytes)
		if len(payload) < 8 {
			return
		}
		apiKey := int16(binary.BigEndian.Uint16(payload[0:2]))
		apiVersion := int16(binary.BigEndian.Uint16(payload[2:4]))
		corrID := int32(binary.BigEndian.Uint32(payload[4:8]))

		// 4) handle ApiVersions
		if apiKey == apiKeyApiVersions {
			supported := apiVersion >= 0 && apiVersion <= 4
			errCode := errorNone
			if !supported {
				errCode = errorUnsupported
			}
			resp := buildApiVersionsV4Response(corrID, errCode, supported)
			_ = writeAll(conn, resp)
			continue
		}

		// 5) fallback: minimal echo of correlation_id (8 bytes total)
		resp := make([]byte, 8)
		binary.BigEndian.PutUint32(resp[0:4], 0)              // message_size (placeholder)
		binary.BigEndian.PutUint32(resp[4:8], uint32(corrID)) // correlation_id
		_ = writeAll(conn, resp)
	}
}

// buildApiVersionsV4Response assembles the full v4 body:
// body:
//   int16 error_code
//   COMPACT_ARRAY api_keys:
//       UVarInt (N+1); for each entry:
//         int16 api_key
//         int16 min_version
//         int16 max_version
//         TAGGED_FIELDS (UVarInt 0)
//   int32 throttle_time_ms
//   TAGGED_FIELDS (UVarInt 0)
//
// If supported==true, include (only) one entry for ApiVersions: (18, 0, 4).
// If supported==false, include an empty array.
func buildApiVersionsV4Response(corrID int32, errorCode int16, supported bool) []byte {
	// Header v0: correlation_id
	header := make([]byte, 4)
	binary.BigEndian.PutUint32(header, uint32(corrID))

	// Body buffer
	body := make([]byte, 0, 32)

	// error_code
	body = appendInt16(body, errorCode)

	// api_keys COMPACT_ARRAY
	if supported {
		// N = 1 -> length varint = N + 1 = 2 -> 0x02
		body = appendUVarInt(body, 2)
		// entry: (api_key=18, min=0, max=4)
		body = appendInt16(body, apiKeyApiVersions)
		body = appendInt16(body, 0)
		body = appendInt16(body, 4)
		// entry TAG_BUFFER (no tags) -> UVarInt 0
		body = appendUVarInt(body, 0)
	} else {
		// N = 0 -> length varint = 1 -> 0x01 (empty, not null)
		body = appendUVarInt(body, 1)
	}

	// throttle_time_ms = 0
	body = appendInt32(body, 0)

	// response TAG_BUFFER (no tags)
	body = appendUVarInt(body, 0)

	// message_size = len(header) + len(body)
	total := len(header) + len(body)
	size := make([]byte, 4)
	binary.BigEndian.PutUint32(size, uint32(total))

	// final: size + header + body
	out := make([]byte, 0, 4+total)
	out = append(out, size...)
	out = append(out, header...)
	out = append(out, body...)
	return out
}

func appendInt16(b []byte, v int16) []byte {
	var tmp [2]byte
	binary.BigEndian.PutUint16(tmp[:], uint16(v))
	return append(b, tmp[:]...)
}

func appendInt32(b []byte, v int32) []byte {
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], uint32(v))
	return append(b, tmp[:]...)
}

// appendUVarInt encodes an unsigned varint (Kafka's base-128 varint).
func appendUVarInt(b []byte, x uint32) []byte {
	for {
		if (x & ^uint32(0x7F)) == 0 {
			b = append(b, byte(x))
			return b
		}
		b = append(b, byte(x&0x7F|0x80))
		x >>= 7
	}
}

func writeAll(w io.Writer, data []byte) error {
	for off := 0; off < len(data); {
		n, err := w.Write(data[off:])
		if err != nil {
			return err
		}
		off += n
	}
	return nil
}

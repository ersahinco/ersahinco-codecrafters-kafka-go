package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	apiKeyApiVersions        = int16(18)
	apiKeyDescribeTopicParts = int16(75)

	errNone                    = int16(0)
	errUnsupportedVersion      = int16(35) // 0x0023
	errUnknownTopicOrPartition = int16(3)

	maxFrameSize = 16 << 20 // 16 MiB safety guard
)

type TopicMeta struct {
	ID         [16]byte
	Partitions int // number of partitions (0..N-1)
}

type BrokerState struct {
	Topics map[string]TopicMeta // topic name -> meta
}

func main() {
	fmt.Println("Kafka-ish broker starting on :9092")

	state := BrokerState{Topics: map[string]TopicMeta{}}
	// optional properties path passed as first arg
	if len(os.Args) > 1 {
		if err := loadProps(os.Args[1], &state); err != nil {
			fmt.Println("WARN: failed to load properties:", err)
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}
		go handleConn(conn, &state)
	}
}

func loadProps(path string, state *BrokerState) error {
	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	lines := strings.Split(string(b), "\n")
	tmp := map[string]TopicMeta{}
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if !strings.HasPrefix(line, "topic.") {
			continue
		}
		// expected keys:
		// topic.<name>.id=<uuid>
		// topic.<name>.partitions=<N>
		kv := strings.SplitN(line, "=", 2)
		if len(kv) != 2 {
			continue
		}
		key, val := kv[0], strings.TrimSpace(kv[1])
		rest := strings.TrimPrefix(key, "topic.")
		dot := strings.LastIndex(rest, ".")
		if dot <= 0 || dot == len(rest)-1 {
			continue
		}
		name := rest[:dot]
		field := rest[dot+1:]

		meta := tmp[name]
		switch field {
		case "id":
			id, err := parseUUID(val)
			if err != nil {
				fmt.Println("WARN: invalid uuid for topic", name, ":", err)
				break
			}
			meta.ID = id
		case "partitions":
			n, err := strconv.Atoi(val)
			if err != nil || n < 0 {
				fmt.Println("WARN: invalid partitions for topic", name, ":", val)
				break
			}
			meta.Partitions = n
		}
		tmp[name] = meta
	}
	// commit
	for k, v := range tmp {
		state.Topics[k] = v
	}
	return nil
}

func handleConn(conn net.Conn, state *BrokerState) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		// read message_size
		sizeBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, sizeBuf); err != nil {
			return
		}
		msgSize := int32(binary.BigEndian.Uint32(sizeBuf))
		if msgSize <= 0 || msgSize > maxFrameSize {
			return
		}

		// read payload
		payload := make([]byte, msgSize)
		if _, err := io.ReadFull(r, payload); err != nil {
			return
		}

		// parse request header v2: first 8 bytes are fixed
		if len(payload) < 8 {
			return
		}
		apiKey := int16(binary.BigEndian.Uint16(payload[0:2]))
		apiVersion := int16(binary.BigEndian.Uint16(payload[2:4]))
		corrID := int32(binary.BigEndian.Uint32(payload[4:8]))

		// header v2 continues with: client_id (COMPACT_NULLABLE_STRING), header TAG_BUFFER (UVarInt)
		hbr := bytesReader{b: payload, off: 8}
		_, _ = readCompactNullableString(&hbr) // client_id (ignored; just advance)
		_ = readUVarInt(&hbr)                  // header TAG_BUFFER

		// the request body starts here (flexible)
		if hbr.off > len(payload) {
			return
		}
		body := payload[hbr.off:]

		switch apiKey {
		case apiKeyApiVersions:
			// Support 0..4; v4 uses flexible response
			supported := apiVersion >= 0 && apiVersion <= 4
			if !supported {
				resp := buildApiVersionsErrorOnly(corrID, errUnsupportedVersion)
				_ = writeAll(conn, resp)
				continue
			}
			resp := buildApiVersionsV4Body(corrID)
			_ = writeAll(conn, resp)

		case apiKeyDescribeTopicParts:
			// Only version 0 for these stages
			if apiVersion != 0 {
				resp := buildSimpleError(corrID, errUnsupportedVersion)
				_ = writeAll(conn, resp)
				continue
			}
			resp := handleDescribeTopicPartitionsV0(corrID, body, state)
			_ = writeAll(conn, resp)

		default:
			// Minimal echo: 8 bytes total: size=0 + correlation_id
			min := make([]byte, 8)
			binary.BigEndian.PutUint32(min[0:4], 0)
			binary.BigEndian.PutUint32(min[4:8], uint32(corrID))
			_ = writeAll(conn, min)
		}
	}
}

/* ---------------- ApiVersions ---------------- */

// error-only (older stage behavior)
func buildApiVersionsErrorOnly(corrID int32, errorCode int16) []byte {
	// Response header v1: correlation_id + header TAG_BUFFER (empty)
	header := make([]byte, 0, 5)
	header = appendInt32(header, corrID)
	header = appendUVarInt(header, 0) // header tags

	// Body: error_code(int16)
	body := make([]byte, 0, 2)
	body = appendInt16(body, errorCode)

	return frameResponse(header, body)
}

// Generic tiny error response: header v1 + body(error_code only)
func buildSimpleError(corrID int32, errorCode int16) []byte {
	header := make([]byte, 0, 5)
	header = appendInt32(header, corrID)
	header = appendUVarInt(header, 0) // header tags
	body := make([]byte, 0, 2)
	body = appendInt16(body, errorCode)
	return frameResponse(header, body)
}

// Full ApiVersions v4 body with two entries: (18:0..4) and (75:0..0)
func buildApiVersionsV4Body(corrID int32) []byte {
	// Response header v1
	header := make([]byte, 0, 5)
	header = appendInt32(header, corrID)
	header = appendUVarInt(header, 0) // header tags empty

	body := make([]byte, 0, 64)
	// error_code = 0
	body = appendInt16(body, errNone)

	// api_keys: compact array with 2 entries → length = count+1 = 3
	body = appendUVarInt(body, 3)

	// entry #1: ApiVersions (18, 0..4) + entry TAG_BUFFER=0
	body = appendInt16(body, apiKeyApiVersions)
	body = appendInt16(body, 0)
	body = appendInt16(body, 4)
	body = appendUVarInt(body, 0)

	// entry #2: DescribeTopicPartitions (75, 0..0) + entry TAG_BUFFER=0
	body = appendInt16(body, apiKeyDescribeTopicParts)
	body = appendInt16(body, 0)
	body = appendInt16(body, 0)
	body = appendUVarInt(body, 0)

	// throttle_time_ms = 0
	body = appendInt32(body, 0)

	// response TAG_BUFFER=0
	body = appendUVarInt(body, 0)

	return frameResponse(header, body)
}

/* ------------- DescribeTopicPartitions v0 ------------- */

func handleDescribeTopicPartitionsV0(corrID int32, reqBody []byte, state *BrokerState) []byte {
	br := bytesReader{b: reqBody}

	// topics: COMPACT_ARRAY of TopicRequest(name: COMPACT_STRING, TAG_BUFFER)
	nTopics := int(readUVarInt(&br)) - 1
	if nTopics < 0 {
		nTopics = 0
	}
	reqNames := make([]string, 0, nTopics)
	for i := 0; i < nTopics; i++ {
		name := readCompactString(&br)
		_ = readUVarInt(&br) // TopicRequest TAG_BUFFER (ignored)
		reqNames = append(reqNames, name)
	}

	// response_partition_limit
	_ = readInt32(&br)

	// cursor: OPTION<Cursor> (nullable struct).
	if br.canRead(1) {
		marker := br.b[br.off]
		if marker == 0xFF {
			br.off++
		} else {
			// Cursor struct: topic_name (compact string), partition_index (int32), TAG_BUFFER
			_ = readCompactString(&br)
			_ = readInt32(&br)
			_ = readUVarInt(&br) // cursor TAG_BUFFER
		}
	}

	// trailing request TAG_BUFFER
	if br.remaining() > 0 {
		_ = readUVarInt(&br)
	}

	// Build response
	// Response header v1
	header := make([]byte, 0, 5)
	header = appendInt32(header, corrID)
	header = appendUVarInt(header, 0) // header tags

	// Body
	body := make([]byte, 0, 256)
	// throttle_time_ms
	body = appendInt32(body, 0)

	// topics must be sorted alphabetically by name
	sort.Strings(reqNames)

	// topics: COMPACT_ARRAY
	body = appendUVarInt(body, uint32(len(reqNames)+1))
	for _, name := range reqNames {
		meta, ok := state.Topics[name]
		if !ok {
			// unknown topic
			body = appendInt16(body, errUnknownTopicOrPartition)
			body = appendCompactString(body, name)
			tmp := nilUUID()
			body = append(body, tmp[:]...) // topic_id = nil
			body = append(body, 0x00)      // is_internal: false
			// partitions: empty compact array
			body = appendUVarInt(body, 1)
			// topic_authorized_operations: INT32 default -2147483648
			body = appendInt32(body, int32(-2147483648))
			body = appendUVarInt(body, 0) // topic TAG_BUFFER
			continue
		}

		// known topic
		body = appendInt16(body, errNone)
		body = appendCompactString(body, name)
		body = append(body, meta.ID[:]...)
		body = append(body, 0x00) // is_internal: false

		// partitions: COMPACT_ARRAY with N entries
		N := meta.Partitions
		body = appendUVarInt(body, uint32(N+1))
		for p := 0; p < N; p++ {
			body = appendInt16(body, errNone)                // error_code
			body = appendInt32(body, int32(p))               // partition_index
			body = appendInt32(body, 0)                      // leader_id (dummy)
			body = appendInt32(body, int32(-1))              // leader_epoch = -1
			body = appendCompactInt32Array(body, []int32{0}) // replica_nodes
			body = appendCompactInt32Array(body, []int32{0}) // isr_nodes
			body = appendUVarInt(body, 1)                    // eligible_leader_replicas: empty
			body = appendUVarInt(body, 1)                    // last_known_elr: empty
			body = appendUVarInt(body, 1)                    // offline_replicas: empty
			body = appendUVarInt(body, 0)                    // partition TAG_BUFFER
		}
		body = appendInt32(body, int32(-2147483648)) // topic_authorized_operations
		body = appendUVarInt(body, 0)                // topic TAG_BUFFER
	}

	// next_cursor: null (single byte FF in flexible)
	body = append(body, 0xFF)

	// response TAG_BUFFER
	body = appendUVarInt(body, 0)

	return frameResponse(header, body)
}

/* ---------------- Encoding helpers ---------------- */

func frameResponse(header, body []byte) []byte {
	total := len(header) + len(body)
	out := make([]byte, 0, 4+total)
	out = appendInt32(out, int32(total)) // message_size excludes these 4 bytes
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

// Kafka unsigned varint (LEB128)
func appendUVarInt(b []byte, x uint32) []byte {
	for {
		if (x & ^uint32(0x7F)) == 0 {
			return append(b, byte(x))
		}
		b = append(b, byte(x&0x7F|0x80))
		x >>= 7
	}
}

func appendCompactString(b []byte, s string) []byte {
	b = appendUVarInt(b, uint32(len(s)+1))
	return append(b, []byte(s)...)
}

func appendCompactInt32Array(b []byte, xs []int32) []byte {
	b = appendUVarInt(b, uint32(len(xs)+1))
	for _, v := range xs {
		b = appendInt32(b, v)
	}
	return b
}

func nilUUID() [16]byte {
	return [16]byte{}
}

func parseUUID(in string) ([16]byte, error) {
	var out [16]byte
	s := strings.ReplaceAll(strings.TrimSpace(in), "-", "")
	if len(s) != 32 {
		return out, fmt.Errorf("uuid must be 32 hex chars (no dashes), got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	copy(out[:], b[:16])
	return out, nil
}

/* ---------------- Decoding helpers (request) ---------------- */

type bytesReader struct {
	b   []byte
	off int
}

func (br *bytesReader) canRead(n int) bool { return br.off+n <= len(br.b) }
func (br *bytesReader) remaining() int     { return len(br.b) - br.off }

func readInt32(br *bytesReader) int32 {
	if !br.canRead(4) {
		return 0
	}
	v := int32(binary.BigEndian.Uint32(br.b[br.off : br.off+4]))
	br.off += 4
	return v
}

func readUVarInt(br *bytesReader) uint32 {
	var x uint32
	var s uint
	for i := 0; i < 5; i++ {
		if !br.canRead(1) {
			return x
		}
		b := br.b[br.off]
		br.off++
		if b < 0x80 {
			if i == 4 && b > 1 {
				return x
			}
			return x | uint32(b)<<s
		}
		x |= uint32(b&0x7F) << s
		s += 7
	}
	return x
}

func readCompactString(br *bytesReader) string {
	l := int(readUVarInt(br)) - 1
	if l <= 0 {
		return ""
	}
	if !br.canRead(l) {
		return ""
	}
	s := string(br.b[br.off : br.off+l])
	br.off += l
	return s
}

// Compact nullable string: if length (uvarint) == 0 => null
func readCompactNullableString(br *bytesReader) (string, bool) {
	l := int(readUVarInt(br))
	if l == 0 {
		return "", true
	}
	n := l - 1
	if n < 0 || !br.canRead(n) {
		return "", false
	}
	s := string(br.b[br.off : br.off+n])
	br.off += n
	return s, false
}

/* ---------------- IO ---------------- */

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

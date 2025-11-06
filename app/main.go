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
	Partitions int
}

type BrokerState struct {
	Topics map[string]TopicMeta
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
	
	tmp := map[string]TopicMeta{}
	for _, line := range strings.Split(string(b), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") || !strings.HasPrefix(line, "topic.") {
			continue
		}
		
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
		
		name, field := rest[:dot], rest[dot+1:]
		meta := tmp[name]
		
		switch field {
		case "id":
			if id, err := parseUUID(val); err == nil {
				meta.ID = id
			} else {
				fmt.Println("WARN: invalid uuid for topic", name, ":", err)
			}
		case "partitions":
			if n, err := strconv.Atoi(val); err == nil && n >= 0 {
				meta.Partitions = n
			} else {
				fmt.Println("WARN: invalid partitions for topic", name, ":", val)
			}
		}
		tmp[name] = meta
	}
	
	for k, v := range tmp {
		state.Topics[k] = v
	}
	return nil
}

func handleConn(conn net.Conn, state *BrokerState) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		payload, corrID, apiKey, apiVersion, err := readRequest(r)
		if err != nil {
			return
		}

		var resp []byte
		switch apiKey {
		case apiKeyApiVersions:
			if apiVersion < 0 || apiVersion > 4 {
				resp = buildApiVersionsErrorOnly(corrID, errUnsupportedVersion)
			} else {
				resp = buildApiVersionsV4Body(corrID)
			}
		case apiKeyDescribeTopicParts:
			if apiVersion != 0 {
				resp = buildSimpleError(corrID, errUnsupportedVersion)
			} else {
				resp = handleDescribeTopicPartitionsV0(corrID, payload, state)
			}
		default:
			resp = frameResponse(appendInt32(nil, corrID), nil)
		}
		
		if writeAll(conn, resp) != nil {
			return
		}
	}
}

func readRequest(r *bufio.Reader) (body []byte, corrID int32, apiKey, apiVersion int16, err error) {
	var sizeBuf [4]byte
	if _, err = io.ReadFull(r, sizeBuf[:]); err != nil {
		return
	}
	
	msgSize := int32(binary.BigEndian.Uint32(sizeBuf[:]))
	if msgSize <= 0 || msgSize > maxFrameSize {
		err = fmt.Errorf("invalid message size")
		return
	}

	payload := make([]byte, msgSize)
	if _, err = io.ReadFull(r, payload); err != nil {
		return
	}

	if len(payload) < 8 {
		err = fmt.Errorf("payload too short")
		return
	}
	
	apiKey = int16(binary.BigEndian.Uint16(payload[0:2]))
	apiVersion = int16(binary.BigEndian.Uint16(payload[2:4]))
	corrID = int32(binary.BigEndian.Uint32(payload[4:8]))

	hbr := bytesReader{b: payload, off: 8}
	_, _ = readCompactNullableString(&hbr) // client_id
	
	// Read and skip TAG_BUFFER
	tagBufLen := readUVarInt(&hbr)
	if tagBufLen > 0 && hbr.canRead(int(tagBufLen)) {
		hbr.off += int(tagBufLen) // Skip tagged fields
	}

	if hbr.off > len(payload) {
		err = fmt.Errorf("invalid header")
		return
	}
	
	body = payload[hbr.off:]
	return
}

/* ---------------- ApiVersions ---------------- */

func buildApiVersionsErrorOnly(corrID int32, errorCode int16) []byte {
	return buildSimpleError(corrID, errorCode)
}

func buildSimpleError(corrID int32, errorCode int16) []byte {
	header := appendInt32(nil, corrID)
	body := appendInt16(nil, errorCode)
	return frameResponse(header, body)
}

func buildApiVersionsV4Body(corrID int32) []byte {
	header := appendInt32(nil, corrID)
	
	body := appendInt16(nil, errNone)
	body = appendUVarInt(body, 3) // 2 entries + 1
	
	// ApiVersions (18, 0..4)
	body = appendInt16(body, apiKeyApiVersions)
	body = appendInt16(body, 0)
	body = appendInt16(body, 4)
	body = appendUVarInt(body, 0)
	
	// DescribeTopicPartitions (75, 0..0)
	body = appendInt16(body, apiKeyDescribeTopicParts)
	body = appendInt16(body, 0)
	body = appendInt16(body, 0)
	body = appendUVarInt(body, 0)
	
	body = appendInt32(body, 0)  // throttle_time_ms
	body = appendUVarInt(body, 0) // TAG_BUFFER

	return frameResponse(header, body)
}

/* ------------- DescribeTopicPartitions v0 ------------- */

func handleDescribeTopicPartitionsV0(corrID int32, reqBody []byte, _ *BrokerState) []byte {
	reqNames := parseTopicRequests(reqBody)
	sort.Strings(reqNames)

	// Response header v1 (flexible): correlation_id + TAG_BUFFER
	header := appendInt32(nil, corrID)
	header = appendUVarInt(header, 0) // header TAG_BUFFER
	
	body := appendInt32(nil, 0) // throttle_time_ms
	body = appendUVarInt(body, uint32(len(reqNames)+1))
	
	for _, name := range reqNames {
		body = appendInt16(body, errUnknownTopicOrPartition)
		body = appendCompactString(body, name)
		uuid := nilUUID()
		body = append(body, uuid[:]...)
		body = append(body, 0x00)         // is_internal
		body = appendUVarInt(body, 1)     // empty partitions array
		body = appendInt32(body, -2147483648)
		body = appendUVarInt(body, 0)     // TAG_BUFFER
	}
	
	body = append(body, 0xFF)         // next_cursor: null
	body = appendUVarInt(body, 0)     // TAG_BUFFER

	return frameResponse(header, body)
}

func parseTopicRequests(reqBody []byte) []string {
	br := bytesReader{b: reqBody}
	
	// Skip request-level TAG_BUFFER
	_ = readUVarInt(&br)
	
	// topics: COMPACT_ARRAY of TopicRequest
	nTopics := int(readUVarInt(&br)) - 1
	if nTopics < 0 {
		return nil
	}
	
	names := make([]string, 0, nTopics)
	for i := 0; i < nTopics; i++ {
		name := readCompactString(&br)
		_ = readUVarInt(&br) // TopicRequest TAG_BUFFER
		names = append(names, name)
	}
	return names
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

func nilUUID() [16]byte {
	return [16]byte{}
}

func parseUUID(in string) ([16]byte, error) {
	var out [16]byte
	s := strings.ReplaceAll(strings.TrimSpace(in), "-", "")
	if len(s) != 32 {
		return out, fmt.Errorf("uuid must be 32 hex chars, got %d", len(s))
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return out, err
	}
	copy(out[:], b)
	return out, nil
}

type bytesReader struct {
	b   []byte
	off int
}

func (br *bytesReader) canRead(n int) bool { return br.off+n <= len(br.b) }

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

func writeAll(w io.Writer, data []byte) error {
	_, err := w.Write(data)
	return err
}

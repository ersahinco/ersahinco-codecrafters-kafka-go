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
	apiKeyFetch              = int16(1)
	apiKeyApiVersions        = int16(18)
	apiKeyDescribeTopicParts = int16(75)

	errNone                    = int16(0)
	errUnknownTopicOrPartition = int16(3)
	errUnsupportedVersion      = int16(35)  // 0x0023
	errUnknownTopicID          = int16(100) // 0x0064

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
	// First try to load from cluster metadata log
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	if err := loadClusterMetadata(logPath, state); err == nil {
		return nil
	}
	
	// Fallback to simple properties file format
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

func loadClusterMetadata(logPath string, state *BrokerState) error {
	data, err := os.ReadFile(logPath)
	if err != nil {
		return err
	}

	
	// Parse Kafka log file
	// The log contains record batches with topic metadata
	// We'll scan for TopicRecord (type 2) and PartitionRecord (type 3) entries
	
	// First pass: collect topic records
	topicRecords := make(map[string]TopicMeta)
	partitionCounts := make(map[[16]byte]int) // Count partitions by topic UUID
	
	offset := 0
	for offset < len(data)-20 {
		// Each batch starts with: baseOffset(8) + batchLength(4) + ...
		if offset+12 > len(data) {
			break
		}
		
		batchLen := int(binary.BigEndian.Uint32(data[offset+8 : offset+12]))
		if batchLen <= 0 || batchLen > len(data)-offset-12 {
			offset++
			continue
		}
		
		batchEnd := offset + 12 + batchLen
		if batchEnd > len(data) {
			break
		}
		
		// Parse records within this batch
		// Skip batch header (61 bytes total from start)
		recordsStart := offset + 61
		if recordsStart >= batchEnd {
			offset = batchEnd
			continue
		}
		
		// Parse records
		parseRecords(data[recordsStart:batchEnd], topicRecords, partitionCounts)
		offset = batchEnd
	}
	
	// Merge partition counts into topic metadata
	for name, meta := range topicRecords {
		if count, ok := partitionCounts[meta.ID]; ok && count > 0 {
			meta.Partitions = count
		} else if meta.Partitions == 0 {
			meta.Partitions = 1 // Default to 1 partition
		}
		state.Topics[name] = meta
	}
	

	
	if len(state.Topics) == 0 {
		return fmt.Errorf("no topics found in cluster metadata")
	}
	return nil
}

func parseRecords(data []byte, topicRecords map[string]TopicMeta, partitionCounts map[[16]byte]int) {
	br := bytesReader{b: data}
	
	for br.off < len(data)-5 {
		// Record: length(varint) + attributes(int8) + timestampDelta(varint) + offsetDelta(varint) + key + value + headers
		recLen := int(readVarInt(&br))
		if recLen <= 0 || br.off+recLen > len(data) {
			break
		}
		
		recStart := br.off
		_ = readInt8(&br)          // attributes
		_ = readVarInt(&br)        // timestampDelta
		_ = readVarInt(&br)        // offsetDelta
		keyLen := int(readVarInt(&br))
		
		// Skip key if present (or handle null key)
		if keyLen > 0 && br.canRead(keyLen) {
			br.off += keyLen
		}
		
		// Read value
		valueLen := int(readVarInt(&br))
		
		if valueLen > 0 && br.canRead(valueLen) {
			valueData := br.b[br.off : br.off+valueLen]
			
			// Parse value to determine record type
			if len(valueData) >= 2 {
				recordType := valueData[1]
				
				// TopicRecord type = 2
				if recordType == 2 {
					parseTopicRecordValue(valueData, topicRecords)
				} else if recordType == 3 {
					// PartitionRecord type = 3
					parsePartitionRecordValue(valueData, partitionCounts)
				}
			}
		}
		
		// Skip to next record
		br.off = recStart + recLen
	}
}

func parseTopicRecordValue(data []byte, topicRecords map[string]TopicMeta) {
	if len(data) < 20 {
		return
	}
	
	br := bytesReader{b: data}
	_ = readInt8(&br) // frame version
	_ = readInt8(&br) // record type (should be 2)
	_ = readUVarInt(&br) // TAG_BUFFER
	
	// Read topic name (compact string using uvarint)
	nameLen := int(readUVarInt(&br)) - 1
	if nameLen <= 0 || !br.canRead(nameLen) {
		return
	}
	name := string(br.b[br.off : br.off+nameLen])
	br.off += nameLen
	
	// Read topic ID (UUID - 16 bytes)
	if !br.canRead(16) {
		return
	}
	var topicID [16]byte
	copy(topicID[:], br.b[br.off:br.off+16])
	br.off += 16
	
	meta := TopicMeta{
		ID:         topicID,
		Partitions: 0, // Will be updated when we parse PartitionRecords
	}
	topicRecords[name] = meta
}

func parsePartitionRecordValue(data []byte, partitionCounts map[[16]byte]int) {
	if len(data) < 20 {
		return
	}
	
	br := bytesReader{b: data}
	_ = readInt8(&br) // frame version
	_ = readInt8(&br) // record type (should be 3)
	_ = readUVarInt(&br) // TAG_BUFFER
	
	// Read partition_id (int32)
	if !br.canRead(4) {
		return
	}
	_ = readInt32(&br) // partition_id (we just need to count them)
	
	// Read topic_id (UUID - 16 bytes)
	if !br.canRead(16) {
		return
	}
	var topicID [16]byte
	copy(topicID[:], br.b[br.off:br.off+16])
	
	// Increment partition count for this topic
	partitionCounts[topicID]++
}

func readInt8(br *bytesReader) int8 {
	if !br.canRead(1) {
		return 0
	}
	v := int8(br.b[br.off])
	br.off++
	return v
}

func readVarInt(br *bytesReader) int64 {
	var x uint64
	var s uint
	for i := 0; i < 10; i++ {
		if !br.canRead(1) {
			return int64(x >> 1) ^ -(int64(x) & 1)
		}
		b := br.b[br.off]
		br.off++
		if b < 0x80 {
			return int64(x|uint64(b)<<s) >> 1 ^ -(int64(x|uint64(b)<<s) & 1)
		}
		x |= uint64(b&0x7F) << s
		s += 7
	}
	return 0
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
		case apiKeyFetch:
			if apiVersion != 16 {
				resp = buildSimpleError(corrID, errUnsupportedVersion)
			} else {
				resp = handleFetchV16(corrID, payload, state)
			}
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
	body = appendUVarInt(body, 4) // 3 entries + 1
	
	// Fetch (1, 0..16)
	body = appendInt16(body, apiKeyFetch)
	body = appendInt16(body, 0)
	body = appendInt16(body, 16)
	body = appendUVarInt(body, 0)
	
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

func handleDescribeTopicPartitionsV0(corrID int32, reqBody []byte, state *BrokerState) []byte {
	reqNames := parseTopicRequests(reqBody)
	sort.Strings(reqNames)

	// Response header v1 (flexible): correlation_id + TAG_BUFFER
	header := appendInt32(nil, corrID)
	header = appendUVarInt(header, 0) // header TAG_BUFFER
	
	body := appendInt32(nil, 0) // throttle_time_ms
	body = appendUVarInt(body, uint32(len(reqNames)+1))
	
	for _, name := range reqNames {
		meta, exists := state.Topics[name]
		
		if !exists {
			// Topic not found
			body = appendInt16(body, errUnknownTopicOrPartition)
			body = appendCompactString(body, name)
			uuid := nilUUID()
			body = append(body, uuid[:]...)
			body = append(body, 0x00)         // is_internal
			body = appendUVarInt(body, 1)     // empty partitions array
			body = appendInt32(body, -2147483648)
			body = appendUVarInt(body, 0)     // TAG_BUFFER
		} else {
			// Topic found - return actual data
			body = appendInt16(body, errNone) // error_code = 0
			body = appendCompactString(body, name)
			body = append(body, meta.ID[:]...) // topic_id (UUID)
			body = append(body, 0x00)          // is_internal = false
			
			// partitions array (COMPACT_ARRAY)
			numPartitions := meta.Partitions
			if numPartitions == 0 {
				numPartitions = 1 // Default to 1 partition
			}
			body = appendUVarInt(body, uint32(numPartitions+1))
			
			for partIdx := 0; partIdx < numPartitions; partIdx++ {
				body = appendInt16(body, errNone)           // error_code = 0
				body = appendInt32(body, int32(partIdx))    // partition_index
				body = appendInt32(body, 1)                 // leader_id (broker 1)
				body = appendInt32(body, -1)                // leader_epoch
				body = appendUVarInt(body, 2)               // replica_nodes (1 replica)
				body = appendInt32(body, 1)                 // replica node 1
				body = appendUVarInt(body, 2)               // isr_nodes (1 node)
				body = appendInt32(body, 1)                 // isr node 1
				body = appendUVarInt(body, 1)               // eligible_leader_replicas (empty)
				body = appendUVarInt(body, 1)               // last_known_elr (empty)
				body = appendUVarInt(body, 1)               // offline_replicas (empty)
				body = appendUVarInt(body, 0)               // TAG_BUFFER
			}
			
			body = appendInt32(body, -2147483648)       // topic_authorized_operations
			body = appendUVarInt(body, 0)               // TAG_BUFFER
		}
	}
	
	body = append(body, 0xFF)         // next_cursor: null (-1 as signed byte)
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

/* ------------- Fetch v16 ------------- */

func handleFetchV16(corrID int32, reqBody []byte, state *BrokerState) []byte {
	// Parse the Fetch request to get topic IDs
	topicIDs := parseFetchRequestV16(reqBody)
	
	// Response header v1 (flexible): correlation_id + TAG_BUFFER
	header := appendInt32(nil, corrID)
	header = appendUVarInt(header, 0) // header TAG_BUFFER
	
	// Fetch Response v16 body
	body := appendInt32(nil, 0)       // throttle_time_ms = 0
	body = appendInt16(body, errNone) // error_code = 0
	body = appendInt32(body, 0)       // session_id = 0
	
	// responses array (COMPACT_ARRAY)
	body = appendUVarInt(body, uint32(len(topicIDs)+1))
	
	for _, topicID := range topicIDs {
		// Check if topic exists and get topic name
		var topicName string
		exists := false
		for name, meta := range state.Topics {
			if meta.ID == topicID {
				exists = true
				topicName = name
				break
			}
		}
		
		// FetchableTopicResponse
		body = append(body, topicID[:]...)    // topic_id (UUID)
		body = appendUVarInt(body, 2)         // partitions array (1 partition)
		
		// PartitionData
		body = appendInt32(body, 0)           // partition_index = 0
		if !exists {
			body = appendInt16(body, errUnknownTopicID) // error_code = 100
			body = appendInt64(body, 0)           // high_watermark = 0
			body = appendInt64(body, 0)           // last_stable_offset = 0
			body = appendInt64(body, 0)           // log_start_offset = 0
			body = appendUVarInt(body, 1)         // aborted_transactions (empty)
			body = appendInt32(body, 0)           // preferred_read_replica = 0
			body = appendUVarInt(body, 1)         // records (empty compact bytes)
			body = appendUVarInt(body, 0)         // TAG_BUFFER
		} else {
			// Topic exists - read records from disk
			records := readPartitionRecords(topicName, 0)
			
			body = appendInt16(body, errNone)     // error_code = 0
			body = appendInt64(body, 1)           // high_watermark = 1 (we have 1 message)
			body = appendInt64(body, 0)           // last_stable_offset = 0
			body = appendInt64(body, 0)           // log_start_offset = 0
			body = appendUVarInt(body, 1)         // aborted_transactions (empty)
			body = appendInt32(body, 0)           // preferred_read_replica = 0
			
			// records: COMPACT_BYTES (length + data)
			if len(records) > 0 {
				body = appendUVarInt(body, uint32(len(records)+1))
				body = append(body, records...)
			} else {
				body = appendUVarInt(body, 1) // empty
			}
			body = appendUVarInt(body, 0)         // TAG_BUFFER
		}
		
		body = appendUVarInt(body, 0)         // FetchableTopicResponse TAG_BUFFER
	}
	
	body = appendUVarInt(body, 0)     // TAG_BUFFER

	return frameResponse(header, body)
}

func readPartitionRecords(topicName string, partition int32) []byte {
	// Construct log file path
	logPath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partition)
	
	// Read the entire log file
	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil
	}
	
	// Return the raw record batch data
	// The log file contains the complete RecordBatch which we return as-is
	return data
}

func parseFetchRequestV16(reqBody []byte) [][16]byte {
	br := bytesReader{b: reqBody}
	
	// Fetch Request v16 fields (in order from Kafka protocol)
	// cluster_id: COMPACT_NULLABLE_STRING (added in v12)
	_ = readCompactString(&br) // cluster_id
	
	_ = readInt32(&br) // max_wait_ms
	_ = readInt32(&br) // min_bytes
	_ = readInt32(&br) // max_bytes
	_ = readInt8(&br)  // isolation_level
	_ = readInt32(&br) // session_id
	_ = readInt32(&br) // session_epoch
	
	// topics: COMPACT_ARRAY of FetchTopic
	nTopics := int(readUVarInt(&br)) - 1
	if nTopics < 0 {
		return nil
	}
	
	topicIDs := make([][16]byte, 0, nTopics)
	for i := 0; i < nTopics; i++ {
		// Read topic_id (UUID - 16 bytes)
		if !br.canRead(16) {
			break
		}
		var topicID [16]byte
		copy(topicID[:], br.b[br.off:br.off+16])
		br.off += 16
		topicIDs = append(topicIDs, topicID)
		
		// Skip partitions array and TAG_BUFFER
		nPartitions := int(readUVarInt(&br)) - 1
		for j := 0; j < nPartitions; j++ {
			_ = readInt32(&br)   // partition
			_ = readInt32(&br)   // current_leader_epoch
			_ = readInt64(&br)   // fetch_offset
			_ = readInt64(&br)   // last_fetched_epoch
			_ = readInt64(&br)   // log_start_offset
			_ = readInt32(&br)   // partition_max_bytes
			_ = readUVarInt(&br) // TAG_BUFFER
		}
		_ = readUVarInt(&br) // FetchTopic TAG_BUFFER
	}
	
	return topicIDs
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

func appendInt64(b []byte, v int64) []byte {
	var tmp [8]byte
	binary.BigEndian.PutUint64(tmp[:], uint64(v))
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

func readInt64(br *bytesReader) int64 {
	if !br.canRead(8) {
		return 0
	}
	v := int64(binary.BigEndian.Uint64(br.b[br.off : br.off+8]))
	br.off += 8
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

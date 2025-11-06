package topic

import (
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/kafka-starter-go/app/logger"
	"github.com/codecrafters-io/kafka-starter-go/app/parser"
)

type Meta struct {
	ID         [16]byte
	Partitions int
}

type BrokerState struct {
	Topics map[string]Meta
}

func LoadFromProperties(path string, state *BrokerState) error {
	logPath := "/tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log"
	if err := loadClusterMetadata(logPath, state); err == nil {
		return nil
	}

	b, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	tmp := map[string]Meta{}
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
			if id, err := parser.ParseUUID(val); err == nil {
				meta.ID = id
			} else {
				logger.Warn("invalid uuid for topic %s: %v", name, err)
			}
		case "partitions":
			if n, err := strconv.Atoi(val); err == nil && n >= 0 {
				meta.Partitions = n
			} else {
				logger.Warn("invalid partitions for topic %s: %s", name, val)
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

	topicRecords := make(map[string]Meta)
	partitionCounts := make(map[[16]byte]int)

	offset := 0
	for offset < len(data)-20 {
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

		recordsStart := offset + 61
		if recordsStart >= batchEnd {
			offset = batchEnd
			continue
		}

		parseRecords(data[recordsStart:batchEnd], topicRecords, partitionCounts)
		offset = batchEnd
	}

	for name, meta := range topicRecords {
		if count, ok := partitionCounts[meta.ID]; ok && count > 0 {
			meta.Partitions = count
		} else if meta.Partitions == 0 {
			meta.Partitions = 1
		}
		state.Topics[name] = meta
	}

	if len(state.Topics) == 0 {
		return fmt.Errorf("no topics found in cluster metadata")
	}
	return nil
}

func parseRecords(data []byte, topicRecords map[string]Meta, partitionCounts map[[16]byte]int) {
	br := parser.BytesReader{B: data}

	for br.Off < len(data)-5 {
		recLen := int(parser.ReadVarInt(&br))
		if recLen <= 0 || br.Off+recLen > len(data) {
			break
		}

		recStart := br.Off
		_ = parser.ReadInt8(&br)
		_ = parser.ReadVarInt(&br)
		_ = parser.ReadVarInt(&br)
		keyLen := int(parser.ReadVarInt(&br))

		if keyLen > 0 && br.CanRead(keyLen) {
			br.Off += keyLen
		}

		valueLen := int(parser.ReadVarInt(&br))

		if valueLen > 0 && br.CanRead(valueLen) {
			valueData := br.B[br.Off : br.Off+valueLen]

			if len(valueData) >= 2 {
				recordType := valueData[1]

				if recordType == 2 {
					parseTopicRecordValue(valueData, topicRecords)
				} else if recordType == 3 {
					parsePartitionRecordValue(valueData, partitionCounts)
				}
			}
		}

		br.Off = recStart + recLen
	}
}

func parseTopicRecordValue(data []byte, topicRecords map[string]Meta) {
	if len(data) < 20 {
		return
	}

	br := parser.BytesReader{B: data}
	_ = parser.ReadInt8(&br)
	_ = parser.ReadInt8(&br)
	_ = parser.ReadUVarInt(&br)

	nameLen := int(parser.ReadUVarInt(&br)) - 1
	if nameLen <= 0 || !br.CanRead(nameLen) {
		return
	}
	name := string(br.B[br.Off : br.Off+nameLen])
	br.Off += nameLen

	if !br.CanRead(16) {
		return
	}
	var topicID [16]byte
	copy(topicID[:], br.B[br.Off:br.Off+16])
	br.Off += 16

	meta := Meta{
		ID:         topicID,
		Partitions: 0,
	}
	topicRecords[name] = meta
}

func parsePartitionRecordValue(data []byte, partitionCounts map[[16]byte]int) {
	if len(data) < 20 {
		return
	}

	br := parser.BytesReader{B: data}
	_ = parser.ReadInt8(&br)
	_ = parser.ReadInt8(&br)
	_ = parser.ReadUVarInt(&br)

	if !br.CanRead(4) {
		return
	}
	_ = parser.ReadInt32(&br)

	if !br.CanRead(16) {
		return
	}
	var topicID [16]byte
	copy(topicID[:], br.B[br.Off:br.Off+16])

	partitionCounts[topicID]++
}

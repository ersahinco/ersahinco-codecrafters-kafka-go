package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/errors"
	"github.com/codecrafters-io/kafka-starter-go/app/parser"
	"github.com/codecrafters-io/kafka-starter-go/app/partition"
	"github.com/codecrafters-io/kafka-starter-go/app/topic"
)

func HandleFetchV16(corrID int32, reqBody []byte, state *topic.BrokerState) []byte {
	topicIDs := parseFetchRequestV16(reqBody)

	header := parser.AppendInt32(nil, corrID)
	header = parser.AppendUVarInt(header, 0)

	body := parser.AppendInt32(nil, 0)
	body = parser.AppendInt16(body, errors.ErrNone)
	body = parser.AppendInt32(body, 0)

	body = parser.AppendUVarInt(body, uint32(len(topicIDs)+1))

	for _, topicID := range topicIDs {
		var topicName string
		exists := false
		for name, meta := range state.Topics {
			if meta.ID == topicID {
				exists = true
				topicName = name
				break
			}
		}

		body = append(body, topicID[:]...)
		body = parser.AppendUVarInt(body, 2)

		body = parser.AppendInt32(body, 0)
		if !exists {
			body = parser.AppendInt16(body, errors.ErrUnknownTopicID)
			body = parser.AppendInt64(body, 0)
			body = parser.AppendInt64(body, 0)
			body = parser.AppendInt64(body, 0)
			body = parser.AppendUVarInt(body, 1)
			body = parser.AppendInt32(body, 0)
			body = parser.AppendUVarInt(body, 1)
			body = parser.AppendUVarInt(body, 0)
		} else {
			records := partition.ReadRecords(topicName, 0)

			body = parser.AppendInt16(body, errors.ErrNone)
			body = parser.AppendInt64(body, 1)
			body = parser.AppendInt64(body, 0)
			body = parser.AppendInt64(body, 0)
			body = parser.AppendUVarInt(body, 1)
			body = parser.AppendInt32(body, 0)

			if len(records) > 0 {
				body = parser.AppendUVarInt(body, uint32(len(records)+1))
				body = append(body, records...)
			} else {
				body = parser.AppendUVarInt(body, 1)
			}
			body = parser.AppendUVarInt(body, 0)
		}

		body = parser.AppendUVarInt(body, 0)
	}

	body = parser.AppendUVarInt(body, 0)

	return frameResponse(header, body)
}

func parseFetchRequestV16(reqBody []byte) [][16]byte {
	br := parser.BytesReader{B: reqBody}

	_ = parser.ReadCompactString(&br)

	_ = parser.ReadInt32(&br)
	_ = parser.ReadInt32(&br)
	_ = parser.ReadInt32(&br)
	_ = parser.ReadInt8(&br)
	_ = parser.ReadInt32(&br)
	_ = parser.ReadInt32(&br)

	nTopics := int(parser.ReadUVarInt(&br)) - 1
	if nTopics < 0 {
		return nil
	}

	topicIDs := make([][16]byte, 0, nTopics)
	for i := 0; i < nTopics; i++ {
		if !br.CanRead(16) {
			break
		}
		var topicID [16]byte
		copy(topicID[:], br.B[br.Off:br.Off+16])
		br.Off += 16
		topicIDs = append(topicIDs, topicID)

		nPartitions := int(parser.ReadUVarInt(&br)) - 1
		for j := 0; j < nPartitions; j++ {
			_ = parser.ReadInt32(&br)
			_ = parser.ReadInt32(&br)
			_ = parser.ReadInt64(&br)
			_ = parser.ReadInt64(&br)
			_ = parser.ReadInt64(&br)
			_ = parser.ReadInt32(&br)
			_ = parser.ReadUVarInt(&br)
		}
		_ = parser.ReadUVarInt(&br)
	}

	return topicIDs
}

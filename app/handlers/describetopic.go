package handlers

import (
	"sort"

	"github.com/codecrafters-io/kafka-starter-go/app/errors"
	"github.com/codecrafters-io/kafka-starter-go/app/parser"
	"github.com/codecrafters-io/kafka-starter-go/app/topic"
)

func HandleDescribeTopicPartitionsV0(corrID int32, reqBody []byte, state *topic.BrokerState) []byte {
	reqNames := parseTopicRequests(reqBody)
	sort.Strings(reqNames)

	header := parser.AppendInt32(nil, corrID)
	header = parser.AppendUVarInt(header, 0)

	body := parser.AppendInt32(nil, 0)
	body = parser.AppendUVarInt(body, uint32(len(reqNames)+1))

	for _, name := range reqNames {
		meta, exists := state.Topics[name]

		if !exists {
			body = parser.AppendInt16(body, errors.ErrUnknownTopicOrPartition)
			body = parser.AppendCompactString(body, name)
			uuid := parser.NilUUID()
			body = append(body, uuid[:]...)
			body = append(body, 0x00)
			body = parser.AppendUVarInt(body, 1)
			body = parser.AppendInt32(body, -2147483648)
			body = parser.AppendUVarInt(body, 0)
		} else {
			body = parser.AppendInt16(body, errors.ErrNone)
			body = parser.AppendCompactString(body, name)
			body = append(body, meta.ID[:]...)
			body = append(body, 0x00)

			numPartitions := meta.Partitions
			if numPartitions == 0 {
				numPartitions = 1
			}
			body = parser.AppendUVarInt(body, uint32(numPartitions+1))

			for partIdx := 0; partIdx < numPartitions; partIdx++ {
				body = parser.AppendInt16(body, errors.ErrNone)
				body = parser.AppendInt32(body, int32(partIdx))
				body = parser.AppendInt32(body, 1)
				body = parser.AppendInt32(body, -1)
				body = parser.AppendUVarInt(body, 2)
				body = parser.AppendInt32(body, 1)
				body = parser.AppendUVarInt(body, 2)
				body = parser.AppendInt32(body, 1)
				body = parser.AppendUVarInt(body, 1)
				body = parser.AppendUVarInt(body, 1)
				body = parser.AppendUVarInt(body, 1)
				body = parser.AppendUVarInt(body, 0)
			}

			body = parser.AppendInt32(body, -2147483648)
			body = parser.AppendUVarInt(body, 0)
		}
	}

	body = append(body, 0xFF)
	body = parser.AppendUVarInt(body, 0)

	return frameResponse(header, body)
}

func parseTopicRequests(reqBody []byte) []string {
	br := parser.BytesReader{B: reqBody}

	_ = parser.ReadUVarInt(&br)

	nTopics := int(parser.ReadUVarInt(&br)) - 1
	if nTopics < 0 {
		return nil
	}

	names := make([]string, 0, nTopics)
	for i := 0; i < nTopics; i++ {
		name := parser.ReadCompactString(&br)
		_ = parser.ReadUVarInt(&br)
		names = append(names, name)
	}
	return names
}

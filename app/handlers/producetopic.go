package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/errors"
	"github.com/codecrafters-io/kafka-starter-go/app/parser"
	"github.com/codecrafters-io/kafka-starter-go/app/partition"
	"github.com/codecrafters-io/kafka-starter-go/app/topic"
)

type ProduceTopicRequest struct {
	Name       string
	Partitions []ProducePartitionRequest
}

type ProducePartitionRequest struct {
	Index   int32
	Records []byte
}

func HandleProduceV11(corrID int32, reqBody []byte, state *topic.BrokerState) []byte {
	topicRequests := parseProduceRequestV11(reqBody)

	header := parser.AppendInt32(nil, corrID)
	header = parser.AppendUVarInt(header, 0)

	body := parser.AppendUVarInt(nil, uint32(len(topicRequests)+1))

	for _, topicReq := range topicRequests {
		body = parser.AppendCompactString(body, topicReq.Name)

		topicMeta, topicExists := state.Topics[topicReq.Name]

		body = parser.AppendUVarInt(body, uint32(len(topicReq.Partitions)+1))

		for _, partReq := range topicReq.Partitions {
			errorCode := errors.ErrUnknownTopicOrPartition
			baseOffset := int64(-1)
			logAppendTime := int64(-1)
			logStartOffset := int64(-1)

			if topicExists {
				numPartitions := topicMeta.Partitions
				if numPartitions == 0 {
					numPartitions = 1
				}

				if partReq.Index >= 0 && partReq.Index < int32(numPartitions) {
					if err := partition.WriteRecords(topicReq.Name, partReq.Index, partReq.Records); err == nil {
						errorCode = errors.ErrNone
						baseOffset = 0
						logAppendTime = -1
						logStartOffset = 0
					}
				}
			}

			body = parser.AppendInt32(body, partReq.Index)
			body = parser.AppendInt16(body, errorCode)
			body = parser.AppendInt64(body, baseOffset)
			body = parser.AppendInt64(body, logAppendTime)
			body = parser.AppendInt64(body, logStartOffset)
			body = parser.AppendUVarInt(body, 1)
			body = parser.AppendCompactString(body, "")
			body = parser.AppendUVarInt(body, 0)
		}

		body = parser.AppendUVarInt(body, 0)
	}

	body = parser.AppendInt32(body, 0)
	body = parser.AppendUVarInt(body, 0)

	return frameResponse(header, body)
}

func parseProduceRequestV11(reqBody []byte) []ProduceTopicRequest {
	br := parser.BytesReader{B: reqBody}

	_, _ = parser.ReadCompactNullableString(&br)
	_ = parser.ReadUVarInt(&br)
	_ = parser.ReadInt16(&br)
	_ = parser.ReadInt32(&br)

	nTopics := int(parser.ReadUVarInt(&br)) - 1
	if nTopics < 0 {
		return nil
	}

	topicRequests := make([]ProduceTopicRequest, 0, nTopics)
	for i := 0; i < nTopics; i++ {
		topicReq := ProduceTopicRequest{}
		topicReq.Name = parser.ReadCompactString(&br)

		nPartitions := int(parser.ReadUVarInt(&br)) - 1
		topicReq.Partitions = make([]ProducePartitionRequest, 0, nPartitions)

		for j := 0; j < nPartitions; j++ {
			partReq := ProducePartitionRequest{}
			partReq.Index = parser.ReadInt32(&br)

			recordsLen := int(parser.ReadUVarInt(&br)) - 1
			if recordsLen > 0 && br.CanRead(recordsLen) {
				partReq.Records = make([]byte, recordsLen)
				copy(partReq.Records, br.B[br.Off:br.Off+recordsLen])
				br.Off += recordsLen
			}

			_ = parser.ReadUVarInt(&br)

			topicReq.Partitions = append(topicReq.Partitions, partReq)
		}

		_ = parser.ReadUVarInt(&br)
		topicRequests = append(topicRequests, topicReq)
	}

	return topicRequests
}

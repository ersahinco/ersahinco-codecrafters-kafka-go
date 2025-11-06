package handlers

import (
	"github.com/codecrafters-io/kafka-starter-go/app/errors"
	"github.com/codecrafters-io/kafka-starter-go/app/parser"
)

const (
	APIKeyProduce            = int16(0)
	APIKeyFetch              = int16(1)
	APIKeyApiVersions        = int16(18)
	APIKeyDescribeTopicParts = int16(75)
)

func BuildApiVersionsErrorOnly(corrID int32, errorCode int16) []byte {
	return BuildSimpleError(corrID, errorCode)
}

func BuildSimpleError(corrID int32, errorCode int16) []byte {
	header := parser.AppendInt32(nil, corrID)
	body := parser.AppendInt16(nil, errorCode)
	return frameResponse(header, body)
}

func BuildApiVersionsV4Body(corrID int32) []byte {
	header := parser.AppendInt32(nil, corrID)

	body := parser.AppendInt16(nil, errors.ErrNone)
	body = parser.AppendUVarInt(body, 5)

	body = parser.AppendInt16(body, APIKeyProduce)
	body = parser.AppendInt16(body, 0)
	body = parser.AppendInt16(body, 11)
	body = parser.AppendUVarInt(body, 0)

	body = parser.AppendInt16(body, APIKeyFetch)
	body = parser.AppendInt16(body, 0)
	body = parser.AppendInt16(body, 16)
	body = parser.AppendUVarInt(body, 0)

	body = parser.AppendInt16(body, APIKeyApiVersions)
	body = parser.AppendInt16(body, 0)
	body = parser.AppendInt16(body, 4)
	body = parser.AppendUVarInt(body, 0)

	body = parser.AppendInt16(body, APIKeyDescribeTopicParts)
	body = parser.AppendInt16(body, 0)
	body = parser.AppendInt16(body, 0)
	body = parser.AppendUVarInt(body, 0)

	body = parser.AppendInt32(body, 0)
	body = parser.AppendUVarInt(body, 0)

	return frameResponse(header, body)
}

func frameResponse(header, body []byte) []byte {
	total := len(header) + len(body)
	out := make([]byte, 0, 4+total)
	out = parser.AppendInt32(out, int32(total))
	out = append(out, header...)
	out = append(out, body...)
	return out
}

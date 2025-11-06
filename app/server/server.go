package server

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"

	"github.com/codecrafters-io/kafka-starter-go/app/errors"
	"github.com/codecrafters-io/kafka-starter-go/app/handlers"
	"github.com/codecrafters-io/kafka-starter-go/app/parser"
	"github.com/codecrafters-io/kafka-starter-go/app/topic"
)

const maxFrameSize = 16 << 20

func HandleConnection(conn net.Conn, state *topic.BrokerState) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		payload, corrID, apiKey, apiVersion, err := readRequest(r)
		if err != nil {
			return
		}

		var resp []byte
		switch apiKey {
		case handlers.APIKeyProduce:
			if apiVersion != 11 {
				resp = handlers.BuildSimpleError(corrID, errors.ErrUnsupportedVersion)
			} else {
				resp = handlers.HandleProduceV11(corrID, payload, state)
			}
		case handlers.APIKeyFetch:
			if apiVersion != 16 {
				resp = handlers.BuildSimpleError(corrID, errors.ErrUnsupportedVersion)
			} else {
				resp = handlers.HandleFetchV16(corrID, payload, state)
			}
		case handlers.APIKeyApiVersions:
			if apiVersion < 0 || apiVersion > 4 {
				resp = handlers.BuildApiVersionsErrorOnly(corrID, errors.ErrUnsupportedVersion)
			} else {
				resp = handlers.BuildApiVersionsV4Body(corrID)
			}
		case handlers.APIKeyDescribeTopicParts:
			if apiVersion != 0 {
				resp = handlers.BuildSimpleError(corrID, errors.ErrUnsupportedVersion)
			} else {
				resp = handlers.HandleDescribeTopicPartitionsV0(corrID, payload, state)
			}
		default:
			resp = frameResponse(parser.AppendInt32(nil, corrID), nil)
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

	hbr := parser.BytesReader{B: payload, Off: 8}
	_, _ = parser.ReadCompactNullableString(&hbr)

	tagBufLen := parser.ReadUVarInt(&hbr)
	if tagBufLen > 0 && hbr.CanRead(int(tagBufLen)) {
		hbr.Off += int(tagBufLen)
	}

	if hbr.Off > len(payload) {
		err = fmt.Errorf("invalid header")
		return
	}

	body = payload[hbr.Off:]
	return
}

func frameResponse(header, body []byte) []byte {
	total := len(header) + len(body)
	out := make([]byte, 0, 4+total)
	out = parser.AppendInt32(out, int32(total))
	out = append(out, header...)
	out = append(out, body...)
	return out
}

func writeAll(w io.Writer, data []byte) error {
	_, err := w.Write(data)
	return err
}

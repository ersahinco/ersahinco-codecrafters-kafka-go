package errors

import "fmt"

const (
	ErrNone                    = int16(0)
	ErrUnknownTopicOrPartition = int16(3)
	ErrUnsupportedVersion      = int16(35)
	ErrUnknownTopicID          = int16(100)
)

type KafkaError struct {
	Code    int16
	Message string
}

func (e *KafkaError) Error() string {
	return fmt.Sprintf("kafka error %d: %s", e.Code, e.Message)
}

func NewKafkaError(code int16, message string) *KafkaError {
	return &KafkaError{Code: code, Message: message}
}

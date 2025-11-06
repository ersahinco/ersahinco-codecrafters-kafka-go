package partition

import (
	"fmt"
	"os"
)

func ReadRecords(topicName string, partition int32) []byte {
	logPath := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d/00000000000000000000.log", topicName, partition)

	data, err := os.ReadFile(logPath)
	if err != nil {
		return nil
	}

	return data
}

func WriteRecords(topicName string, partition int32, records []byte) error {
	logDir := fmt.Sprintf("/tmp/kraft-combined-logs/%s-%d", topicName, partition)

	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logPath := fmt.Sprintf("%s/00000000000000000000.log", logDir)

	return os.WriteFile(logPath, records, 0644)
}

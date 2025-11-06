package main

import (
	"net"
	"os"

	"github.com/codecrafters-io/kafka-starter-go/app/logger"
	"github.com/codecrafters-io/kafka-starter-go/app/server"
	"github.com/codecrafters-io/kafka-starter-go/app/topic"
)

func main() {
	logger.Info("Kafka broker starting on :9092")

	state := topic.BrokerState{Topics: map[string]topic.Meta{}}
	if len(os.Args) > 1 {
		if err := topic.LoadFromProperties(os.Args[1], &state); err != nil {
			logger.Warn("failed to load properties: %v", err)
		}
	}

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		logger.Error("Failed to bind to port 9092")
		os.Exit(1)
	}

	logger.Success("Broker ready, accepting connections")

	for {
		conn, err := l.Accept()
		if err != nil {
			logger.Error("Error accepting connection: %v", err)
			continue
		}
		go server.HandleConnection(conn, &state)
	}
}

package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	sKafka "github.com/segmentio/kafka-go"
)

func customCompletionHandler(messages []sKafka.Message, err error) {
	if err != nil {
		log.Printf("Batch failed: %d messages, error: %v\n", len(messages), err)
	} else {
		log.Printf("Batch succeeded: %d messages\n", len(messages))
	}
}

func main() {
	cfg := config.Connector{
		CDC: cdcConfig.Config{
			MongoDB: cdcConfig.MongoDB{
				Connection: cdcConfig.Connection{
					URI:        "localhost:27017",
					Database:   "exampleDB",
					Collection: "exampleCollection",
				},
			},
			Checkpoint: cdcConfig.CheckpointConfig{
				TokenSaveInterval:     10 * time.Second,
				BootstrapSaveCount:    5000,
				BootstrapSaveInterval: 10 * time.Second,
			},
			Partition: cdcConfig.PartitionConfig{
				TotalPartition: 5,
			},
			Logger: cdcConfig.LoggerConfig{
				LogLevel: "info",
			},
		},
		Kafka: config.Kafka{
			Brokers:                     []string{"localhost:9092"},
			Topic:                       "example-topic",
			ProducerBatchBytes:          "900kb",
			ProducerBatchTickerDuration: 10 * time.Second,
			RejectionLog: config.RejectionLog{
				Topic:        "rejection-log-topic",
				IncludeValue: true,
			},
		},
	}

	connector, err := mongokafka.NewConnectorBuilder(cfg).
		SetSinkResponseHandler(kafka.NewRejectionLogSinkResponseHandler()).
		SetCompletionHandler(customCompletionHandler).
		Build()

	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

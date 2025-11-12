package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	jsoniter "github.com/json-iterator/go"
	sKafka "github.com/segmentio/kafka-go"
)

func customMapper(event mongo.Event) []message.KafkaMessage {
	if event.IsDelete {
		return nil
	}

	jsonBytes, err := jsoniter.Marshal(event.FullDocument)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("failed to marshal document to JSON: %v", err)
		}
		return nil
	}

	return []message.KafkaMessage{
		{
			Key:   helpers.ConvertIDToBytes(event.DocumentID),
			Value: jsonBytes,
		},
	}
}

func customCompletionHandler(messages []sKafka.Message, err error) {
	if err != nil {
		log.Printf("Batch failed: %d messages, error: %v\n", len(messages), err)
	} else {
		log.Printf("Batch succeeded: %d messages\n", len(messages))
	}
}

func main() {
	cfg := config.Config{
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
				ChangeStreamBatchSize: 500,
				BootstrapSaveCount:    5000,
				BootstrapSaveInterval: 10 * time.Second,
				BootstrapBatchSize:    5000,
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
			ProducerBatchSize:           5000,
			ProducerBatchBytes:          "900kb",
			ProducerBatchTickerDuration: 10 * time.Second,
			RejectionLog: config.RejectionLog{
				Topic:        "rejection-log-topic",
				IncludeValue: true,
			},
		},
	}

	connector, err := mongokafka.NewConnectorBuilder(cfg).
		SetMapper(customMapper).
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

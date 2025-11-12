package main

import (
	"context"
	"fmt"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	jsoniter "github.com/json-iterator/go"
)

func bigMapper(event mongo.Event) []message.KafkaMessage {
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

	messages := make([]message.KafkaMessage, 0)

	for i := 0; i < 250; i++ {
		messages = append(messages, message.KafkaMessage{
			Key:   []byte(fmt.Sprintf("%v-%d", event.DocumentID, i)),
			Value: jsonBytes,
		})
	}

	log.Printf("Mapper generated %d messages from single MongoDB event", len(messages))
	return messages
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
		SetMapper(bigMapper).
		SetSinkResponseHandler(kafka.NewRejectionLogSinkResponseHandler()).
		Build()

	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

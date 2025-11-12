package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

func customMapper(event mongo.Event) []message.KafkaMessage {
	jsonBytes, err := jsoniter.Marshal(event.FullDocument)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("failed to marshal document to JSON: %v", err)
		}
		return nil
	}

	type Metadata struct {
		Operation   string `json:"operation"`
		Database    string `json:"database"`
		Collection  string `json:"collection"`
		Timestamp   int64  `json:"timestamp"`
		PartitionID int    `json:"partitionId"`
	}

	metadata := Metadata{
		Operation:   string(event.OperationType),
		Database:    event.Database,
		Collection:  event.Collection,
		Timestamp:   event.EventTime.Unix(),
		PartitionID: event.PartitionID,
	}

	metadataBytes, _ := jsoniter.Marshal(metadata)

	return []message.KafkaMessage{
		{
			Key:   helpers.DocumentIDToBytes(event.DocumentID),
			Value: jsonBytes,
			Headers: []kafka.Header{
				{
					Key:   "metadata",
					Value: metadataBytes,
				},
				{
					Key:   "operation",
					Value: []byte(event.OperationType),
				},
			},
		},
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
		},
	}

	connector, err := mongokafka.NewConnectorBuilder(cfg).SetMapper(customMapper).Build()
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

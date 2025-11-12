package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/producer"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
)

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
		},
	}

	connector, err := mongokafka.NewConnectorBuilder(cfg).
		SetCompletionHandler(producer.DefaultCompletion).
		Build()

	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}


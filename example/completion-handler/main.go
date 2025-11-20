package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"github.com/segmentio/kafka-go"
)

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
				ConsumerGroup:  "consumerGroup",
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

	completionHandler := func(messages []kafka.Message, err error) {
		if err != nil {
			log.Printf("❌ Batch write failed: %v, messages: %d", err, len(messages))
		} else {
			log.Printf("✅ Batch write successful: %d messages", len(messages))
			for i, msg := range messages {
				if i < 3 {
					log.Printf("   - Message %d: key=%s, topic=%s", i+1, string(msg.Key), msg.Topic)
				}
			}
			if len(messages) > 3 {
				log.Printf("   ... and %d more messages", len(messages)-3)
			}
		}
	}

	connector, err := mongokafka.NewConnectorBuilder(cfg).
		SetCompletionHandler(completionHandler).
		Build()
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

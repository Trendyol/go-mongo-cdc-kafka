package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func main() {
	zapConfig := zap.NewProductionConfig()
	zapConfig.EncoderConfig.TimeKey = "timestamp"
	zapConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	zapConfig.EncoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)

	zapLogger, err := zapConfig.Build()
	if err != nil {
		log.Fatal("failed to create zap logger:", err)
	}
	defer zapLogger.Sync()

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

	connector, err := mongokafka.NewConnectorBuilder(cfg).
		SetLogger(zapLogger).
		Build()

	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

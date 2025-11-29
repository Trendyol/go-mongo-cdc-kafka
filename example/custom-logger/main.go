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
		log.Printf("failed to create zap logger: %v", err)
		return
	}
	defer func() {
		_ = zapLogger.Sync()
	}()

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
				BootstrapQueryBatchSize: 5000,
			},
			Partition: cdcConfig.PartitionConfig{
				TotalPartition: 15,
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
		log.Printf("failed to create connector: %v", err)
		return
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

package main

import (
	"context"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
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
				ConnectionPool: cdcConfig.ConnectionPool{
					MaxPoolSize:   100,
					MinPoolSize:   5,
					MaxIdleTimeMS: 300000,
				},
				Timeouts: cdcConfig.Timeouts{
					ConnectTimeoutMS:         30000,
					ServerSelectionTimeoutMS: 60000,
					SocketTimeoutMS:          120000,
				},
			},
			Metric: cdcConfig.MetricConfig{
				Port: 8080,
			},
			Checkpoint: cdcConfig.CheckpointConfig{
				TokenSaveInterval:     10 * time.Second,
				ChangeStreamBatchSize: 500,
				BootstrapSaveCount:    5000,
				BootstrapSaveInterval: 10 * time.Second,
				BootstrapBatchSize:    5000,
			},
			Partition: cdcConfig.PartitionConfig{
				HeartbeatInterval:      10 * time.Second,
				WorkerTimeout:          90 * time.Second,
				RebalanceCheckInterval: 10 * time.Second,
				TotalPartition:         5,
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
			Compression:                 0,
			RequiredAcks:                1,
			SecureConnection:            false,
		},
	}

	connector, err := mongokafka.NewConnector(cfg, nil)
	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

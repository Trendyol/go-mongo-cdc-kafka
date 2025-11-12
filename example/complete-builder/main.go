package main

import (
	"context"
	"github.com/Trendyol/go-mongo-cdc/logger"
	jsoniter "github.com/json-iterator/go"
	"log"
	"time"

	mongokafka "github.com/Trendyol/go-mongo-cdc-kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	sKafka "github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func customMapper(event mongo.Event) []message.KafkaMessage {
	jsonBytes, err := jsoniter.Marshal(event.FullDocument)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("failed to marshal document to JSON: %v", err)
		}
		return nil
	}

	key := helpers.DocumentIDToBytes(event.DocumentID)

	return []message.KafkaMessage{
		{
			Key:   key,
			Value: jsonBytes,
			Headers: []sKafka.Header{
				{Key: "operation", Value: []byte(event.OperationType)},
				{Key: "collection", Value: []byte(event.Collection)},
				{Key: "database", Value: []byte(event.Database)},
			},
		},
	}
}

type customSinkResponseHandler struct{}

func (c *customSinkResponseHandler) OnInit(ctx *kafka.SinkResponseHandlerInitContext) {
	log.Printf("Custom sink response handler initialized - Topic: %s", ctx.Config.Topic)
}

func (c *customSinkResponseHandler) OnSuccess(ctx *kafka.SinkResponseHandlerContext) {
	log.Printf("✅ Message sent successfully - Key: %s", string(ctx.Message.Key))
}

func (c *customSinkResponseHandler) OnError(ctx *kafka.SinkResponseHandlerContext) {
	log.Printf("❌ Message failed - Key: %s, Error: %v", string(ctx.Message.Key), ctx.Err)
}

func customCompletionHandler(messages []sKafka.Message, err error) {
	if err != nil {
		log.Printf("❌ Batch failed: %d messages, error: %v", len(messages), err)
		for i, msg := range messages {
			log.Printf("  Failed message %d: Topic=%s, Key=%s", i+1, msg.Topic, string(msg.Key))
		}
	} else {
		log.Printf("✅ Batch succeeded: %d messages processed successfully", len(messages))
	}
}

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
		SetLogger(zapLogger).
		SetMapper(customMapper).
		SetSinkResponseHandler(&customSinkResponseHandler{}).
		SetCompletionHandler(customCompletionHandler).
		Build()

	if err != nil {
		log.Fatal("failed to create connector:", err)
	}

	defer connector.Close()

	ctx := context.Background()
	connector.Start(ctx)
}

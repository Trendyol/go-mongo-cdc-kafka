package mongokafka

import (
	"context"
	"fmt"

	cdc "github.com/Trendyol/go-mongo-cdc"
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/producer"
	"github.com/Trendyol/go-mongo-cdc-kafka/metric"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/stream"
	sKafka "github.com/segmentio/kafka-go"
)

type Connector interface {
	Start(ctx context.Context)
	Close()
}

type connector struct {
	mongoCDC            cdc.Connector
	mapper              Mapper
	producer            producer.Producer
	config              *config.Config
	metric              metric.Metric
	sinkResponseHandler kafka.SinkResponseHandler
	completionHandler   func(messages []sKafka.Message, err error)
}

func NewConnector(cfg config.Config, mapper Mapper) (Connector, error) {
	return NewConnectorBuilder(cfg).
		SetMapper(mapper).
		Build()
}

func (c *connector) Start(ctx context.Context) {
	logger.Log.Info("Starting CDC to Kafka connector")

	c.producer.StartBatch()

	c.mongoCDC.Start(ctx)
}

func (c *connector) Close() {
	logger.Log.Info("Closing CDC to Kafka connector")

	c.mongoCDC.Close()

	if err := c.producer.Close(); err != nil {
		logger.Log.Error("failed to close producer: %v", err)
	}

	logger.Log.Info("CDC to Kafka connector closed")
}

type ConnectorBuilder struct {
	config              config.Config
	mapper              Mapper
	sinkResponseHandler kafka.SinkResponseHandler
	completionHandler   func(messages []sKafka.Message, err error)
}

func NewConnectorBuilder(cfg config.Config) *ConnectorBuilder {
	return &ConnectorBuilder{
		config: cfg,
		mapper: DefaultMapper,
	}
}

func (cb *ConnectorBuilder) SetMapper(mapper Mapper) *ConnectorBuilder {
	if mapper != nil {
		cb.mapper = mapper
	}
	return cb
}

func (cb *ConnectorBuilder) SetSinkResponseHandler(handler kafka.SinkResponseHandler) *ConnectorBuilder {
	cb.sinkResponseHandler = handler
	return cb
}

func (cb *ConnectorBuilder) SetCompletionHandler(handler func(messages []sKafka.Message, err error)) *ConnectorBuilder {
	cb.completionHandler = handler
	return cb
}

func (cb *ConnectorBuilder) Build() (Connector, error) {
	cb.config.ApplyDefaults()

	m := metric.NewMetric()

	kafkaClient, err := kafka.NewClient(&cb.config)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	prod, err := producer.NewProducer(kafkaClient, &cb.config, m, cb.sinkResponseHandler, cb.completionHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	c := &connector{
		producer:            prod,
		mapper:              cb.mapper,
		config:              &cb.config,
		metric:              m,
		sinkResponseHandler: cb.sinkResponseHandler,
		completionHandler:   cb.completionHandler,
	}

	mongoCDC, err := cdc.NewConnector(cb.config.CDC, c.listener)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongo cdc connector: %w", err)
	}

	c.mongoCDC = mongoCDC

	return c, nil
}

func (c *connector) listener(ctx *stream.ListenerContext) error {
	event := mongo.NewEvent(ctx.Message, ctx.PartitionID)

	kafkaMessages := c.mapper(event)

	if len(kafkaMessages) == 0 {
		ctx.Ack()
		return nil
	}

	messages := make([]sKafka.Message, 0, len(kafkaMessages))
	for _, message := range kafkaMessages {
		topic := message.Topic
		if topic == "" {
			topic = c.config.Kafka.Topic
		}

		messages = append(messages, sKafka.Message{
			Topic:   topic,
			Key:     message.Key,
			Value:   message.Value,
			Headers: message.Headers,
		})
	}

	batchSizeLimit := c.config.Kafka.ProducerBatchSize
	if len(messages) > batchSizeLimit {
		chunks := helpers.ChunkSliceWithSize[sKafka.Message](messages, batchSizeLimit)
		lastChunkIndex := len(chunks) - 1
		for idx, chunk := range chunks {
			c.producer.Produce(ctx, event.EventTime, chunk, idx == lastChunkIndex)
		}
	} else {
		c.producer.Produce(ctx, event.EventTime, messages, true)
	}

	return nil
}

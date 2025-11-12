package mongokafka

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"

	jsoniter "github.com/json-iterator/go"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"

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
	mongoCDC cdc.Connector
	mapper   Mapper
	producer producer.Producer
	config   *config.Connector
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
	config              any
	mapper              Mapper
	sinkResponseHandler kafka.SinkResponseHandler
	completionHandler   func(messages []sKafka.Message, err error)
}

func NewConnectorBuilder(cfg any) *ConnectorBuilder {
	return &ConnectorBuilder{
		config:              cfg,
		mapper:              DefaultMapper,
		sinkResponseHandler: nil,
		completionHandler:   nil,
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

func (cb *ConnectorBuilder) SetLogger(zapLogger *zap.Logger) *ConnectorBuilder {
	logger.Log = &logger.Loggers{
		Zap: zapLogger,
	}
	return cb
}

func (cb *ConnectorBuilder) Build() (Connector, error) {
	c, err := newConfig(cb.config)
	if err != nil {
		return nil, err
	}
	c.ApplyDefaults()

	connector := &connector{
		mapper: cb.mapper,
		config: c,
	}

	c.CDC.Checkpoint.Type = "manual"

	mongoCDC, err := cdc.NewConnector(c.CDC, connector.listener)
	if err != nil {
		return nil, fmt.Errorf("failed to create mongo cdc connector: %w", err)
	}

	copyOfConfig := c.Kafka
	printConfiguration(copyOfConfig)

	connector.mongoCDC = mongoCDC

	kafkaClient, err := createKafkaClient(c)
	if err != nil {
		return nil, err
	}

	checkpointCommit := func() {
		mongoCDC.Commit()
	}

	checkpointCommitBootstrap := func(partitionID int) {
		mongoCDC.CommitBootstrap(partitionID)
	}

	metricsRecorder := metric.NewMetricsRecorder()

	prod, err := producer.NewProducer(kafkaClient, c, metricsRecorder, checkpointCommit, checkpointCommitBootstrap, cb.sinkResponseHandler, cb.completionHandler)
	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	connector.producer = prod

	return connector, nil
}

func printConfiguration(config config.Kafka) {
	config.ScramPassword = "*****"
	configJSON, _ := jsoniter.Marshal(config)

	dst := &bytes.Buffer{}
	if err := json.Compact(dst, configJSON); err != nil {
		logger.Log.Error("error while print kafka configuration, err: %v", err)
		panic(err)
	}

	logger.Log.Info("using kafka config: %v", dst.String())
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
			c.producer.Produce(ctx, event.EventTime, chunk, idx == lastChunkIndex, ctx.PartitionID, ctx.IsBootstrap)
		}
	} else {
		c.producer.Produce(ctx, event.EventTime, messages, true, ctx.PartitionID, ctx.IsBootstrap)
	}

	return nil
}

func newConfig(cf any) (*config.Connector, error) {
	switch v := cf.(type) {
	case *config.Connector:
		return v, nil
	case config.Connector:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func createKafkaClient(cc *config.Connector) (kafka.Client, error) {
	kafkaClient := kafka.NewClient(cc)

	if !cc.Kafka.AllowAutoTopicCreation {
		if err := kafkaClient.CheckTopic(cc.Kafka.Topic); err != nil {
			logger.Log.Error("topic check error: %v", err)
			return nil, err
		}
	}

	return kafkaClient, nil
}

func newConnectorConfigFromPath(path string) (*config.Connector, error) {
	file, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	envPattern := regexp.MustCompile(`\${([^}]+)}`)
	matches := envPattern.FindAllStringSubmatch(string(file), -1)
	for _, match := range matches {
		envVar := match[1]
		if value, exists := os.LookupEnv(envVar); exists {
			updatedFile := strings.ReplaceAll(string(file), "${"+envVar+"}", value)
			file = []byte(updatedFile)
		}
	}
	var c config.Connector
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, err
	}
	c.ApplyDefaults()
	return &c, nil
}

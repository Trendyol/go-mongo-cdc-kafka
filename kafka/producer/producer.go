package producer

import (
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"time"

	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/metric"
	"github.com/Trendyol/go-mongo-cdc/stream"
	sKafka "github.com/segmentio/kafka-go"
)

type Producer struct {
	ProducerBatch *Batch
}

func NewProducer(
	kafkaClient kafka.Client,
	config *config.Config,
	metric metric.Metric,
	sinkResponseHandler kafka.SinkResponseHandler,
	completionHandler func(messages []sKafka.Message, err error),
) (Producer, error) {
	writer := kafkaClient.Producer(completionHandler)

	if sinkResponseHandler != nil {
		sinkResponseHandler.OnInit(&kafka.SinkResponseHandlerInitContext{
			Config:      config.Kafka,
			KafkaClient: kafkaClient,
			Writer:      writer,
		})
	}

	return Producer{
		ProducerBatch: newBatch(
			config.Kafka.ProducerBatchTickerDuration,
			writer,
			config.Kafka.ProducerBatchSize,
			int64(helpers.ResolveUnionIntOrStringValue(config.Kafka.ProducerBatchBytes)),
			metric,
			sinkResponseHandler,
		),
	}, nil
}

func (p *Producer) StartBatch() {
	p.ProducerBatch.StartBatchTicker()
}

func (p *Producer) Produce(
	ctx *stream.ListenerContext,
	eventTime time.Time,
	messages []sKafka.Message,
	isLastChunk bool,
) {
	p.ProducerBatch.AddMessages(ctx, messages, eventTime, isLastChunk)
}

func (p *Producer) Close() error {
	p.ProducerBatch.Close()
	return p.ProducerBatch.Writer.Close()
}

func (p *Producer) GetMetric() metric.Metric {
	return p.ProducerBatch.metric
}

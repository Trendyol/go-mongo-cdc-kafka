package producer

import (
	"time"

	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	gKafka "github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc/stream"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	ProducerBatch *Batch
}

func NewProducer(
	kafkaClient gKafka.Client,
	config *config.Connector,
	metricsRecorder gKafka.MetricsRecorder,
	checkpointCommit func(),
	sinkResponseHandler gKafka.SinkResponseHandler,
	completionHandler func(messages []kafka.Message, err error),
) (Producer, error) {
	writer := kafkaClient.Producer(completionHandler)

	if sinkResponseHandler != nil {
		sinkResponseHandler.OnInit(&gKafka.SinkResponseHandlerInitContext{
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
			metricsRecorder,
			checkpointCommit,
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
	messages []kafka.Message,
	isLastChunk bool,
) {
	p.ProducerBatch.AddMessages(ctx, messages, eventTime, isLastChunk)
}

func (p *Producer) Close() error {
	p.ProducerBatch.Close()
	return p.ProducerBatch.Writer.Close()
}

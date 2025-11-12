package kafka

import (
	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/segmentio/kafka-go"
)

type SinkResponseHandler interface {
	OnInit(ctx *SinkResponseHandlerInitContext)
	OnSuccess(ctx *SinkResponseHandlerContext)
	OnError(ctx *SinkResponseHandlerContext)
}

type SinkResponseHandlerInitContext struct {
	Config      config.Kafka
	KafkaClient Client
	Writer      *kafka.Writer
}

type SinkResponseHandlerContext struct {
	Message *message.KafkaMessage
	Err     error
}


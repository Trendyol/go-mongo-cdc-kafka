package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/Trendyol/go-mongo-cdc/logger"
	jsoniter "github.com/json-iterator/go"
	"github.com/segmentio/kafka-go"
)

type RejectionLog struct {
	Topic        string `yaml:"topic" json:"topic"`
	IncludeValue bool   `yaml:"includeValue" json:"includeValue"`
}

type RejectionLogSinkResponseHandler struct {
	kafkaClient  Client
	writer       *kafka.Writer
	topic        string
	includeValue bool
}

func NewRejectionLogSinkResponseHandler() SinkResponseHandler {
	return &RejectionLogSinkResponseHandler{}
}

func (r *RejectionLogSinkResponseHandler) OnInit(ctx *SinkResponseHandlerInitContext) {
	if ctx.Config.RejectionLog.Topic == "" {
		if logger.Log != nil {
			logger.Log.Debug("Rejection log topic not configured, rejection log disabled")
		}
		return
	}

	r.kafkaClient = ctx.KafkaClient
	r.topic = ctx.Config.RejectionLog.Topic
	r.includeValue = ctx.Config.RejectionLog.IncludeValue

	err := r.kafkaClient.CheckTopic(r.topic)
	if err != nil {
		if logger.Log != nil {
			logger.Log.Error("error while rejection topic exist request, err: %v", err)
		}
		panic(err)
	}

	r.writer = &kafka.Writer{
		Addr:         kafka.TCP(ctx.Config.Brokers...),
		Topic:        r.topic,
		Balancer:     ctx.Config.GetBalancer(),
		MaxAttempts:  ctx.Config.ProducerMaxAttempts,
		ReadTimeout:  ctx.Config.ReadTimeout,
		WriteTimeout: ctx.Config.WriteTimeout,
		RequiredAcks: kafka.RequiredAcks(ctx.Config.RequiredAcks),
		Compression:  kafka.Compression(ctx.Config.GetCompression()),
		Transport:    ctx.Writer.Transport,
	}

	if logger.Log != nil {
		logger.Log.Info("Rejection log handler initialized - topic: %s, includeValue: %v", r.topic, r.includeValue)
	}
}

func (r *RejectionLogSinkResponseHandler) OnSuccess(ctx *SinkResponseHandlerContext) {
}

func (r *RejectionLogSinkResponseHandler) OnError(ctx *SinkResponseHandlerContext) {
	if r.writer == nil {
		return
	}

	rejectionLog := r.buildRejectionLog(ctx)
	if err := r.publishToKafka(ctx, rejectionLog); err != nil {
		if logger.Log != nil {
			logger.Log.Error("failed to publish rejection log: %v", err)
		}
	}
}

func (r *RejectionLogSinkResponseHandler) buildRejectionLog(ctx *SinkResponseHandlerContext) *RejectionLogMessage {
	headers := make(map[string]string)
	for _, header := range ctx.Message.Headers {
		headers[header.Key] = string(header.Value)
	}

	rejectionLog := &RejectionLogMessage{
		Topic:        ctx.Message.Topic,
		Key:          string(ctx.Message.Key),
		Headers:      headers,
		Error:        ctx.Err.Error(),
		ErrorTime:    time.Now().Unix(),
		IncludeValue: r.includeValue,
	}

	if r.includeValue {
		rejectionLog.Value = string(ctx.Message.Value)
	}

	return rejectionLog
}

func (r *RejectionLogSinkResponseHandler) publishToKafka(ctx *SinkResponseHandlerContext, rejectionLog *RejectionLogMessage) error {
	logBytes, err := jsoniter.Marshal(rejectionLog)
	if err != nil {
		return fmt.Errorf("failed to marshal rejection log: %w", err)
	}

	kafkaMessage := kafka.Message{
		Key:     ctx.Message.Key,
		Value:   logBytes,
		Headers: ctx.Message.Headers,
	}

	if err := r.writer.WriteMessages(context.Background(), kafkaMessage); err != nil {
		return fmt.Errorf("failed to write rejection log to Kafka: %w", err)
	}

	if logger.Log != nil {
		logger.Log.Debug("Rejection log published - topic: %s, key: %s, error: %s", r.topic, string(ctx.Message.Key), ctx.Err.Error())
	}
	return nil
}

type RejectionLogMessage struct {
	Topic        string            `json:"topic"`
	Key          string            `json:"key,omitempty"`
	Value        string            `json:"value,omitempty"`
	Headers      map[string]string `json:"headers,omitempty"`
	Error        string            `json:"error"`
	ErrorTime    int64             `json:"errorTime"`
	IncludeValue bool              `json:"includeValue"`
}

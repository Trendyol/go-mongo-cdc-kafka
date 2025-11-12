package producer

import (
	"context"
	"fmt"
	"sync"
	"time"

	gKafka "github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/Trendyol/go-mongo-cdc/stream"
	"github.com/segmentio/kafka-go"
)

type Batch struct {
	sinkResponseHandler gKafka.SinkResponseHandler
	batchTicker         *time.Ticker
	Writer              *kafka.Writer
	metricsRecorder     gKafka.MetricsRecorder
	checkpointCommit    func()
	messages            []kafka.Message
	currentMessageBytes int64
	batchTickerDuration time.Duration
	batchLimit          int
	batchBytes          int64
	flushLock           sync.Mutex
}

func newBatch(
	batchTime time.Duration,
	writer *kafka.Writer,
	batchLimit int,
	batchBytes int64,
	metricsRecorder gKafka.MetricsRecorder,
	checkpointCommit func(),
	sinkResponseHandler gKafka.SinkResponseHandler,
) *Batch {
	batch := &Batch{
		batchTickerDuration: batchTime,
		batchTicker:         time.NewTicker(batchTime),
		metricsRecorder:     metricsRecorder,
		checkpointCommit:    checkpointCommit,
		messages:            make([]kafka.Message, 0, batchLimit),
		Writer:              writer,
		batchLimit:          batchLimit,
		batchBytes:          batchBytes,
		sinkResponseHandler: sinkResponseHandler,
	}
	return batch
}

func (b *Batch) StartBatchTicker() {
	go func() {
		for {
			<-b.batchTicker.C
			b.FlushMessages()
		}
	}()
}

func (b *Batch) Close() {
	b.batchTicker.Stop()
	b.FlushMessages()
}

func (b *Batch) AddMessages(ctx *stream.ListenerContext, messages []kafka.Message, eventTime time.Time, isLastChunk bool) {
	b.flushLock.Lock()
	b.messages = append(b.messages, messages...)
	b.currentMessageBytes += totalSizeOfMessages(messages)
	if isLastChunk {
		ctx.Ack()
	}
	b.flushLock.Unlock()

	if isLastChunk {
		b.metricsRecorder.RecordKafkaConnectorLatency(time.Since(eventTime).Milliseconds())
	}

	if len(b.messages) >= b.batchLimit || b.currentMessageBytes >= b.batchBytes {
		b.FlushMessages()
	}
}

func (b *Batch) FlushMessages() {
	b.flushLock.Lock()
	defer b.flushLock.Unlock()

	if len(b.messages) > 0 {
		startedTime := time.Now()
		err := b.Writer.WriteMessages(context.Background(), b.messages...)

		if err != nil && b.sinkResponseHandler == nil {
			err = fmt.Errorf("batch producer flush error %v", err)
			logger.Log.Error("error while flush, err: %v", err)
			panic(err)
		}

		b.metricsRecorder.RecordBatchProduceLatency(time.Since(startedTime).Milliseconds())

		if b.sinkResponseHandler != nil {
			switch e := err.(type) {
			case nil:
				b.handleResponseSuccess()
			case kafka.WriteErrors:
				b.handleWriteError(e)
			case kafka.MessageTooLargeError:
				b.handleMessageTooLargeError(e)
			default:
				b.handleResponseError(e)
				logger.Log.Error("batch producer flush default error %v", err)
			}
		}

		b.messages = b.messages[:0]
		b.currentMessageBytes = 0
		b.batchTicker.Reset(b.batchTickerDuration)
	}
	b.checkpointCommit()
}

func (b *Batch) handleWriteError(writeErrors kafka.WriteErrors) {
	for i := range writeErrors {
		if writeErrors[i] != nil {
			b.sinkResponseHandler.OnError(&gKafka.SinkResponseHandlerContext{
				Message: convertKafkaMessage(b.messages[i]),
				Err:     writeErrors[i],
			})
		} else {
			b.sinkResponseHandler.OnSuccess(&gKafka.SinkResponseHandlerContext{
				Message: convertKafkaMessage(b.messages[i]),
				Err:     nil,
			})
		}
	}
}

func (b *Batch) handleResponseError(err error) {
	for _, msg := range b.messages {
		b.sinkResponseHandler.OnError(&gKafka.SinkResponseHandlerContext{
			Message: convertKafkaMessage(msg),
			Err:     err,
		})
	}
}

func (b *Batch) handleResponseSuccess() {
	for _, msg := range b.messages {
		b.sinkResponseHandler.OnSuccess(&gKafka.SinkResponseHandlerContext{
			Message: convertKafkaMessage(msg),
			Err:     nil,
		})
	}
}

func (b *Batch) handleMessageTooLargeError(mTooLargeError kafka.MessageTooLargeError) {
	b.sinkResponseHandler.OnError(&gKafka.SinkResponseHandlerContext{
		Message: convertKafkaMessage(mTooLargeError.Message),
		Err:     mTooLargeError,
	})
}

func convertKafkaMessage(src kafka.Message) *message.KafkaMessage {
	return &message.KafkaMessage{
		Topic:   src.Topic,
		Headers: src.Headers,
		Key:     src.Key,
		Value:   src.Value,
	}
}

func totalSizeOfMessages(messages []kafka.Message) int64 {
	var size int
	for _, m := range messages {
		headerSize := 0
		for _, header := range m.Headers {
			headerSize += 2 + len(header.Key)
			headerSize += len(header.Value)
		}
		size += 14 + (4 + len(m.Key)) + (4 + len(m.Value)) + headerSize
	}
	return int64(size)
}

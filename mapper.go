package mongokafka

import (
	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	"github.com/Trendyol/go-mongo-cdc/logger"
	jsoniter "github.com/json-iterator/go"
)

type Mapper func(event mongo.Event) []message.KafkaMessage

func DefaultMapper(event mongo.Event) []message.KafkaMessage {
	if event.IsDelete {
		return nil
	}

	jsonBytes, err := jsoniter.Marshal(event.FullDocument)
	if err != nil {
		logger.Log.Error("failed to marshal document to JSON: %v", err)
		return nil
	}

	return []message.KafkaMessage{
		{
			Key:   helpers.DocumentIDToBytes(event.DocumentID),
			Value: jsonBytes,
		},
	}
}

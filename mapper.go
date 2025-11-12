package mongokafka

import (
	"fmt"
	jsoniter "github.com/json-iterator/go"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strconv"

	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/message"
	"github.com/Trendyol/go-mongo-cdc-kafka/mongo"
	"github.com/Trendyol/go-mongo-cdc/logger"
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
			Key:   documentIDToBytes(event.DocumentID),
			Value: jsonBytes,
		},
	}
}

func documentIDToBytes(id interface{}) []byte {
	var docID string
	switch id := id.(type) {
	case primitive.ObjectID:
		docID = id.Hex()
	case int:
		docID = strconv.Itoa(id)
	case int32:
		docID = strconv.FormatInt(int64(id), 10)
	case int64:
		docID = strconv.FormatInt(id, 10)
	case string:
		docID = id
	default:
		docID = fmt.Sprintf("%v", id)
		logger.Log.Warn("Unexpected document ID type: %T, value: %v", id, id)
	}

	return helpers.Byte(docID)
}

package mongo

import (
	"time"

	"github.com/Trendyol/go-mongo-cdc/mongo/message"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Event struct {
	EventTime     time.Time
	DocumentID    interface{}
	FullDocument  bson.M
	OperationType message.OperationType
	Database      string
	Collection    string
	PartitionID   int
	ClusterTime   primitive.Timestamp
	IsInsert      bool
	IsUpdate      bool
	IsDelete      bool
	IsReplace     bool
}

func NewEvent(msg message.Message, partitionID int) Event {
	return Event{
		OperationType: msg.OperationType,
		ClusterTime:   msg.ClusterTime,
		Database:      msg.Database,
		Collection:    msg.Collection,
		DocumentID:    msg.DocumentID,
		FullDocument:  msg.FullDocument,
		EventTime:     msg.EventTime,
		PartitionID:   partitionID,
		IsInsert:      msg.IsInsert(),
		IsUpdate:      msg.IsUpdate(),
		IsDelete:      msg.IsDelete(),
		IsReplace:     msg.IsReplace(),
	}
}

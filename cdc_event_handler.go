package mongokafka

import (
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/producer"
)

type CdcEventHandler struct {
	producerBatch *producer.Batch
}

func (h *CdcEventHandler) BeforePartitionStop(partitionID int) {
	h.producerBatch.PreparePartitionRebalancing(partitionID)
}

func (h *CdcEventHandler) AfterPartitionStop(partitionID int) {
	h.producerBatch.FlushMessages()
	h.producerBatch.EndPartitionRebalancing(partitionID)
}

func (h *CdcEventHandler) BeforePartitionStart(partitionID int) {
}

func (h *CdcEventHandler) AfterPartitionStart(partitionID int) {
}

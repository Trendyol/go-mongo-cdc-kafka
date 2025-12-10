package mongokafka

import (
	"context"

	"github.com/Trendyol/go-mongo-cdc-kafka/kafka/producer"
	"github.com/Trendyol/go-mongo-cdc/stream"
)

type CdcEventHandler struct {
	producerBatch *producer.Batch
}

func NewCdcEventHandler(producerBatch *producer.Batch) stream.EventHandler {
	return &CdcEventHandler{
		producerBatch: producerBatch,
	}
}

func (h *CdcEventHandler) BeforeRebalanceStart() {
}

func (h *CdcEventHandler) AfterRebalanceStart() {
}

func (h *CdcEventHandler) BeforeRebalanceEnd() {
}

func (h *CdcEventHandler) AfterRebalanceEnd() {
}

func (h *CdcEventHandler) BeforeStreamStart() {
}

func (h *CdcEventHandler) AfterStreamStart() {
}

func (h *CdcEventHandler) BeforeStreamStop() {
}

func (h *CdcEventHandler) AfterStreamStop() {
}

func (h *CdcEventHandler) WaitForRebalanceReady(ctx context.Context) error {
	return h.producerBatch.PauseForRebalance(ctx)
}

func (h *CdcEventHandler) NotifyRebalanceComplete() {
	h.producerBatch.ResumeAfterRebalance()
}

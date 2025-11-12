package metric

import (
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

type Metric interface {
	SetKafkaConnectorLatency(latency int64)
	SetBatchProduceLatency(latency int64)
	GetKafkaConnectorLatency() int64
	GetBatchProduceLatency() int64
}

type metric struct {
	kafkaConnectorLatency int64
	batchProduceLatency   int64

	kafkaConnectorLatencyGauge prometheus.Gauge
	batchProduceLatencyGauge   prometheus.Gauge
}

func NewMetric() Metric {
	return &metric{
		kafkaConnectorLatencyGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "go_mongo_kafka",
			Name:      "connector_latency_ms_current",
			Help:      "Time to adding to the batch",
		}),
		batchProduceLatencyGauge: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace: "go_mongo_kafka",
			Name:      "batch_produce_latency_ms_current",
			Help:      "Time to produce messages in the batch",
		}),
	}
}

func (m *metric) SetKafkaConnectorLatency(latency int64) {
	atomic.StoreInt64(&m.kafkaConnectorLatency, latency)
	m.kafkaConnectorLatencyGauge.Set(float64(latency))
}

func (m *metric) SetBatchProduceLatency(latency int64) {
	atomic.StoreInt64(&m.batchProduceLatency, latency)
	m.batchProduceLatencyGauge.Set(float64(latency))
}

func (m *metric) GetKafkaConnectorLatency() int64 {
	return atomic.LoadInt64(&m.kafkaConnectorLatency)
}

func (m *metric) GetBatchProduceLatency() int64 {
	return atomic.LoadInt64(&m.batchProduceLatency)
}


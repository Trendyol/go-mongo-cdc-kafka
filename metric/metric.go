package metric

import (
	"github.com/Trendyol/go-mongo-cdc-kafka/kafka"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

const Name = "go_mongo_cdc_kafka"

var (
	kafkaConnectorLatencyGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(Name, "kafka_connector_latency_ms", "current"),
			Help: "Kafka connector latency ms",
		},
	)

	batchProduceLatencyGauge = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: prometheus.BuildFQName(Name, "kafka_connector_batch_produce_latency_ms", "current"),
			Help: "Kafka connector batch produce latency ms",
		},
	)
)

type PrometheusMetricsRecorder struct{}

func NewMetricsRecorder() kafka.MetricsRecorder {
	return &PrometheusMetricsRecorder{}
}

func (m *PrometheusMetricsRecorder) RecordKafkaConnectorLatency(latencyMs int64) {
	kafkaConnectorLatencyGauge.Set(float64(latencyMs))
}

func (m *PrometheusMetricsRecorder) RecordBatchProduceLatency(latencyMs int64) {
	batchProduceLatencyGauge.Set(float64(latencyMs))
}

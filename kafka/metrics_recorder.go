package kafka

type MetricsRecorder interface {
	RecordKafkaConnectorLatency(latencyMs int64)
	RecordBatchProduceLatency(latencyMs int64)
}

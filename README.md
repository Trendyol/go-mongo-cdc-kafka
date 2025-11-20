## Go Mongo CDC Kafka

**Go Mongo CDC Kafka** is a Go connector library that captures real-time changes from MongoDB using Change Streams and publishes them to Kafka.  
It builds on top of `go-mongo-cdc` for MongoDB CDC and adds a configurable Kafka producer layer.

### Features

- **Managing batch configurations** such as maximum batch size, batch bytes, batch ticker durations.
- **Flexible mapper**: Allows generating one or multiple Kafka messages for each MongoDB event and customizing key, headers and value.
- **Kafka compression** support(Gzip, Snappy, Lz4, Zstd).
- **Kafka producer acknowledges** support(fire-and-forget, wait for the leader, wait for the full ISR).
- **Rejection log support**: Failed messages can be routed to a dedicated rejection-log topic.

### Installation

```bash
go get github.com/Trendyol/go-mongo-cdc-kafka
```

### Quick Start (Struct Config)

The following example shows a minimal setup that reads changes from MongoDB and forwards them to a single Kafka topic:

```go
cfg := config.Connector{
    CDC: cdcConfig.Config{
        MongoDB: cdcConfig.MongoDB{
            Connection: cdcConfig.Connection{
                URI:        "localhost:27017",
                Database:   "exampleDB",
                Collection: "exampleCollection",
            },
        },
        Checkpoint: cdcConfig.CheckpointConfig{
            TokenSaveInterval:     10 * time.Second,
            BootstrapSaveCount:    5000,
            BootstrapSaveInterval: 10 * time.Second,
        },
        Partition: cdcConfig.PartitionConfig{
            TotalPartition: 5,
            ConsumerGroup:  "consumerGroup",
        },
        Logger: cdcConfig.LoggerConfig{
            LogLevel: "info",
        },
    },
    Kafka: config.Kafka{
        Brokers:                     []string{"localhost:9092"},
        Topic:                       "example-topic",
        ProducerBatchBytes:          "900kb",
        ProducerBatchTickerDuration: 10 * time.Second,
    },
}

connector, err := mongokafka.NewConnectorBuilder(cfg).Build()
if err != nil {
    log.Fatal("failed to create connector:", err)
}
defer connector.Close()

ctx := context.Background()
connector.Start(ctx)
```

For more advanced scenarios, check the examples under the `example` directory:

- **`example/struct-config`**: Basic usage with struct-based configuration.
- **`example/complete-builder`**: Full builder usage with custom mapper, custom logger, custom sink response handler and completion handler.
- **`example/simple-rejection-log-sink-response-handler`**: Usage of the built-in rejection log sink response handler.
- **`example/grafana`**: Docker-compose setup to observe CDC & Kafka metrics with Prometheus and Grafana.

### Configuration

`go-mongo-cdc-kafka` uses two main configuration blocks:

- **CDC (`CDC`)**: MongoDB connection, partitioning, checkpointing, metrics and logging (identical to `go-mongo-cdc`).
- **Kafka (`Kafka`)**: Kafka brokers, topic mapping, batching, timeouts, compression and security.

#### CDC / MongoDB Configuration (Summary)

CDC configuration is provided by `go-mongo-cdc`’s `Config` struct. Common fields include:

- **`mongodb.connection.uri`**: MongoDB connection URI.
- **`mongodb.connection.database`**: Database to watch.
- **`mongodb.connection.collection`**: Collection to watch.
- **`checkpoint.*`**: Oplog checkpoint and bootstrap behavior.
- **`partition.*`**: Worker/partition distribution and consumer group settings.
- **`metric.port`**: Prometheus metrics port.
- **`logger.logLevel`**: Log level (`info`, `debug`, etc.).

For the full configuration tables and examples, see the `go-mongo-cdc` README.

#### Kafka Configuration

The Kafka-specific configuration is defined by the `config.Kafka` struct:

| Field                               | Type             | Required | Default       | Description                                                                                                            |
|-------------------------------------|------------------|----------|---------------|------------------------------------------------------------------------------------------------------------------------|
| `kafka.topic`                       | string           | yes      | -             | Default Kafka topic name. If a topic is not set in the mapper, this value is used.                                     |
| `kafka.brokers`                     | []string         | yes      | -             | List of Kafka broker addresses (e.g. `["localhost:9092"]`).                                                            |
| `kafka.producerBatchSize`           | int              | no       | 2000          | Maximum number of messages per batch. If exceeded, the batch is flushed.                                               |
| `kafka.producerBatchBytes`          | any (int/string) | no       | `1mb`         | Maximum batch size in bytes. Can be provided as a number or as a string (e.g. `\"1mb\"`, `\"900kb\"`).                 |
| `kafka.producerBatchTickerDuration` | time.Duration    | no       | 10s           | Interval for automatically flushing batches that wait too long.                                                        |
| `kafka.readTimeout`                 | time.Duration    | no       | 30s           | Timeout for Kafka read operations.                                                                                     |
| `kafka.writeTimeout`                | time.Duration    | no       | 30s           | Timeout for Kafka write operations.                                                                                    |
| `kafka.requiredAcks`                | int              | no       | 1             | Acknowledgement level: `0` = fire-and-forget, `1` = wait for leader, `-1` = wait for full ISR.                         |
| `kafka.compression`                 | int8             | no       | 0             | Compression type: `0=None`, `1=Gzip`, `2=Snappy`, `3=Lz4`, `4=Zstd`. Invalid values cause a panic.                     |
| `kafka.balancer`                    | string           | no       | Hash          | Balancer strategy. Available: `Hash`, `LeastBytes`, `RoundRobin`, `ReferenceHash`, `CRC32Balancer`, `Murmur2Balancer`. |
| `kafka.clientID`                    | string           | no       | -             | Client identifier communicated to brokers.                                                                             |
| `kafka.metadataTTL`                 | time.Duration    | no       | 60s           | TTL for metadata cached by the Kafka client.                                                                           |
| `kafka.metadataTopics`              | []string         | no       | -             | Topic names for the metadata cache. In large clusters, limiting topics can reduce memory usage.                        |
| `kafka.producerMaxAttempts`         | int              | no       | `math.MaxInt` | Maximum number of attempts to deliver a message.                                                                       |
| `kafka.secureConnection`            | bool             | no       | false         | Enables TLS-based secure Kafka connection.                                                                             |
| `kafka.rootCAPath`                  | string           | no       | -             | Root CA file path for TLS.                                                                                             |
| `kafka.interCAPath`                 | string           | no       | -             | Intermediate CA file path for TLS.                                                                                     |
| `kafka.scramUsername`               | string           | no       | -             | SCRAM username for SASL authentication.                                                                                |
| `kafka.scramPassword`               | string           | no       | -             | SCRAM password for SASL authentication.                                                                                |
| `kafka.allowAutoTopicCreation`      | bool             | no       | false         | If `false`, the client checks topic existence via `CheckTopic` and expects topics to be created beforehand.            |
| `kafka.rejectionLog.topic`          | string           | no       | -             | Topic name used for rejection log messages.                                                                            |
| `kafka.rejectionLog.includeValue`   | bool             | no       | false         | Whether to include the original message value in rejection log entries.                                                |

### SinkResponseHandler and Completion Handler

The Kafka producer exposes two callback mechanisms:

- **SinkResponseHandler**: Per-message callbacks (`OnInit`, `OnSuccess`, `OnError`).
- **Completion Handler**: Per-batch callback invoked after a batch is produced (success or failure).

Example sink response handler:

```go
type customSinkResponseHandler struct{}

func (c *customSinkResponseHandler) OnInit(ctx *kafka.SinkResponseHandlerInitContext) {
    log.Printf("Custom sink response handler initialized - Topic: %s", ctx.Config.Topic)
}

func (c *customSinkResponseHandler) OnSuccess(ctx *kafka.SinkResponseHandlerContext) {
    log.Printf("Message sent successfully - Key: %s", string(ctx.Message.Key))
}

func (c *customSinkResponseHandler) OnError(ctx *kafka.SinkResponseHandlerContext) {
    log.Printf("Message failed - Key: %s, Error: %v", string(ctx.Message.Key), ctx.Err)
}
```

You can plug it into the builder:

```go
connector, err := mongokafka.NewConnectorBuilder(cfg).
    SetSinkResponseHandler(&customSinkResponseHandler{}).
    SetCompletionHandler(customCompletionHandler).
    Build()
```

For a ready-to-use rejection log implementation, you can use `kafka.NewRejectionLogSinkResponseHandler()`.

### Metrics & Monitoring

`go-mongo-cdc-kafka` exposes:

- **MongoCDC metrics**: All metrics provided by `go-mongo-cdc`, including insert/update/delete/replace counters, checkpoint metrics, CDC latency, etc.
- **Kafka metrics**: Producer-level metrics such as current batch latency and batch produce latency.

#### Exposed Kafka Metrics

Below is the list of Prometheus metrics exposed by the Kafka side of the connector:

| Metric Name                                           | Type   | Description                                        | Labels |
|-------------------------------------------------------|--------|----------------------------------------------------|--------|
| `go_mongo_cdc_kafka_kafka_connector_latency_ms_current`        | Gauge  | Time spent from CDC event to Kafka write (ms).     | N/A    |
| `go_mongo_cdc_kafka_kafka_connector_batch_produce_latency_ms_current` | Gauge  | Time to produce messages in the current batch (ms). | N/A    |

With the `example/grafana` setup you can:

- Start MongoDB, `go-mongo-cdc-kafka`, Prometheus and Grafana in a single docker-compose network.
- Use the provided Grafana dashboard to monitor CDC and Kafka metrics in real-time.

### Running Examples

From the project root:

```bash
make deps      # download dependencies
make test      # run tests
make example   # run simple example (example/complete-builder/main.go)
```

For the Grafana example:

```bash
cd example/grafana
docker-compose up --build
```

This will start MongoDB, Prometheus, Grafana and the `go-mongo-cdc-kafka` example app together so you can explore metrics visually.

### Contributing

Contributions are welcome. Feel free to open issues for bug reports or feature requests, and send pull requests for improvements or new examples.

### License

Released under the `MIT License` (see `LICENSE` for details).


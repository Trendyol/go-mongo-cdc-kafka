package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"math"
	"net"
	"os"
	"time"

	"github.com/segmentio/kafka-go/sasl"

	"github.com/Trendyol/go-mongo-cdc-kafka/config"
	"github.com/Trendyol/go-mongo-cdc/logger"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
)

type Client interface {
	GetPartitions(topic string) ([]int, error)
	Producer(completionHandler func(messages []kafka.Message, err error)) *kafka.Writer
	Consumer(topic string, partition int, startOffset int64) *kafka.Reader
	CheckTopic(topic string) error
}

type client struct {
	addr        net.Addr
	kafkaClient *kafka.Client
	config      *config.Connector
	transport   *kafka.Transport
	dialer      *kafka.Dialer
}

type tlsContent struct {
	config *tls.Config
	sasl   sasl.Mechanism
}

func newTLSContent(
	scramUsername,
	scramPassword,
	rootCAPath,
	interCAPath,
	rootCA,
	interCA string,
) (*tlsContent, error) {
	mechanism, err := scram.Mechanism(scram.SHA512, scramUsername, scramPassword)
	if err != nil {
		return nil, err
	}

	certCount := 0
	caCertPool := x509.NewCertPool()

	if rootCAPath != "" {
		caCert, err := os.ReadFile(os.ExpandEnv(rootCAPath))
		if err != nil {
			logger.Log.Error("an error occurred while reading ca.pem file! Error: %s", err.Error())
			return nil, err
		}
		caCertPool.AppendCertsFromPEM(caCert)
		certCount++
	}

	if interCAPath != "" {
		intCert, err := os.ReadFile(os.ExpandEnv(interCAPath))
		if err != nil {
			logger.Log.Error("an error occurred while reading int.pem file! Error: %s", err.Error())
			return nil, err
		}
		caCertPool.AppendCertsFromPEM(intCert)
		certCount++
	}

	if rootCA != "" {
		caCertPool.AppendCertsFromPEM([]byte(rootCA))
		certCount++
	}

	if interCA != "" {
		caCertPool.AppendCertsFromPEM([]byte(interCA))
		certCount++
	}

	if certCount == 0 {
		err := errors.New("certPool is empty")
		logger.Log.Error("an error occurred while creating tls content! Error: %s", err.Error())
		return nil, err
	}

	return &tlsContent{
		config: &tls.Config{
			RootCAs:    caCertPool,
			MinVersion: tls.VersionTLS12,
		},
		sasl: mechanism,
	}, nil
}

func (c *client) GetPartitions(topic string) ([]int, error) {
	response, err := c.kafkaClient.Metadata(context.Background(), &kafka.MetadataRequest{
		Topics: []string{topic},
		Addr:   c.addr,
	})
	if err != nil {
		return nil, err
	}

	var partitions []int

	for _, responseTopic := range response.Topics {
		if responseTopic.Name == topic {
			for _, partition := range responseTopic.Partitions {
				partitions = append(partitions, partition.ID)
			}
		}
	}

	return partitions, nil
}

func (c *client) CheckTopic(topic string) error {
	response, err := c.kafkaClient.Metadata(context.Background(), &kafka.MetadataRequest{
		Topics: []string{topic},
		Addr:   c.addr,
	})
	if err != nil {
		return err
	}

	for _, responseTopic := range response.Topics {
		if responseTopic.Error != nil {
			return fmt.Errorf("topic=%s, err=%v", responseTopic.Name, responseTopic.Error)
		}
	}

	return nil
}

func (c *client) Producer(completionHandler func(messages []kafka.Message, err error)) *kafka.Writer {
	return &kafka.Writer{
		Addr:                   kafka.TCP(c.config.Kafka.Brokers...),
		Balancer:               c.config.Kafka.GetBalancer(),
		BatchSize:              c.config.Kafka.ProducerBatchSize,
		BatchBytes:             math.MaxInt,
		BatchTimeout:           time.Nanosecond,
		MaxAttempts:            c.config.Kafka.ProducerMaxAttempts,
		ReadTimeout:            c.config.Kafka.ReadTimeout,
		WriteTimeout:           c.config.Kafka.WriteTimeout,
		RequiredAcks:           kafka.RequiredAcks(c.config.Kafka.RequiredAcks),
		Compression:            kafka.Compression(c.config.Kafka.GetCompression()),
		Transport:              c.transport,
		AllowAutoTopicCreation: c.config.Kafka.AllowAutoTopicCreation,
		Completion:             completionHandler,
	}
}

func (c *client) Consumer(topic string, partition int, startOffset int64) *kafka.Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers:     c.config.Kafka.Brokers,
		Topic:       topic,
		Partition:   partition,
		StartOffset: startOffset,
	}

	if c.dialer != nil {
		readerConfig.Dialer = c.dialer
	}

	return kafka.NewReader(readerConfig)
}

func NewClient(config *config.Connector) Client {
	addr := kafka.TCP(config.Kafka.Brokers...)

	newClient := &client{
		addr: addr,
		kafkaClient: &kafka.Client{
			Addr: addr,
		},
		config: config,
	}

	newClient.transport = &kafka.Transport{
		MetadataTTL:    config.Kafka.MetadataTTL,
		MetadataTopics: config.Kafka.MetadataTopics,
		ClientID:       config.Kafka.ClientID,
	}

	if config.Kafka.SecureConnection {
		tlsContent, err := newTLSContent(
			config.Kafka.ScramUsername,
			config.Kafka.ScramPassword,
			config.Kafka.RootCAPath,
			config.Kafka.InterCAPath,
			config.Kafka.RootCA,
			config.Kafka.InterCA,
		)
		if err != nil {
			logger.Log.Error("error while creating new tls content, err: %v", err)
			panic(err)
		}

		newClient.transport.TLS = tlsContent.config
		newClient.transport.SASL = tlsContent.sasl

		newClient.dialer = &kafka.Dialer{
			Timeout:       10 * time.Second,
			DualStack:     true,
			TLS:           tlsContent.config,
			SASLMechanism: tlsContent.sasl,
		}
	}
	newClient.kafkaClient.Transport = newClient.transport
	return newClient
}

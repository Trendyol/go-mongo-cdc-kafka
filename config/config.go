package config

import (
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/Trendyol/go-mongo-cdc-kafka/helpers"

	cdcConfig "github.com/Trendyol/go-mongo-cdc/config"
	"github.com/segmentio/kafka-go"
)

type Config struct {
	CDC   cdcConfig.Config `yaml:",inline" mapstructure:",squash"`
	Kafka Kafka            `yaml:"kafka" mapstructure:"kafka"`
}

type Kafka struct {
	ProducerBatchBytes          any           `yaml:"producerBatchBytes"`
	InterCAPath                 string        `yaml:"interCAPath"`
	InterCA                     string        `yaml:"interCA"`
	ScramUsername               string        `yaml:"scramUsername"`
	ScramPassword               string        `yaml:"scramPassword"`
	RootCAPath                  string        `yaml:"rootCAPath"`
	RootCA                      string        `yaml:"rootCA"`
	ClientID                    string        `yaml:"clientID"`
	Balancer                    string        `yaml:"balancer"`
	Topic                       string        `yaml:"topic"`
	Brokers                     []string      `yaml:"brokers"`
	MetadataTopics              []string      `yaml:"metadataTopics"`
	RejectionLog                RejectionLog  `yaml:"rejectionLog"`
	ProducerMaxAttempts         int           `yaml:"producerMaxAttempts"`
	ReadTimeout                 time.Duration `yaml:"readTimeout"`
	WriteTimeout                time.Duration `yaml:"writeTimeout"`
	RequiredAcks                int           `yaml:"requiredAcks"`
	ProducerBatchSize           int           `yaml:"producerBatchSize"`
	MetadataTTL                 time.Duration `yaml:"metadataTTL"`
	ProducerBatchTickerDuration time.Duration `yaml:"producerBatchTickerDuration"`
	Compression                 int8          `yaml:"compression"`
	SecureConnection            bool          `yaml:"secureConnection"`
	AllowAutoTopicCreation      bool          `yaml:"allowAutoTopicCreation"`
}

type RejectionLog struct {
	Topic        string `yaml:"topic"`
	IncludeValue bool   `yaml:"includeValue"`
}

func (k *Kafka) GetBalancer() kafka.Balancer {
	switch k.Balancer {
	case "", "Hash":
		return &kafka.Hash{}
	case "LeastBytes":
		return &kafka.LeastBytes{}
	case "RoundRobin":
		return &kafka.RoundRobin{}
	case "ReferenceHash":
		return &kafka.ReferenceHash{}
	case "CRC32Balancer":
		return kafka.CRC32Balancer{}
	case "Murmur2Balancer":
		return kafka.Murmur2Balancer{}
	default:
		panic(errors.New("invalid kafka balancer method, given: " + k.Balancer))
	}
}

func (k *Kafka) GetCompression() int8 {
	if k.Compression < 0 || k.Compression > 4 {
		panic(errors.New("invalid kafka compression method, given: " + strconv.Itoa(int(k.Compression))))
	}
	return k.Compression
}

func (c *Config) ApplyDefaults() {
	if c.Kafka.ReadTimeout == 0 {
		c.Kafka.ReadTimeout = 30 * time.Second
	}

	if c.Kafka.WriteTimeout == 0 {
		c.Kafka.WriteTimeout = 30 * time.Second
	}

	if c.Kafka.ProducerBatchTickerDuration == 0 {
		c.Kafka.ProducerBatchTickerDuration = 10 * time.Second
	}

	if c.Kafka.ProducerBatchSize == 0 {
		c.Kafka.ProducerBatchSize = 2000
	}

	if c.Kafka.ProducerBatchBytes == nil {
		c.Kafka.ProducerBatchBytes = helpers.ResolveUnionIntOrStringValue("1mb")
	}

	if c.Kafka.RequiredAcks == 0 {
		c.Kafka.RequiredAcks = 1
	}

	if c.Kafka.MetadataTTL == 0 {
		c.Kafka.MetadataTTL = 60 * time.Second
	}

	if c.Kafka.ProducerMaxAttempts == 0 {
		c.Kafka.ProducerMaxAttempts = math.MaxInt
	}
}

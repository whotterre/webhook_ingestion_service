package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
)

var (
	sha256Gen scram.HashGeneratorFcn = sha256.New
	sha512Gen scram.HashGeneratorFcn = sha512.New
)

type xdgSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

func (x *xdgSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *xdgSCRAMClient) Step(challenge string) (response string, err error) {
	return x.ClientConversation.Step(challenge)
}

func (x *xdgSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

type Config struct {
	Brokers       []string
	Topic         string
	Username      string
	Password      string
	SASLMechanism string
	CACertPath    string
	TLS           bool
}

type Publisher struct {
	producer sarama.SyncProducer
	topic    string
}

// ConfigureNetwork applies TLS and SASL settings to Sarama config.
func ConfigureNetwork(saramaConfig *sarama.Config, cfg Config) error {
	if err := configureTLS(saramaConfig, cfg); err != nil {
		return err
	}
	return configureSASL(saramaConfig, cfg)
}

func NewPublisher(cfg Config) (*Publisher, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka brokers are required")
	}
	if strings.TrimSpace(cfg.Topic) == "" {
		return nil, fmt.Errorf("kafka topic is required")
	}

	saramaConfig := sarama.NewConfig()
	saramaConfig.Version = sarama.V2_8_0_0
	saramaConfig.Producer.RequiredAcks = sarama.WaitForAll
	saramaConfig.Producer.Return.Successes = true
	saramaConfig.Producer.Return.Errors = true

	if err := ConfigureNetwork(saramaConfig, cfg); err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaConfig)
	if err != nil {
		return nil, err
	}

	return &Publisher{producer: producer, topic: cfg.Topic}, nil
}

func (p *Publisher) Publish(payload []byte) error {
	if p == nil {
		return nil
	}

	_, _, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(payload),
	})
	return err
}

func (p *Publisher) Close() error {
	if p == nil || p.producer == nil {
		return nil
	}
	return p.producer.Close()
}

func configureTLS(cfg *sarama.Config, appCfg Config) error {
	if !appCfg.TLS {
		return nil
	}

	rootCAs, err := x509.SystemCertPool()
	if err != nil || rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	if appCfg.CACertPath != "" {
		caCert, err := os.ReadFile(appCfg.CACertPath)
		if err != nil {
			return err
		}
		if ok := rootCAs.AppendCertsFromPEM(caCert); !ok {
			return fmt.Errorf("failed to load kafka CA certificate from %s", appCfg.CACertPath)
		}
	}

	cfg.Net.TLS.Enable = true
	cfg.Net.TLS.Config = &tls.Config{
		RootCAs:    rootCAs,
		MinVersion: tls.VersionTLS12,
	}

	return nil
}

func configureSASL(cfg *sarama.Config, appCfg Config) error {
	if appCfg.Username == "" && appCfg.Password == "" && appCfg.SASLMechanism == "" {
		return nil
	}

	mechanism := strings.ToLower(strings.TrimSpace(appCfg.SASLMechanism))
	if mechanism == "" {
		mechanism = "scram-sha-512"
	}

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.User = appCfg.Username
	cfg.Net.SASL.Password = appCfg.Password
	cfg.Net.SASL.Handshake = true

	switch mechanism {
	case "plain", "plaintext":
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "scram-sha-256":
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{HashGeneratorFcn: sha256Gen}
		}
	case "scram-sha-512":
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &xdgSCRAMClient{HashGeneratorFcn: sha512Gen}
		}
	default:
		return fmt.Errorf("unsupported kafka sasl mechanism %q", appCfg.SASLMechanism)
	}

	return nil
}

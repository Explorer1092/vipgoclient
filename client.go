// Copyright 2019 zhvala
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package pulsar

import (
	"context"
	"crypto/tls"
	"errors"
	"math/rand"
	"strings"
	"time"

	"github.com/jiazhai/vipgoclient/auth"
	"github.com/jiazhai/vipgoclient/utils"
)

const (
	defaultOpTimeout = time.Second * 15
)

// NewClient returns a Pulsar client for the given configuration options
func NewClient(ctx context.Context, cfg ClientConfig) (Client, error) {
	if err := cfg.SetDefault(); err != nil {
		return nil, err
	}

	connPool := &ConnectionPool{
		Timeout:    cfg.OperationTimeout,
		AuthMethod: cfg.AuthMethod,
	}

	lookup := NewLookupService(ctx, cfg.URL, cfg.TLSConfig, connPool)

	var tls bool
	if strings.HasPrefix(cfg.URL, "https://") || strings.HasPrefix(cfg.URL, "pulsar+ssl://") {
		tls = true
	}

	cli := &client{
		Ctx:              ctx,
		URL:              cfg.URL,
		ConnPool:         connPool,
		Lookup:           lookup,
		UseTLS:           tls,
		OperationTimeout: cfg.OperationTimeout,
		ProducerID:       &MonotonicID{ID: 0},
		ConsumerID:       &MonotonicID{ID: 0},
	}

	return cli, nil
}

// ClientConfig pulsar client configuration options.
type ClientConfig struct {
	// URL the lookup URL for the Pulsar service.
	// It can be either a binary address(pulsar:// or pulsar+ssl://)
	// or an HTTP address(http:// or https://).
	// This parameter is required
	URL string

	// TLSConfig tls configuration.
	TLSConfig *tls.Config

	// OperationTimeout (default: 5 seconds)
	// Timeout for dialing to server and creating producer, consumer, reader.
	OperationTimeout time.Duration

	// Authentication configure the authentication provider. (default: no authentication)
	// Example: `Authentication: NewAuthenticationTLS("my-cert.pem", "my-key.pem")`
	AuthMethod auth.Authentication
}

// SetDefault set default if not set.
func (cfg *ClientConfig) SetDefault() error {
	if cfg.URL == "" {
		return errors.New("empty service url")
	}

	if cfg.OperationTimeout == 0 {
		cfg.OperationTimeout = defaultOpTimeout
	}

	return nil
}

// Client opaque interface that provides the ability to interact with pulsar.
type Client interface {
	// CreateProducer create the producer instance
	// This method will block until the producer is created successfully
	CreateProducer(ProducerConfig) (Producer, error)

	// Subscribe create a `Consumer` by subscribing to a topic.
	//
	// If the subscription does not exist, a new subscription will be created and all messages published after the
	// creation will be retained until acknowledged, even if the consumer is not connected
	Subscribe(ConsumerConfig) (Consumer, error)

	// Close the Client and free associated resources
	Close() error
}

type client struct {
	Ctx              context.Context
	URL              string
	UseTLS           bool
	Lookup           LookupService
	ConnPool         *ConnectionPool
	OperationTimeout time.Duration
	ProducerID       *MonotonicID
	ConsumerID       *MonotonicID
}

func (cli *client) CreateProducer(cfg ProducerConfig) (Producer, error) {
	if err := cfg.SetDefault(); err != nil {
		return nil, err
	}
	topicObject, err := ParseTopicName(cfg.Topic)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(cli.Ctx, cli.OperationTimeout)
	defer cancel()
	partitions, err := cli.Lookup.GetPartitions(ctx, topicObject.String())
	if err != nil {
		return nil, err
	}

	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = cli.OperationTimeout
	}
	if partitions > 1 {
		// 这个过程可能会比较慢, 待优化
		return cli.createPartitionedProducerImpl(topicObject, partitions, cfg)
	}
	return cli.createProducerImpl(topicObject.String(), &MonotonicID{ID: 0}, cfg)
}

func (cli *client) Subscribe(cfg ConsumerConfig) (Consumer, error) {
	if err := cfg.SetDefault(); err != nil {
		return nil, err
	}
	topicObject, err := ParseTopicName(cfg.Topic)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(cli.Ctx, cli.OperationTimeout)
	defer cancel()
	partitions, err := cli.Lookup.GetPartitions(ctx, topicObject.String())
	if err != nil {
		return nil, err
	}

	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = cli.OperationTimeout
	}

	queue := make(chan *Message, cfg.ReceiverQueueSize)
	if partitions > 1 {
		// 这个过程可能会比较慢, 待优化
		return cli.createPartitionedConsumerImpl(topicObject, partitions, cfg, queue)
	}
	return cli.createConsumerImpl(topicObject, -1, cfg, queue)
}

func (cli *client) Close() error {
	cli.ConnPool.Close()
	return nil
}

func (cli *client) getConnection(topic string) (*ConnWrapper, error) {
	ctx, cancel := context.WithTimeout(cli.Ctx, cli.OperationTimeout)
	defer cancel()
	lookupData, err := cli.Lookup.LookupTopic(ctx, topic)
	if err != nil {
		return nil, err
	}

	var logicAddr, phyAddr string
	if cli.UseTLS {
		logicAddr = lookupData.BrokerServiceUrlTls
	} else {
		logicAddr = lookupData.BrokerServiceUrl
	}

	if lookupData.ProxyThroughServiceUrl {
		// what will happen when URL is http
		phyAddr = cli.URL
	} else {
		phyAddr = logicAddr
	}
	return cli.ConnPool.GetConnection(logicAddr, phyAddr)
}

func (cli *client) getMessageRouter(mode MessageRoutingMode, partitions uint32) MessageRouter {
	var router MessageRouter
	switch mode {
	case RoundRobinDistribution:
		router = &RoundRobinRouter{
			Partition: partitions,
		}
	case UseSinglePartition:
		fallthrough
	default:
		// random pick?
		rand.Seed(time.Now().Unix())
		router = &SinglePartitionRouter{
			Partition:       partitions,
			SinglePartition: rand.Uint32() % partitions,
		}
	}
	return router
}

func (cli *client) createPartitionedProducerImpl(topicObject *TopicObject, partitions uint32, cfg ProducerConfig) (Producer, error) {
	producers := make([]Producer, 0, partitions)
	var createErr error
	defer func() {
		if createErr != nil {
			for _, p := range producers {
				p.Close()
			}
		}
	}()
	// share ?
	// 如果要保证唯一，就共享一个
	// 如果要保证连续，就每个partition producer 一个
	seqID := &MonotonicID{ID: 0}
	cfg.MaxPendingMessages = cfg.MaxPendingMessagesAcrossPartitions
	for i := uint32(0); i < partitions; i++ {
		p, err := cli.createProducerImpl(topicObject.GetPartitionName(i), seqID, cfg)
		if err != nil {
			createErr = err
			return nil, err
		}
		producers = append(producers, p)
	}

	var router MessageRouter
	if cfg.RoutingMode != CustomPartition {
		router = cli.getMessageRouter(cfg.RoutingMode, partitions)
	} else {
		router = cfg.CostomRouter
	}
	return &PartitionedProducerImpl{
		TopicName:   topicObject.String(),
		TopicObject: topicObject,
		Partitions:  partitions,
		Producers:   producers,
		Router:      router,
		SequenceID:  seqID,
	}, nil
}

func (cli *client) createProducerImpl(topic string, seqID *MonotonicID, cfg ProducerConfig) (Producer, error) {
	conn, err := cli.getConnection(topic)
	if err != nil {
		return nil, err
	}
	producerID := cli.ProducerID.Next()

	producer := &ProducerImpl{
		Ctx:          cli.Ctx,
		TopicName:    topic,
		ID:           producerID,
		ProducerName: cfg.Name,
		SequenceID:   seqID,
		Conn:         conn,
		PendingSize:  cfg.MaxPendingMessages,
		Compression:  cfg.Compression,
		SendTimeout:  cfg.SendTimeout,
		Closedc:      make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(cli.Ctx, cli.OperationTimeout)
	defer cancel()
	err = conn.RegisterProducer(ctx, topic, cfg.Name, producerID, producer)
	if err != nil {
		return nil, err
	}
	return producer, nil
}

func (cli *client) createPartitionedConsumerImpl(topicObject *TopicObject, partitions uint32, cfg ConsumerConfig, queue chan *Message) (Consumer, error) {
	consumers := make([]Consumer, 0, partitions)
	var createErr error
	defer func() {
		if createErr != nil {
			for _, p := range consumers {
				p.Close()
			}
		}
	}()

	cfg.ReceiverQueueSize = cfg.ReceiverQueueSize / partitions
	for i := uint32(0); i < partitions; i++ {
		c, err := cli.createConsumerImpl(topicObject, int32(i), cfg, queue)
		if err != nil {
			createErr = err
			return nil, err
		}
		consumers = append(consumers, c)
	}

	return &PartitionedConsumerImpl{
		TopicObject:      topicObject,
		SubscriptionName: cfg.SubscriptionName,
		Partitions:       partitions,
		Consumers:        consumers,
		Queue:            queue,
		Closedc:          make(chan struct{}),
		EndOfTopicc:      make(chan struct{}),
	}, nil
}

func (cli *client) createConsumerImpl(topicObject *TopicObject, index int32, cfg ConsumerConfig, queue chan *Message) (Consumer, error) {
	var topic string
	if index >= 0 {
		topic = topicObject.GetPartitionName(uint32(index))
	} else {
		topic = topicObject.String()
	}
	conn, err := cli.getConnection(topic)
	if err != nil {
		return nil, err
	}
	consumerID := cli.ConsumerID.Next()
	consumerName := utils.RandString(8)

	consumer := &ConsumerImpl{
		Ctx:              cli.Ctx,
		TopicName:        topic,
		SubscriptionName: cfg.SubscriptionName,
		PartitionIndex:   index,
		ID:               consumerID,
		Name:             consumerName,
		AckTimeout:       cfg.AckTimeout,
		Conn:             conn,
		Queue:            queue,
		QueueSize:        cfg.ReceiverQueueSize,
		Closedc:          make(chan struct{}),
		EndOfTopicc:      make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(cli.Ctx, cli.OperationTimeout)
	defer cancel()
	err = conn.Subscribe(ctx, topic, cfg.SubscriptionName, consumerName,
		cfg.Type, consumerID, cfg.SubscriptionInitPos, consumer)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}

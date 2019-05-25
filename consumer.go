//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jiazhai/vipgoclient/frame"
)

// Types of subscription supported by Pulsar
type SubscriptionType int

const (
	// There can be only 1 consumer on the same topic with the same subscription name
	Exclusive SubscriptionType = iota

	// Multiple consumer will be able to use the same subscription name and the messages will be dispatched according to
	// a round-robin rotation between the connected consumers
	Shared

	// Multiple consumer will be able to use the same subscription name but only 1 consumer will receive the messages.
	// If that consumer disconnects, one of the other connected consumers will start receiving messages.
	Failover

	KeyShare
)

type InitialPosition int

const (
	// Latest position which means the start consuming position will be the last message
	Latest InitialPosition = iota

	// Earliest position which means the start consuming position will be the first message
	Earliest
)

// ConsumerBuilder is used to configure and create instances of Consumer
type ConsumerConfig struct {
	// Specify the topic this consumer will subscribe on.
	// Either a topic, a list of topics or a topics pattern are required when subscribing
	Topic string

	// Specify the subscription name for this consumer
	// This argument is required when subscribing
	SubscriptionName string

	// Set the timeout for acking messages.
	// default (30 seconds)
	AckTimeout time.Duration

	// Select the subscription type to be used when subscribing to the topic.
	// Default is `Exclusive`
	Type SubscriptionType

	// InitialPosition at which the cursor will be set when subscribe
	// Default is `Latest`
	SubscriptionInitPos InitialPosition

	// Sets the size of the consumer receive queue.
	// The consumer receive queue controls how many messages can be accumulated by the `Consumer` before the
	// application calls `Consumer.receive()`. Using a higher value could potentially increase the consumer
	// throughput at the expense of bigger memory utilization.
	// Default value is `1000` messages and should be good for most use cases.
	ReceiverQueueSize uint32
}

func (cfg *ConsumerConfig) SetDefault() error {
	if cfg.ReceiverQueueSize < 1000 {
		cfg.ReceiverQueueSize = 1000
	}
	if cfg.AckTimeout == 0 {
		cfg.AckTimeout = time.Second * 30
	}
	return nil
}

// An interface that abstracts behavior of Pulsar's consumer
type Consumer interface {
	// Get the topic for the consumer
	Topic() string

	// Get a subscription for the consumer
	Subscription() string

	// Unsubscribe the consumer
	Unsubscribe() error

	// Receives a single message.
	// This calls blocks until a message is available.
	Receive(context.Context) (*Message, error)

	//Ack the consumption of a single message
	Ack(*Message) error

	// Ack the consumption of a single message, identified by its MessageID
	AckID(MessageID) error

	// Close the consumer and stop the broker to push more messages
	Close() error

	// Reset the subscription associated with this consumer to a specific message id.
	// The message id can either be a specific message or represent the first or last messages in the topic.
	//
	// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
	//       seek() on the individual partitions.
	Seek(msgID MessageID) error

	// Redelivers all the unacknowledged messages. In Failover mode, the request is ignored if the consumer is not
	// active for the given topic. In Shared mode, the consumers messages to be redelivered are distributed across all
	// the connected consumers. This is a non blocking call and doesn't throw an exception. In case the connection
	// breaks, the messages are redelivered after reconnect.
	RedeliverUnackedMessages() error
}

type ConsumerImpl struct {
	Ctx              context.Context
	TopicName        string
	SubscriptionName string
	PartitionIndex   int32
	ID               uint64
	Name             string
	AckTimeout       time.Duration
	Conn             *ConnWrapper

	Queue            chan *Message
	QueueSize        uint32
	availablePermits uint32

	Mu           sync.Mutex // protects following
	IsClosed     bool
	Closedc      chan struct{}
	IsEndOfTopic bool
	EndOfTopicc  chan struct{}
}

func (c *ConsumerImpl) Topic() string {
	return c.TopicName
}

// Get a subscription for the consumer
func (c *ConsumerImpl) Subscription() string {
	return c.SubscriptionName
}

// Unsubscribe the consumer
func (c *ConsumerImpl) Unsubscribe() error {
	return c.Conn.Unubscribe(c.Ctx, c.ID)
}

// Receives a single message.
// This calls blocks until a message is available.
func (c *ConsumerImpl) Receive(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.Closedc:
		return nil, errors.New("consumer closed")
	case <-c.EndOfTopicc:
		return nil, errors.New("end of topic")
	case msg, ok := <-c.Queue:
		if ok {
			c.flushPermits()
			return msg, nil
		}
		return nil, errors.New("recv queue closed")
	}
}

func (c *ConsumerImpl) flushPermits() error {
	c.availablePermits++
	if c.availablePermits > c.QueueSize/2 {
		permit := c.availablePermits
		c.availablePermits = 0
		if err := c.Conn.Flow(c.Ctx, c.ID, permit); err != nil {
			c.availablePermits += permit
			return err
		}
	}
	return nil
}

//Ack the consumption of a single message
func (c *ConsumerImpl) Ack(msg *Message) error {
	return c.AckID(msg.ID)
}

// Ack the consumption of a single message, identified by its MessageID
func (c *ConsumerImpl) AckID(msgID MessageID) error {
	ctx, cancel := context.WithTimeout(c.Ctx, c.AckTimeout)
	defer cancel()
	return c.Conn.Ack(ctx, c.ID, msgID)
}

// Close the consumer and stop the broker to push more messages
func (c *ConsumerImpl) Close() error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.IsClosed {
		return nil
	}

	if err := c.Conn.CloseConsumer(c.Ctx, c.ID); err != nil {
		return err
	}
	close(c.Closedc)
	c.IsClosed = true
	return nil
}

// Seek reset the subscription associated with this consumer to a specific message id.
// The message id can either be a specific message or represent the first or last messages in the topic.
//
// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
//       seek() on the individual partitions.
func (c *ConsumerImpl) Seek(msgID MessageID) error {
	return c.Conn.Seek(c.Ctx, c.ID, msgID)
}

// RedeliverUnackedMessages redelivers all the unacknowledged messages.
func (c *ConsumerImpl) RedeliverUnackedMessages() error {
	return c.Conn.RedeliverUnacked(c.Ctx, c.ID)
}

func (c *ConsumerImpl) HandleConnected(f frame.Frame) error {
	return c.Conn.Flow(c.Ctx, c.ID, c.QueueSize)
}

func (c *ConsumerImpl) HandleMessage(f frame.Frame) error {
	id := f.BaseCmd.GetMessage().GetMessageId()
	msgID := MessageID{
		LedgerId:   id.GetLedgerId(),
		EntryId:    id.GetEntryId(),
		Partition:  c.PartitionIndex,
		BatchIndex: id.GetBatchIndex(),
	}
	msg := &Message{
		ID:          msgID,
		Topic:       c.TopicName,
		Properties:  UnmarshalProperties(f.Metadata.GetProperties()),
		Payload:     f.Payload,
		PublishTime: time.Unix(int64(f.Metadata.GetPublishTime()), 0),
		EventTime:   time.Unix(int64(f.Metadata.GetEventTime()), 0),
		Key:         f.Metadata.GetPartitionKey(),
	}

	select {
	case c.Queue <- msg:
		return nil
	default:
		// not very true how to handle this status
		return fmt.Errorf("consumer message queue on topic %s is full (capacity = %d)", c.TopicName, cap(c.Queue))
	}

}

func (c *ConsumerImpl) HandleEndOfTopic(f frame.Frame) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.IsEndOfTopic {
		return nil
	}

	c.IsEndOfTopic = true
	close(c.EndOfTopicc)

	return nil
}

func (c *ConsumerImpl) HandleClose(f frame.Frame) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()

	if c.IsClosed {
		return nil
	}

	c.IsClosed = true
	close(c.Closedc)

	return nil
}

type PartitionedConsumerImpl struct {
	Ctx              context.Context
	SubscriptionName string
	TopicObject      *TopicObject
	Partitions       uint32

	Consumers []Consumer
	Queue     chan *Message

	Mu           sync.Mutex // protects following
	IsClosed     bool
	Closedc      chan struct{}
	IsEndOfTopic bool
	EndOfTopicc  chan struct{}
}

// Topic get a topic for the consumer
func (pc *PartitionedConsumerImpl) Topic() string {
	return pc.TopicObject.String()
}

// Subscription get a subscription for the consumer
func (pc *PartitionedConsumerImpl) Subscription() string {
	return pc.SubscriptionName
}

// Unsubscribe the consumer
func (pc *PartitionedConsumerImpl) Unsubscribe() error {
	pc.Mu.Lock()
	defer pc.Mu.Unlock()

	var errMsg string
	for _, c := range pc.Consumers {
		if err := c.Unsubscribe(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", c.Topic(), c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

// Receive a single message.
// This calls blocks until a message is available.
func (pc *PartitionedConsumerImpl) Receive(ctx context.Context) (*Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-pc.Closedc:
		return nil, errors.New("consumer closed")
	case <-pc.EndOfTopicc:
		return nil, errors.New("end of topic")
	case msg, ok := <-pc.Queue:
		if ok {
			return msg, nil
		}
		return nil, errors.New("recv queue closed")
	}
}

//Ack the consumption of a single message
func (pc *PartitionedConsumerImpl) Ack(msg *Message) error {
	return pc.AckID(msg.ID)
}

// Ack the consumption of a single message, identified by its MessageID
func (pc *PartitionedConsumerImpl) AckID(msgID MessageID) error {
	partition := uint32(msgID.Partition)
	if partition < 0 || partition >= pc.Partitions {
		return errors.New("invalid partition index")
	}
	return pc.Consumers[partition].AckID(msgID)
}

// Close the consumer and stop the broker to push more messages
func (pc *PartitionedConsumerImpl) Close() error {
	pc.Mu.Lock()
	defer pc.Mu.Unlock()
	if pc.IsClosed {
		return nil
	}

	var errMsg string
	for _, c := range pc.Consumers {
		if err := c.Close(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", c.Topic(), c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	close(pc.Closedc)
	pc.IsClosed = true
	return nil
}

// Seek reset the subscription associated with this consumer to a specific message id.
// The message id can either be a specific message or represent the first or last messages in the topic.
//
// Note: this operation can only be done on non-partitioned topics. For these, one can rather perform the
//       seek() on the individual partitions.
func (pc *PartitionedConsumerImpl) Seek(msgID MessageID) error {
	partition := uint32(msgID.Partition)
	if partition < 0 || partition >= pc.Partitions {
		return errors.New("invalid partition index")
	}
	return pc.Consumers[partition].Seek(msgID)
}

// RedeliverUnackedMessages redelivers all the unacknowledged messages.
func (pc *PartitionedConsumerImpl) RedeliverUnackedMessages() error {
	var errMsg string
	for _, c := range pc.Consumers {
		if err := c.RedeliverUnackedMessages(); err != nil {
			errMsg += fmt.Sprintf("topic %s, subscription %s: %s", c.Topic(), c.Subscription(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

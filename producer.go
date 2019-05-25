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
	"log"
	"sync"
	"time"

	"github.com/jiazhai/vipgoclient/frame"
	"github.com/jiazhai/vipgoclient/utils"
)

var (
	errProduceQueueFull = errors.New("producer queue is full")
)

// ProducerConfig contains options of creating a producer.
type ProducerConfig struct {
	// Specify the topic this producer will be publishing on.
	// This argument is required when constructing the producer.
	Topic string

	// Specify a name for the producer
	// If not assigned, the system will generate a globally unique name which can be access with
	// Producer.Name().
	// When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
	// across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
	// a topic.
	Name string

	// Attach a set of application defined properties to the producer
	// This properties will be visible in the topic stats
	Properties map[string]string

	// Set the send timeout (default: 30 seconds)
	// If a ProducerMessage is not acknowledged by the server before the sendTimeout expires, an error will be reported.
	// Setting the timeout to -1, will set the timeout to infinity, which can be useful when using Pulsar's ProducerMessage
	// deduplication feature.
	SendTimeout time.Duration

	// Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
	// When the queue is full, by default, all calls to Producer.send() and Producer.SendAsync() will fail
	// unless `BlockIfQueueFull` is set to true. Use BlockIfQueueFull(boolean) to change the blocking behavior.
	// (default: 1024)
	MaxPendingMessages int

	// Set the number of max pending messages across all the partitions
	// This setting will be used to lower the max pending messages for each partition
	// `MaxPendingMessages(int)`, if the total exceeds the configured value.
	// (default: 256)
	MaxPendingMessagesAcrossPartitions int

	// Set whether the `Producer.Send()` and `Producer.SendAsync()` operations should block when the outgoing
	// message queue is full. Default is `false`. If set to `false`, send operations will immediately fail with
	// `ProducerQueueIsFullError` when there is no space left in pending queue.
	BlockIfQueueFull bool

	// Set the ProducerMessage routing mode for the partitioned producer.
	// Default routing mode is round-robin routing.
	//
	// This logic is applied when the application is not setting a key ProducerMessage#setKey(String) on a
	// particular ProducerMessage.
	RoutingMode MessageRoutingMode

	// Change the `HashingScheme` used to chose the partition on where to publish a particular ProducerMessage.
	// Standard hashing functions available are:
	//
	//  - `JavaStringHash` : Java String.hashCode() equivalent
	//  - `Murmur3_32Hash` : Use Murmur3 hashing function.
	// 		https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash
	//  - `BoostHash`      : C++ based boost::hash
	//
	// Default is `JavaStringHash`.
	// HashScheme HashingScheme

	// Set the compression type for the producer.
	// By default, ProducerMessage payloads are not compressed. Supported compression types are:
	//  - LZ4
	//  - ZLIB
	//  - ZSTD
	//
	// Note: ZSTD is supported since Pulsar 2.3. Consumers will need to be at least at that
	// release in order to be able to receive messages compressed with ZSTD.
	Compression CompressionType

	// Set a custom ProducerMessage routing policy by passing an implementation of MessageRouter
	// The router is a function that given a particular ProducerMessage and the topic metadata, returns the
	// partition index where the ProducerMessage should be routed to
	CostomRouter MessageRouter
}

// SetDefault check and set default value for congfig.
func (cfg *ProducerConfig) SetDefault() error {
	if cfg.SendTimeout == 0 {
		cfg.SendTimeout = time.Second * 30
	}
	if cfg.MaxPendingMessages == 0 {
		cfg.MaxPendingMessages = 1024
	}
	if cfg.MaxPendingMessagesAcrossPartitions == 0 {
		cfg.MaxPendingMessagesAcrossPartitions = 256
	}
	return nil
}

// Producer the producer is used to publish messages on a topic
type Producer interface {
	// return the topic to which producer is publishing to
	Topic() string

	// return the producer name which could have been assigned by the system or specified by the client
	Name() string

	// Send a ProducerMessage
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// producer.Send(pulsar.ProducerMessage{ Payload: myPayload })
	Send(ProducerMessage) error

	// Send a ProducerMessage in asynchronous mode
	// The callback will report back the ProducerMessage being published and
	// the eventual error in publishing
	SendAsync(ProducerMessage, func(ProducerMessage, error))

	// Get the last sequence id that was published by this producer.
	// This represent either the automatically assigned or custom sequence id (set on the ProducerMessage) that
	// was published and acknowledged by the broker.
	// After recreating a producer with the same producer name, this will return the last ProducerMessage that was
	// published in the previous producer session, or -1 if there no ProducerMessage was ever published.
	// return the last sequence id published by this producer.
	LastSequenceID() uint64

	// Close the producer and releases resources allocated
	// No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
	// of errors, pending writes will not be retried.
	Close() error
}

type produceJob struct {
	Msg      ProducerMessage
	Callback func(ProducerMessage, error)
}

type produceResult struct {
	SeqID uint64
	Err   error
}

// ProducerImpl implication of no-partitioned-topic producer.
type ProducerImpl struct {
	Ctx              context.Context
	TopicName        string
	ID               uint64
	ProducerName     string
	SendTimeout      time.Duration
	PendingSize      int
	BlockIfQueueFull bool
	Conn             *ConnWrapper
	SequenceID       *MonotonicID
	Compression      CompressionType
	IsClosed         bool
	Closedc          chan struct{}

	mu          sync.Mutex
	jobQueue    chan *produceJob
	resultQueue chan *produceResult
	callbacks   map[uint64]func(error)
}

// Topic returns TopicName
func (p *ProducerImpl) Topic() string { return p.TopicName }

// Name returns ProducerName
func (p *ProducerImpl) Name() string { return p.ProducerName }

// Send sync send a ProducerMessage.
func (p *ProducerImpl) Send(msg ProducerMessage) error {
	sc := make(chan produceResult)
	p.SendAsync(msg, func(pm ProducerMessage, err error) {
		sc <- produceResult{Err: err}
		close(sc)
	})

	var err error
	select {
	case ret := <-sc:
		err = ret.Err
	}
	return err
}

// SendAsync send a ProducerMessage in asynchronous mode.
func (p *ProducerImpl) SendAsync(msg ProducerMessage, callback func(ProducerMessage, error)) {
	seqID := p.SequenceID.Next()
	// fill msg
	msg.ProducerID = p.ID
	msg.ProducerName = p.ProducerName
	msg.SequenceID = seqID
	msg.Compression = p.Compression

	job := &produceJob{
		Msg:      msg,
		Callback: callback,
	}
	if p.BlockIfQueueFull {
		p.jobQueue <- job
	} else {
		select {
		case p.jobQueue <- job:
		default:
			callback(msg, errProduceQueueFull)
		}
	}
}

// LastSequenceID returns the last sequence id that was published by this producer.
func (p *ProducerImpl) LastSequenceID() uint64 {
	return p.SequenceID.Last()
}

// Close close this producer.
func (p *ProducerImpl) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.IsClosed {
		return nil
	}
	p.IsClosed = true
	close(p.Closedc)
	ctx, cancel := context.WithTimeout(p.Ctx, p.SendTimeout)
	defer cancel()
	return p.Conn.UnregisterProducer(ctx, p.ID)
}

func (p *ProducerImpl) sendLoop() {
	for {
		select {
		case job, ok := <-p.jobQueue:
			if ok {
				p.callbacks[job.Msg.SequenceID] = func(err error) {
					job.Callback(job.Msg, err)
				}
				ctx, cancel := context.WithTimeout(p.Ctx, p.SendTimeout)
				err := p.Conn.Publish(ctx, job.Msg.Frame())
				cancel()
				if err != nil {
					p.resultQueue <- &produceResult{Err: err}
				}
			} else {
				// queue has been closed
				return
			}
		case <-p.Closedc:
			return
		}
	}
}

func (p *ProducerImpl) recvLoop() {
	for {
		select {
		case ret, ok := <-p.resultQueue:
			if ok {
				callback, ok := p.callbacks[ret.SeqID]
				if ok {
					delete(p.callbacks, ret.SeqID)
					// callback may block loop, may run in a new goroutine?
					callback(ret.Err)
				} else {
					log.Printf("callback not found producer: %s seqID: %d.", p.ProducerName, ret.SeqID)
				}
			} else {
				// queue has been closed
				return
			}
		case <-p.Closedc:
			return
		}
	}
}

// HandleConnected implementation of ProduceHandler
func (p *ProducerImpl) HandleConnected(f frame.Frame) error {
	p.ProducerName = f.BaseCmd.GetProducerSuccess().GetProducerName()
	p.callbacks = make(map[uint64]func(error), p.PendingSize)
	p.jobQueue = make(chan *produceJob, p.PendingSize)
	p.resultQueue = make(chan *produceResult, p.PendingSize)
	go utils.WithRecover(p.sendLoop, nil)
	go utils.WithRecover(p.recvLoop, nil)
	return nil
}

// HandleReceipt implementation of ProduceHandler
func (p *ProducerImpl) HandleReceipt(f frame.Frame) error {
	seqID := f.BaseCmd.GetSendReceipt().GetSequenceId()
	p.resultQueue <- &produceResult{SeqID: seqID}
	return nil
}

// HandleError implementation of ProduceHandler
func (p *ProducerImpl) HandleError(f frame.Frame) error {
	// Todo:
	// may be some retry logic here
	seqID := f.BaseCmd.GetSendError().GetSequenceId()
	errMsg := f.BaseCmd.GetSendError()
	err := fmt.Errorf("%s: %s", errMsg.GetError().String(), errMsg.GetMessage())
	p.resultQueue <- &produceResult{SeqID: seqID, Err: err}
	return nil
}

// HandleClose implementation of ProduceHandler
func (p *ProducerImpl) HandleClose(frame.Frame) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.IsClosed {
		return nil
	}
	p.IsClosed = true
	close(p.Closedc)
	return nil
}

// PartitionedProducerImpl implication of partitioned-topic producer.
type PartitionedProducerImpl struct {
	TopicName   string
	TopicObject *TopicObject
	Partitions  uint32
	Producers   []Producer
	Router      MessageRouter
	SequenceID  *MonotonicID
}

// Topic returns TopicName
func (p *PartitionedProducerImpl) Topic() string { return p.TopicName }

// Name returns ProducerName
func (p *PartitionedProducerImpl) Name() string { return p.Producers[0].Name() }

// Send sync send a ProducerMessage.
func (p *PartitionedProducerImpl) Send(msg ProducerMessage) error {
	sc := make(chan produceResult)
	p.SendAsync(msg, func(pm ProducerMessage, err error) {
		sc <- produceResult{Err: err}
		close(sc)
	})

	var err error
	select {
	case ret := <-sc:
		err = ret.Err
	}
	return err
}

// SendAsync send a ProducerMessage in asynchronous mode.
func (p *PartitionedProducerImpl) SendAsync(msg ProducerMessage, callback func(ProducerMessage, error)) {
	partition := p.Router.GetPartition(msg.Key)
	if partition < 0 || partition >= p.Partitions {
		callback(msg, errors.New("invalid partition index"))
	}
	p.Producers[partition].SendAsync(msg, callback)
}

// LastSequenceID returns the last sequence id that was published by this producer.
func (p *PartitionedProducerImpl) LastSequenceID() uint64 {
	return p.SequenceID.Last()
}

// Close close this producer.
func (p *PartitionedProducerImpl) Close() error {
	var errMsg string
	for _, p := range p.Producers {
		if err := p.Close(); err != nil {
			errMsg += fmt.Sprintf("topic %s, name %s: %s ", p.Topic(), p.Name(), err)
		}
	}
	if errMsg != "" {
		return errors.New(errMsg)
	}
	return nil
}

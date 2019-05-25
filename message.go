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
	"sync/atomic"
	"time"

	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/frame"
	"code.vipkid.com.cn/zhanghao1/go-pulsar-client/pkg/api"
	"github.com/golang/protobuf/proto"
)

type CompressionType int

const (
	NoCompression CompressionType = iota
	LZ4
	ZLib
	ZSTD
)

// MonotonicID handles unique id generation
type MonotonicID struct {
	ID uint64
}

// Last returns the last ID
func (r *MonotonicID) Last() uint64 {
	return atomic.LoadUint64(&r.ID) - 1
}

// Next returns the next ID
func (r *MonotonicID) Next() uint64 {
	return atomic.AddUint64(&r.ID, 1) - 1
}

func MarshalProperties(properties map[string]string) []*api.KeyValue {
	kvs := make([]*api.KeyValue, 0, len(properties))
	for k, v := range properties {
		kv := api.KeyValue{
			Key:   proto.String(k),
			Value: proto.String(v),
		}
		kvs = append(kvs, &kv)
	}
	return kvs
}

func UnmarshalProperties(kvs []*api.KeyValue) map[string]string {
	properties := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		properties[*kv.Key] = *kv.Value
	}
	return properties
}

type ProducerMessage struct {
	// ProducerID which should be auto filled by producer while sending
	ProducerID uint64
	// ProducerName which should be auto filled by producer while sending
	ProducerName string
	// SequenceID which should be auto filled by producer while sending
	SequenceID uint64
	// Payload for the message
	Payload []byte
	// Sets the key of the message for routing policy
	Key string
	// Attach application defined properties on the message
	Properties map[string]string
	// Set the event time for a given message
	EventTime   time.Time
	Compression CompressionType
}

func (msg *ProducerMessage) Frame() *frame.Frame {
	cmd := api.BaseCommand{
		Type: api.BaseCommand_SEND.Enum(),
		Send: &api.CommandSend{
			ProducerId:  proto.Uint64(msg.ProducerID),
			SequenceId:  proto.Uint64(msg.SequenceID),
			NumMessages: proto.Int32(1),
		},
	}

	properties := MarshalProperties(msg.Properties)
	var compression api.CompressionType
	switch msg.Compression {
	case NoCompression:
		compression = api.CompressionType_NONE
	case LZ4:
		compression = api.CompressionType_LZ4
	case ZLib:
		compression = api.CompressionType_ZLIB
	case ZSTD:
		compression = api.CompressionType_ZSTD
	default:
		compression = api.CompressionType_NONE
	}

	metadata := api.MessageMetadata{
		PartitionKey: proto.String(msg.Key),
		SequenceId:   proto.Uint64(msg.SequenceID),
		ProducerName: proto.String(msg.ProducerName),
		EventTime:    proto.Uint64(uint64(msg.EventTime.Unix()) * 1000),
		PublishTime:  proto.Uint64(uint64(time.Now().Unix()) * 1000),
		Properties:   properties,
		Compression:  compression.Enum(),
	}

	return &frame.Frame{
		BaseCmd:  &cmd,
		Metadata: &metadata,
		Payload:  msg.Payload,
	}
}

// Identifier for a particular message
type MessageID struct {
	LedgerId   uint64
	EntryId    uint64
	Partition  int32
	BatchIndex int32
}

type Message struct {
	ID          MessageID
	Topic       string
	Properties  map[string]string
	Payload     []byte
	PublishTime time.Time
	EventTime   time.Time
	Key         string
}

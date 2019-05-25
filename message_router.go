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
	"hash/crc32"
	"sync/atomic"
)

type HashingScheme int

const (
	JavaStringHash HashingScheme = iota // Java String.hashCode() equivalent
	Murmur3_32Hash                      // Use Murmur3 hashing function
	BoostHash                           // C++ based boost::hash
)

// MessageRoutingMode
type MessageRoutingMode int

const (
	// RoundRobinDistribution publish messages across all partitions in round-robin.
	RoundRobinDistribution MessageRoutingMode = iota

	// UseSinglePartition the producer will chose one single partition and publish all the messages into that partition
	UseSinglePartition

	// CustomPartition use custom message router implementation that will be called to determine the partition for a particular message.
	CustomPartition
)

// MessageRouterFunc
type MessageRouter interface {
	GetPartition(key string) uint32
}

func hashValue(value string) uint32 {
	return crc32.ChecksumIEEE([]byte(value))
}

type RoundRobinRouter struct {
	Partition uint32
	cur       uint32
}

func (router *RoundRobinRouter) GetPartition(key string) uint32 {
	if key != "" {
		return hashValue(key) % router.Partition
	}
	return (atomic.AddUint32(&router.cur, 1) - 1) % router.Partition
}

type SinglePartitionRouter struct {
	Partition       uint32
	SinglePartition uint32
}

func (router *SinglePartitionRouter) GetPartition(key string) uint32 {
	if key != "" {
		return hashValue(key) % router.Partition
	}
	return router.SinglePartition
}

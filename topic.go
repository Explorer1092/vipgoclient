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
	"bytes"
	"errors"
	"fmt"
	"strings"
)

const (
	// PartitionSuffix partitioned-topic name  will consist of the following parts:
	// TopicObject.String() + PartitionSuffix + index
	PartitionSuffix = "-partition-"
)

// TopicObject standardized topic
type TopicObject struct {
	FullName         string
	Domain           string
	Property         string
	Cluster          string
	NamespacePortion string
	LocalName        string
	IsV2Topic        bool
}

// ParseTopicName parse topic string to a standardized TopicObject.
// Returns an error if the topic was invalid.
func ParseTopicName(topic string) (*TopicObject, error) {
	fullName := topic
	if !strings.Contains(topic, "://") {
		pathTokens := strings.Split(topic, "/")
		if len(pathTokens) == 3 {
			fullName = "persistent://" + pathTokens[0] + "/" + pathTokens[1] + "/" + pathTokens[2]
		} else if len(pathTokens) == 1 {
			fullName = "persistent://public/default/" + pathTokens[0]
		} else {
			return nil, errors.New("invalid topic")
		}
	}

	originFullName := fullName
	fullName = strings.Replace(fullName, "://", "/", -1)
	pathTokens := strings.Split(fullName, "/")
	if len(pathTokens) < 4 {
		return nil, errors.New("invalid topic t")
	}

	var domain, property, cluster, namespacePortion, localName string
	var isV2Topic bool
	var numSlashIndexes uint32
	domain = pathTokens[0]
	if len(pathTokens) == 4 {
		// New topic t without cluster t
		property = pathTokens[1]
		cluster = ""
		namespacePortion = pathTokens[2]
		localName = pathTokens[3]
		isV2Topic = true
	} else {
		// Legacy topic t that includes cluster t
		property = pathTokens[1]
		cluster = pathTokens[2]
		namespacePortion = pathTokens[3]
		localName = pathTokens[4]
		numSlashIndexes = 4
		isV2Topic = false
		slashIndex := strings.IndexFunc(fullName, func(c rune) bool {
			if numSlashIndexes <= 0 {
				return true
			}
			if c == '/' {
				numSlashIndexes--
			}
			return false
		})
		localName = fullName[slashIndex:]
	}

	return &TopicObject{
		FullName:         originFullName,
		Domain:           domain,
		Property:         property,
		Cluster:          cluster,
		NamespacePortion: namespacePortion,
		LocalName:        localName,
		IsV2Topic:        isV2Topic,
	}, nil
}

func (t *TopicObject) String() string {
	var buf bytes.Buffer
	seperator := "/"
	buf.WriteString(t.Domain)
	buf.WriteString("://")
	buf.WriteString(t.Property)
	buf.WriteString(seperator)
	if !t.IsV2Topic || len(t.Cluster) != 0 {
		buf.WriteString(t.Cluster)
		buf.WriteString(seperator)
	}
	buf.WriteString(t.NamespacePortion)
	buf.WriteString(seperator)
	buf.WriteString(t.LocalName)
	return buf.String()
}

// GetPartitionName returns a partitioned-topic string by index.
func (t *TopicObject) GetPartitionName(partition uint32) string {
	return t.String() + PartitionSuffix + fmt.Sprint(partition)
}

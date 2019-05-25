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
	"testing"
)

func TestTopicObjectParse(t *testing.T) {
	input := "persistent://public/default/testing_topic"
	excepted := &TopicObject{
		FullName:         "persistent://public/default/testing_topic",
		Domain:           "persistent",
		Property:         "public",
		Cluster:          "",
		NamespacePortion: "default",
		LocalName:        "testing_topic",
		IsV2Topic:        true,
	}
	output, err := ParseTopicName(input)
	if err != nil {
		t.Fatal(err)
	}
	if *output != *excepted {
		t.Fatal(*excepted, *output)
	}
}

func TestGetPartitionName(t *testing.T) {
	excepted := "persistent://public/default/testing_topic-partition-0"
	object := &TopicObject{
		FullName:         "persistent://public/default/testing_topic",
		Domain:           "persistent",
		Property:         "public",
		Cluster:          "",
		NamespacePortion: "default",
		LocalName:        "testing_topic",
		IsV2Topic:        true,
	}
	output := object.GetPartitionName(0)
	if output != excepted {
		t.Fatal(excepted, output)
	}
}

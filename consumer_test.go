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

// import (
// 	"context"
// 	"fmt"
// 	"github.com/stretchr/testify/assert"
// 	"io/ioutil"
// 	"net/http"
// 	"strings"
// 	"testing"
// 	"time"
// )

// func TestConsumerConnectError(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://invalid-hostname:6650",
// 	})

// 	assert.Nil(t, err)

// 	defer client.Close()

// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		Topic:            "my-topic",
// 		SubscriptionName: "my-subscription",
// 	})

// 	// Expect error in creating consumer
// 	assert.Nil(t, consumer)
// 	assert.NotNil(t, err)

// 	assert.Equal(t, err.(*Error).Result(), ConnectError);
// }

// func TestConsumer(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	assert.Nil(t, err)
// 	defer client.Close()

// 	producer, err := client.CreateProducer(ProducerOptions{
// 		Topic: "my-topic",
// 	})

// 	assert.Nil(t, err)
// 	defer producer.Close()

// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		Topic:             "my-topic",
// 		SubscriptionName:  "my-sub",
// 		AckTimeout:        1 * time.Minute,
// 		Name:              "my-consumer-name",
// 		ReceiverQueueSize: 100,
// 		MaxTotalReceiverQueueSizeAcrossPartitions: 10000,
// 		Type: Shared,
// 	})

// 	assert.Nil(t, err)
// 	defer consumer.Close()

// 	assert.Equal(t, consumer.Topic(), "persistent://public/default/my-topic")
// 	assert.Equal(t, consumer.Subscription(), "my-sub")

// 	ctx := context.Background()

// 	for i := 0; i < 10; i++ {
// 		if err := producer.Send(ctx, ProducerMessage{
// 			Payload: []byte(fmt.Sprintf("hello-%d", i)),
// 		}); err != nil {
// 			t.Fatal(err)
// 		}

// 		msg, err := consumer.Receive(ctx)
// 		assert.Nil(t, err)
// 		assert.NotNil(t, msg)

// 		assert.Equal(t, string(msg.Payload()), fmt.Sprintf("hello-%d", i))
// 		assert.Equal(t, string(msg.Topic()), "persistent://public/default/my-topic")

// 		consumer.Ack(msg)
// 	}

// 	err = consumer.Seek(EarliestMessage)
// 	assert.Nil(t, err)

// 	consumer.Unsubscribe()
// }

// func TestConsumerCompaction(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	assert.Nil(t, err)
// 	defer client.Close()

// 	topic := fmt.Sprintf("my-compaction-topic-%d", time.Now().Unix())

// 	producer, err := client.CreateProducer(ProducerOptions{
// 		Topic: topic,
// 	})

// 	assert.Nil(t, err)
// 	defer producer.Close()

// 	// Pre-create both subscriptions to retain published messages
// 	consumer1, err := client.Subscribe(ConsumerConfig{
// 		Topic:            topic,
// 		SubscriptionName: "my-sub-1",
// 	})

// 	assert.Nil(t, err)
// 	consumer1.Close()

// 	consumer2, err := client.Subscribe(ConsumerConfig{
// 		Topic:            topic,
// 		SubscriptionName: "my-sub-2",
// 		ReadCompacted:    true,
// 	})

// 	assert.Nil(t, err)
// 	consumer2.Close()

// 	ctx := context.Background()

// 	for i := 0; i < 10; i++ {
// 		if err := producer.Send(ctx, ProducerMessage{
// 			Payload: []byte(fmt.Sprintf("hello-%d", i)),
// 			Key:     "Same-Key",
// 		}); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	// Compact topic and wait for operation to complete
// 	url := fmt.Sprintf("http://localhost:8080/admin/v2/persistent/public/default/%s/compaction", topic)
// 	makeHttpPutCall(t, url)
// 	for {
// 		res := makeHttpGetCall(t, url)
// 		if strings.Contains(res, "RUNNING") {
// 			fmt.Println("Compaction still running")
// 			time.Sleep(100 * time.Millisecond)
// 			continue
// 		} else {
// 			assert.Equal(t, strings.Contains(res, "SUCCESS"), true)
// 			fmt.Println("Compaction is done")
// 			break
// 		}
// 	}

// 	// Restart the consumers

// 	consumer1, err = client.Subscribe(ConsumerConfig{
// 		Topic:            topic,
// 		SubscriptionName: "my-sub-1",
// 	})

// 	assert.Nil(t, err)
// 	defer consumer1.Close()

// 	consumer2, err = client.Subscribe(ConsumerConfig{
// 		Topic:            topic,
// 		SubscriptionName: "my-sub-2",
// 		ReadCompacted:    true,
// 	})

// 	assert.Nil(t, err)
// 	defer consumer2.Close()

// 	// Consumer-1 will receive all messages
// 	for i := 0; i < 10; i++ {
// 		msg, err := consumer1.Receive(context.Background())
// 		assert.Nil(t, err)
// 		assert.NotNil(t, msg)

// 		assert.Equal(t, string(msg.Payload()), fmt.Sprintf("hello-%d", i))
// 	}

// 	// Consumer-2 will only receive the last message
// 	msg, err := consumer2.Receive(context.Background())
// 	assert.Nil(t, err)
// 	assert.NotNil(t, msg)
// 	assert.Equal(t, string(msg.Payload()), fmt.Sprintf("hello-9"))

// 	// No more messages on consumer-2
// 	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
// 	defer cancel()

// 	msg, err = consumer2.Receive(ctx)
// 	assert.Nil(t, msg)
// 	assert.NotNil(t, err)
// }

// func TestConsumerWithInvalidConf(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	if err != nil {
// 		t.Fatal(err)
// 		return
// 	}

// 	defer client.Close()

// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		Topic: "my-topic",
// 	})

// 	// Expect error in creating cosnumer
// 	assert.Nil(t, consumer)
// 	assert.NotNil(t, err)

// 	assert.Equal(t, err.(*Error).Result(), InvalidConfiguration)

// 	consumer, err = client.Subscribe(ConsumerConfig{
// 		SubscriptionName: "my-subscription",
// 	})

// 	// Expect error in creating consumer
// 	assert.Nil(t, consumer)
// 	assert.NotNil(t, err)

// 	assert.Equal(t, err.(*Error).Result(), InvalidConfiguration)
// }

// func makeHttpPutCall(t *testing.T, url string) string {
// 	return makeHttpCall(t, http.MethodPut, url)
// }

// func makeHttpGetCall(t *testing.T, url string) string {
// 	return makeHttpCall(t, http.MethodGet, url)
// }

// func makeHttpCall(t *testing.T, method string, url string) string {
// 	client := http.Client{}

// 	req, err := http.NewRequest(method, url, nil)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	req.Header.Set("Content-Type", "application/json")
// 	req.Header.Set("Accept", "application/json")

// 	res, err := client.Do(req)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	body, err := ioutil.ReadAll(res.Body)
// 	if err != nil {
// 		t.Fatal(err)
// 	}

// 	return string(body)
// }

// func TestConsumerMultiTopics(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	assert.Nil(t, err)
// 	defer client.Close()

// 	producer1, err := client.CreateProducer(ProducerOptions{
// 		Topic: "multi-topic-1",
// 	})

// 	assert.Nil(t, err)

// 	producer2, err := client.CreateProducer(ProducerOptions{
// 		Topic: "multi-topic-2",
// 	})

// 	assert.Nil(t, err)
// 	defer producer1.Close()
// 	defer producer2.Close()

// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		Topics:           []string{"multi-topic-1", "multi-topic-2"},
// 		SubscriptionName: "my-sub",
// 	})

// 	assert.Nil(t, err)
// 	defer consumer.Close()

// 	assert.Equal(t, consumer.Subscription(), "my-sub")

// 	ctx := context.Background()

// 	for i := 0; i < 10; i++ {
// 		if err := producer1.Send(ctx, ProducerMessage{
// 			Payload:    []byte(fmt.Sprintf("hello-%d", i)),
// 			SequenceID: 3,
// 		}); err != nil {
// 			t.Fatal(err)
// 		}
// 		assert.Equal(t, producer1.LastSequenceID(), int64(3))

// 		if err := producer2.Send(ctx, ProducerMessage{
// 			Payload:    []byte(fmt.Sprintf("hello-%d", i)),
// 			SequenceID: 0,
// 		}); err != nil {
// 			t.Fatal(err)
// 		}
// 		assert.Equal(t, producer2.LastSequenceID(), int64(i))
// 	}

// 	for i := 0; i < 20; i++ {
// 		msg, err := consumer.Receive(ctx)
// 		assert.Nil(t, err)
// 		assert.NotNil(t, msg)

// 		consumer.Ack(msg)
// 	}

// 	consumer.Unsubscribe()
// }

// func TestConsumerRegex(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	assert.Nil(t, err)
// 	defer client.Close()

// 	producer1, err := client.CreateProducer(ProducerOptions{
// 		Topic: "topic-1",
// 	})

// 	assert.Nil(t, err)

// 	producer2, err := client.CreateProducer(ProducerOptions{
// 		Topic: "topic-2",
// 	})

// 	assert.Nil(t, err)
// 	defer producer1.Close()
// 	defer producer2.Close()

// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		TopicsPattern:    "topic-\\d+",
// 		SubscriptionName: "my-sub",
// 	})

// 	assert.Nil(t, err)
// 	defer consumer.Close()

// 	assert.Equal(t, consumer.Subscription(), "my-sub")

// 	ctx := context.Background()

// 	for i := 0; i < 10; i++ {
// 		if err := producer1.Send(ctx, ProducerMessage{
// 			Payload: []byte(fmt.Sprintf("hello-%d", i)),
// 		}); err != nil {
// 			t.Fatal(err)
// 		}

// 		if err := producer2.Send(ctx, ProducerMessage{
// 			Payload: []byte(fmt.Sprintf("hello-%d", i)),
// 		}); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	for i := 0; i < 20; i++ {
// 		msg, err := consumer.Receive(ctx)
// 		assert.Nil(t, err)
// 		assert.NotNil(t, msg)

// 		consumer.Ack(msg)
// 	}

// 	consumer.Unsubscribe()
// }

// func TestConsumer_Seek(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	assert.Nil(t, err)
// 	defer client.Close()

// 	topicName := "persistent://public/default/testSeek"
// 	subName := "sub-testSeek"

// 	producer, err := client.CreateProducer(ProducerOptions{
// 		Topic: topicName,
// 	})
// 	assert.Nil(t, err)
// 	assert.Equal(t, producer.Topic(), topicName)
// 	defer producer.Close()

// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		Topic:            topicName,
// 		SubscriptionName: subName,
// 	})
// 	assert.Nil(t, err)
// 	assert.Equal(t, consumer.Topic(), topicName)
// 	assert.Equal(t, consumer.Subscription(), subName)
// 	defer consumer.Close()

// 	ctx := context.Background()

// 	// Send 10 messages synchronously
// 	t.Log("Publishing 10 messages synchronously")
// 	for msgNum := 0; msgNum < 10; msgNum++ {
// 		if err := producer.Send(ctx, ProducerMessage{
// 			Payload: []byte(fmt.Sprintf("msg-content-%d", msgNum)),
// 		}); err != nil {
// 			t.Fatal(err)
// 		}
// 	}

// 	t.Log("Trying to receive 10 messages")
// 	for msgNum := 0; msgNum < 10; msgNum++ {
// 		_, err := consumer.Receive(ctx)
// 		assert.Nil(t, err)
// 	}

// 	// seek to earliest, expected receive first message.
// 	err = consumer.Seek(EarliestMessage)
// 	assert.Nil(t, err)

// 	// Sleeping for 500ms to wait for consumer re-connect
// 	time.Sleep(500 * time.Millisecond)

// 	msg, err := consumer.Receive(ctx)
// 	assert.Nil(t, err)
// 	t.Logf("again received message:%+v", msg.ID())
// 	assert.Equal(t, "msg-content-0", string(msg.Payload()))

// 	consumer.Unsubscribe()
// }

// func TestConsumer_SubscriptionInitPos(t *testing.T) {
// 	client, err := NewClient(ClientOptions{
// 		URL: "pulsar://localhost:6650",
// 	})

// 	assert.Nil(t, err)
// 	defer client.Close()

// 	topicName := "persistent://public/default/testSeek"
// 	subName := "test-subscription-initial-earliest-position"

// 	// create producer
// 	producer, err := client.CreateProducer(ProducerOptions{
// 		Topic: topicName,
// 	})
// 	assert.Nil(t, err)
// 	defer producer.Close()

// 	//sent message
// 	ctx := context.Background()

// 	err = producer.Send(ctx, ProducerMessage{
// 		Payload: []byte("msg-1-content-1"),
// 	})
// 	assert.Nil(t, err)

// 	err = producer.Send(ctx, ProducerMessage{
// 		Payload: []byte("msg-1-content-2"),
// 	})
// 	assert.Nil(t, err)

// 	// create consumer
// 	consumer, err := client.Subscribe(ConsumerConfig{
// 		Topic:               topicName,
// 		SubscriptionName:    subName,
// 		SubscriptionInitPos: Earliest,
// 	})
// 	assert.Nil(t, err)
// 	defer consumer.Close()

// 	msg, err := consumer.Receive(ctx)
// 	assert.Nil(t, err)

// 	assert.Equal(t, "msg-1-content-1", string(msg.Payload()))
// }

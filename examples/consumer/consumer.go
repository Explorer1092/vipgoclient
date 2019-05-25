package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pulsar "code.vipkid.com.cn/zhanghao1/go-pulsar-client"
)

func main() {
	ctx := context.Background()
	client, err := pulsar.NewClient(ctx, pulsar.ClientConfig{
		URL: "pulsar://127.0.0.1:6650",
		// URL:              "pulsar://192.168.33.74:6650",
		OperationTimeout: time.Second * 5,
	})

	if err != nil || client == nil {
		log.Fatalf("Could not instantiate Pulsar client: %v", err)
	}

	cfg := pulsar.ConsumerConfig{
		Topic:             "multi_partition_topic",
		SubscriptionName:  "consumer_3",
		Type:              pulsar.Shared,
		ReceiverQueueSize: 1024,
	}

	headConsumer, err := client.Subscribe(cfg)

	if err != nil {
		log.Fatalf("Could not establish subscription: %v", err)
	}
	fmt.Println("create head consumer success")

	defer headConsumer.Close()

	dataOpts := pulsar.ConsumerConfig{
		Topic:             "single_partition_topic",
		SubscriptionName:  "consumer_4",
		Type:              pulsar.Exclusive,
		ReceiverQueueSize: 1024,
	}

	dataConsumer, err := client.Subscribe(dataOpts)

	if err != nil {
		log.Fatalf("Could not establish subscription: %v", err)
	}

	fmt.Println("create data consumer success")
	defer dataConsumer.Close()

	go func() {
		for {
			msg, err := headConsumer.Receive(ctx)
			if err != nil {
				fmt.Println("head", err)
				return
			}
			fmt.Printf("Message topic: %s, id: %v, key: %s, value: %s.\n ",
				msg.Topic, msg.ID, msg.Key, string(msg.Payload))
			headConsumer.Ack(msg)
		}
	}()

	for {
		msg, err := dataConsumer.Receive(ctx)
		if err != nil {
			fmt.Println("data", err)
			return
		}
		fmt.Printf("Message topic: %s, id: %v, key: %s, value: %s.\n ",
			msg.Topic, msg.ID, msg.Key, string(msg.Payload))
		dataConsumer.Ack(msg)
	}
}

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
	cfg := pulsar.ClientConfig{
		URL:              "pulsar://192.168.33.74:6650",
		OperationTimeout: time.Second * 15,
	}
	client, err := pulsar.NewClient(ctx, cfg)
	if err != nil {
		log.Fatal(err)
	}

	produceCfg := pulsar.ProducerConfig{
		Topic:       "multi_partition_topic",
		SendTimeout: time.Second * 15,
		// RoutingMode: pulsar.RoundRobinDistribution,
	}

	begin := time.Now()
	producer, err := client.CreateProducer(produceCfg)
	if err != nil || producer == nil {
		log.Fatal(err)
	}
	fmt.Println(time.Now().Sub(begin))
	fmt.Println(producer.Name())

	singleCfg := pulsar.ProducerConfig{
		Topic:       "single_partition_topic",
		SendTimeout: time.Second * 15,
		// RoutingMode: pulsar.RoundRobinDistribution,
	}

	begin = time.Now()
	singleP, err := client.CreateProducer(singleCfg)
	if err != nil || producer == nil {
		log.Fatal(err)
	}
	fmt.Println(time.Now().Sub(begin))
	fmt.Println(singleP.Name())

	stop := time.After(time.Second * 10)
	multi := time.NewTicker(time.Millisecond * 200)
	defer multi.Stop()
	single := time.NewTicker(time.Millisecond * 200)
	defer single.Stop()
	counter := 0
Loop:
	for {
		select {
		case <-stop:
			fmt.Println("Finish")
			break Loop
		case <-multi.C:
			msg := pulsar.ProducerMessage{
				Key:     fmt.Sprintf("msg key: %d", counter),
				Payload: []byte(fmt.Sprintf("msg payload: %d", counter)),
				Properties: map[string]string{
					"a": "b",
					"c": "d",
				},
				EventTime: time.Now(),
			}
			counter++
			producer.SendAsync(msg, callback)
		case <-single.C:
			msg := pulsar.ProducerMessage{
				Key:     fmt.Sprintf("msg key: %d", counter),
				Payload: []byte(fmt.Sprintf("msg payload: %d", counter)),
				Properties: map[string]string{
					"e": "f",
					"g": "h",
				},
				EventTime: time.Now(),
			}
			counter++
			singleP.SendAsync(msg, callback)
		}
	}
	err = producer.Close()
	if err != nil {
		fmt.Println(err)
	}
	err = singleP.Close()
	if err != nil {
		fmt.Println(err)
	}
}

func callback(msg pulsar.ProducerMessage, err error) {
	if err != nil {
		fmt.Println(msg, err)
	} else {
		// fmt.Println("OK")
	}
}

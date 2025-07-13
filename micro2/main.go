package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	topic := "HVSE"

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"group.id":          "foo",
		"auto.offset.reset": "smallest",
	})

	if err != nil {
		//fmt.Printf("failed to make consumer: &s\n",err)
		log.Fatal(err)
	}

	//subscribing topics
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		log.Fatal(err)
	}

	for {
		ev := consumer.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			fmt.Printf("consumed message from the queue: %s\n", string(e.Value))
		case *kafka.Error:
			fmt.Printf("error consuming messages from the queue: %v\n", e)
		}
	}

}

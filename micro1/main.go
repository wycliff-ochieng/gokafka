package main

import (
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type OrderProducer struct {
	Producer     *kafka.Producer
	Topic        string
	DeliveryChan chan kafka.Event
}

func NewOrderProducer(p *kafka.Producer, topic string) *OrderProducer {
	return &OrderProducer{
		Producer:     p,
		Topic:        topic,
		DeliveryChan: make(chan kafka.Event, 1000),
	}
}

func (op *OrderProducer) PlaceOrder(ordertype string, size int) error {
	var (
		format  = fmt.Sprintf("%s-%d", ordertype, size)
		payload = []byte(format)
	)

	err := op.Producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &op.Topic,
			Partition: kafka.PartitionAny,
		},
		Value: payload,
	},
		op.DeliveryChan,
	)
	if err != nil {
		log.Fatal(err)
	}
	<-op.DeliveryChan

	fmt.Printf("placed an order on the queue %s\n", format)

	return nil

}

func main() {
	//pull out the producer
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"client.id":         "Foo",
		"acks":              "all",
	})

	if err != nil {
		fmt.Printf("failed to create producer:%s\n", err)
	}

	//make connsumer
	/*
		go func() {
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
			err = consumer.Subscribe("HVSE", nil)
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
		}()*/

	op := NewOrderProducer(p, "HVSE")
	for i := 0; i < 1000; i++ {
		if err := op.PlaceOrder("market", i+1); err != nil {
			log.Fatal(err)
		}
		time.Sleep(time.Second * 3)
	}
}

package queue

import (
	"context"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"time"
)

var admin 	 *kafka.AdminClient
var producer *kafka.Producer
var delivery chan kafka.Event

func PrepareProducer()  {

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": "localhost:9093"})
	if err != nil {
		fmt.Printf("%v", err)
		panic("CANNOT CREATE PRODUCER")
	}
	producer = p
	delivery = make(chan kafka.Event)

}

func CreateTopic()  {

	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": "localhost:9093,localhost:9094,localhost:9095"})
	admin = a
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}

	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             "test-topic-1",
			NumPartitions:     1,
			ReplicationFactor: 3}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

}

func Publish(msg string)  {
	// Set topic to publish into
	t := "test-topic-1"
	err := producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &t, Partition: 0},
		Value:          []byte(msg),
		Headers:        []kafka.Header{{Key: "testHeader", Value: []byte("bytes test")}},
	}, delivery)

	if err != nil {
		fmt.Printf("Error: %v", err)
	}

	e := <-delivery
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

}
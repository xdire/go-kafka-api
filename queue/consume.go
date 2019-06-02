package queue

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
)

var consumer *kafka.Consumer
var signals chan os.Signal

type StopSignal struct {
	name string
}

func NewStopSignal(s string) *StopSignal {
	sigObj := StopSignal{name: s}
	return &sigObj
}

func (this *StopSignal) String() string {
	return this.name
}

func (this *StopSignal) Signal() {
	return
}

/*
	PREPARE CONSUMER CLIENT
	---------------------------------------
 */
func PrepareConsumer () {

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9093,localhost:9094,localhost:9095",
		"broker.address.family": "v4",
		"group.id":              "tester",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	consumer = c

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	// Connect system signals to stop runner via channel
	signals = make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
}

/*
    RUN CONSUMER
	---------------------------------------
 */
func Consume ()  {
	// Define topic name
	t := []string{"test-topic-1"}
	// Subscribe for all presented topics
	err := consumer.SubscribeTopics(t, nil)
	// Exit if cannot subscribe
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start consumer: %s\n", err)
		os.Exit(1)
		return
	}
	// Start consumer loop otherwise
	startConsumer()
}

/*
    CONSUMPTION LOOP
	---------------------------------------
 */
func startConsumer()  {
	// Define flag do-while
	runner := true
	// Start loop
	for runner == true {
		select {
		case s := <-signals:
			fmt.Printf("Interrupted by system %v:\n", s)
			runner = false
		default:
			ev := consumer.Poll(1000)
			if ev == nil {
				// fmt.Printf("No event dispatched")
				continue
			}
			propagateEvent(ev)
		}
	}
	fmt.Println("Consumer ended work")
}

/*
    CONSUMPTION LOOP
	---------------------------------------
*/
func propagateEvent(ev kafka.Event)  {
	switch e := ev.(type) {
	case *kafka.Message:
		// Select message and print it out
		fmt.Printf("%% Message on %s:\n%s\n", e.TopicPartition, string(e.Value))
		if e.Headers != nil {
			fmt.Printf("%% Headers: %v\n", e.Headers)
		}
	case kafka.Error:
		// Check on error and terminate if all brokers went away
		fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
		if e.Code() == kafka.ErrAllBrokersDown {
			signals<-NewStopSignal("Stopped because of all Brokers got down")
		}
	default:
		fmt.Printf("Kafka produced type of event which weren't recognized %v\n", e)
	}
}
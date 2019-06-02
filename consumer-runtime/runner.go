package main

import "github.com/xdire/go-kafka-api/queue"

func main()  {
	// Prepare the consumer for connection
	queue.PrepareConsumer()
	// Start the consumer
	queue.Consume()
}

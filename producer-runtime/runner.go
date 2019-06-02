package main

import "github.com/xdire/go-kafka-api/queue"

func main()  {
	// Start the chain
	queue.PrepareProducer()
	queue.CreateTopic()
	// Send messages to queue
	queue.Publish("Sex in the city")
	queue.Publish("Drives us good into")
	queue.Publish("Charmin attires")
}

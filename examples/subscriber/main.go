package main

import (
	"fmt"
	"log"

	"github.com/marrick66/gizmo/pubsub/amqp"
)

func main() {
	sub, err := amqp.NewSubscriber(
		"amqp://test:test@localhost:5672",
		"testExchange",
		"testTopic",
		nil) //Use default options

	if err != nil {
		log.Fatal(err)
	}

	//Receive string messages until the channel is closed:
	for msg := range sub.Start() {
		fmt.Println(string(msg.Message()))
	}

	if err = sub.Err(); err != nil {
		log.Fatal(err)
	}
}

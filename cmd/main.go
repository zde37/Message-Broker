package main

import (
	"fmt"
	"log"
	"time"

	"github.com/zde37/Message-Broker/internal"
)

func main() {
	broker := internal.NewBroker(":7080")

	go func() {
		if err := broker.StartGRPCServer(); err != nil {
			log.Fatalf("failed to start broker: %v", err)
		}
	}()

	go func() {
		if err := publisher1(); err != nil {
			log.Fatalf("failed to start temperature publisher: %v", err)
		}
	}()

	go func() {
		if err := publisher2(); err != nil {
			log.Fatalf("failed to start pressure publisher: %v", err)
		}
	}()

	go func() {
		if err := publisher3(); err != nil {
			log.Fatalf("failed to start speed publisher: %v", err)
		}
	}()

	go func() {
		if err := consumer(); err != nil {
			log.Fatalf("failed to start consumer: %v", err)
		}
	}()

	log.Fatal(broker.AwaitShutdown())
}

func publisher1() error {
	publisher, err := internal.NewPublisher("localhost:7080")
	if err != nil {
		return fmt.Errorf("failed to create temperature publisher: %v", err)
	}

	// defer publisher1.Close()
	i := 0
	for range time.Tick(1 * time.Second) {
		msg := fmt.Sprintf("temperature is %d", i)
		publisher.Publish("topic/temp", []byte(msg))
		i++ 
	}

	return nil
}

func publisher2() error {
	publisher, err := internal.NewPublisher("localhost:7080")
	if err != nil {
		return fmt.Errorf("failed to create pressure publisher: %v", err)
	}

	// defer publisher2.Close()

	i := 0
	for range time.Tick(3 * time.Second) {
		msg := fmt.Sprintf("pressure is %d", i)
		publisher.Publish("topic/pressure", []byte(msg))
		i++ 
	}

	return nil
}

func publisher3() error {
	publisher, err := internal.NewPublisher("localhost:7080")
	if err != nil {
		return fmt.Errorf("failed to create speed publisher: %v", err)
	}
	// defer publisher3.Close()

	i := 0
	for range time.Tick(2 * time.Second) {
		msg := fmt.Sprintf("speed is %d", i)
		publisher.Publish("topic/speed", []byte(msg))
		i++ 
	}

	return nil
}

func consumer() error {
	consumer, err := internal.NewConsumer("localhost:7080")
	if err != nil {
		return fmt.Errorf("failed to create new consumer: %v", err)
	}
	
	// defer consumer.Close()

	if err = consumer.Subscribe("topic/temp"); err != nil {
		return fmt.Errorf("failed to subscribe to topic/temp: %v", err)
	}

	if err = consumer.Subscribe("topic/pressure"); err != nil {
		return fmt.Errorf("failed to subscribe to topic/pressure: %v", err)
	}

	if err = consumer.Subscribe("topic/speed"); err != nil {
		return fmt.Errorf("failed to subscribe to topic/speed: %v", err)
	}

	for msg := range consumer.Messages {
		log.Println(msg)
	}

	return nil
}

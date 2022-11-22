package main

import (
	"fmt"
	"sync"
)

type Broker struct {
	m      *sync.Mutex
	Topics map[string][]Consumer
}

func (b *Broker) Publish(topic string, msg string) {
	consumers := b.Topics[topic]
	for _, consumer := range consumers {
		cs := consumer
		go func() {
			cs.msg <- msg
		}()
	}
}
func (b *Broker) AddConsumer(topic string, consumer string) *Consumer {
	b.m.Lock()

	c := &Consumer{
		ConsumerName: consumer,
		msg:          make(chan string),
		Offset:       0,
		Lock:         &sync.Mutex{},
		Topic:        topic,
	}
	b.Topics[topic] = append(b.Topics[topic], *c)
	b.m.Unlock()
	return c
}

type Consumer struct {
	ConsumerName string
	msg          chan string
	Offset       int
	Lock         *sync.Mutex
	Topic        string
}

func NewProducer(topic string) *Broker {

	tp := map[string][]Consumer{}
	tp[topic] = make([]Consumer, 0)
	return &Broker{
		m:      &sync.Mutex{},
		Topics: tp,
	}
}

func (s *Consumer) Consume() {
	for {
		s.Lock.Lock()
		msg, ok := <-s.msg
		if !ok {
			break
		}

		fmt.Printf("Consumer %s, received: %s from topic: %s\n", s.ConsumerName, msg, s.Topic)
		s.Lock.Unlock()
	}

}
func main() {
	b := NewProducer("topic1")
	consumer1 := b.AddConsumer("topic1", "consumer1")
	consumer2 := b.AddConsumer("topic2", "consumer2")
	consumer3 := b.AddConsumer("topic2", "consumer3")
	b.Publish("topic1", "msg1")
	b.Publish("topic1", "msg2")
	b.Publish("topic1", "msg3")

	b.Publish("topic2", "msg4")
	b.Publish("topic2", "msg5")
	b.Publish("topic2", "msg6")

	// no one is consuming
	b.Publish("topic3", "msg4")
	b.Publish("topic3", "msg5")

	go consumer1.Consume()
	go consumer2.Consume()
	go consumer3.Consume()

	fmt.Scanln()

}

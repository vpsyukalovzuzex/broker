package natsjetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

type Broker struct {
	Url string

	conn *nats.Conn

	js nats.JetStreamContext
}

func New(url string) *Broker {
	return &Broker{
		Url: url,
	}
}

func (b *Broker) Start() error {
	var err error
	b.conn, err = nats.Connect(b.Url)
	if err != nil {
		return fmt.Errorf("start, nats connect: %v", err)
	}

	b.js, err = b.conn.JetStream()
	if err != nil {
		return fmt.Errorf("start, new jet stream: %v", err)
	}

	return nil
}

func (b *Broker) Close() error {
	b.conn.Close()

	return nil
}

func (b *Broker) Produce(
	ctx context.Context,
	channel string,
	queue string,
	object any,
) error {
	log.Println("produce:", fmt.Sprintf("%s.%s", channel, queue))

	if _, err := b.addStreamIfNeeded(channel, queue); err != nil {
		return fmt.Errorf("produce, add stream if needed: %v", err)
	}

	body, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("produce, marshal: %v", err)
	}

	if _, err := b.js.Publish(fmt.Sprintf("%s.%s", channel, queue), body); err != nil {
		return fmt.Errorf("produce, publish: %v", err)
	}

	return nil
}

func (b *Broker) Consume(
	ctx context.Context,
	channel string,
	queue string,
	fn func(context.Context, []byte),
) error {
	log.Println("consume:", fmt.Sprintf("%s.%s", channel, queue))

	if _, err := b.addStreamIfNeeded(channel, queue); err != nil {
		return fmt.Errorf("consume, add stream if needed: %v", err)
	}

	if _, err := b.js.Subscribe(
		fmt.Sprintf("%s.%s", channel, queue),
		func(msg *nats.Msg) {
			msg.Ack()
			fn(ctx, msg.Data)
		},
		nats.DeliverNew(),
	); err != nil {
		return fmt.Errorf("consume, subscribe: %v", err)
	}

	return nil
}

func (b *Broker) addStreamIfNeeded(channel string, queue string) (*nats.StreamInfo, error) {
	stream, err := b.js.StreamInfo(channel)
	if err != nil {
		log.Println(fmt.Errorf("add stream if needed, stream info: %v", err))
	}

	if stream == nil {
		var err error
		stream, err = b.js.AddStream(&nats.StreamConfig{
			Name:     channel,
			Subjects: []string{fmt.Sprintf("%s.%s", channel, queue)},
		})
		if err != nil {
			return nil, fmt.Errorf("add stream if needed, add stream: %v", err)
		}

		log.Println("stream added")
	}

	return stream, nil
}

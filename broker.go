package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Broker struct {
	conn  *amqp.Connection
	chans map[string]*amqp.Channel
}

func New() *Broker {
	return &Broker{
		chans: make(map[string]*amqp.Channel),
	}
}

func (b *Broker) Start(url string) error {
	var err error
	b.conn, err = amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("start, dial: %v", err)
	}

	return nil
}

func (b *Broker) Close() error {
	if err := b.conn.Close(); err != nil {
		return fmt.Errorf("connection close: %v", err)
	}

	return nil
}

func (b *Broker) Produce(
	ctx context.Context,
	channel string,
	queue string,
	object any,
) error {
	ch, err := b.addChannelIfNeeded(ctx, channel)
	if err != nil {
		return fmt.Errorf("produce, add channel if needed: %v", err)
	}

	q, err := b.declareQueueIfNeeded(ch, queue)
	if err != nil {
		return fmt.Errorf("produce, queue declare if needed: %v", err)
	}

	body, err := json.Marshal(object)
	if err != nil {
		return fmt.Errorf("produce, marshal: %v", err)
	}

	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := ch.PublishWithContext(
		ctx,
		"",
		q.Name,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        body,
		},
	); err != nil {
		return fmt.Errorf("produce, publish with context: %v", err)
	}

	return nil
}

func (b *Broker) Consume(
	ctx context.Context,
	channel string,
	queue string,
	block func(context.Context, *amqp.Delivery),
) error {
	ch, err := b.addChannelIfNeeded(ctx, channel)
	if err != nil {
		return fmt.Errorf("consume, add channel if needed: %v", err)
	}

	q, err := b.declareQueueIfNeeded(ch, queue)
	if err != nil {
		return fmt.Errorf("consume, queue declare if needed: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return fmt.Errorf("consume, channel consume: %v", err)
	}

	go b.listenQueue(ctx, msgs, block)

	return nil
}

func (b *Broker) addChannelIfNeeded(ctx context.Context, channel string) (*amqp.Channel, error) {
	if ch, ok := b.chans[channel]; ok {
		return ch, nil
	}

	ch, err := b.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("create channel: %v", err)
	}

	b.chans[channel] = ch

	go b.closeChannel(ctx, channel)

	return ch, nil
}

func (b *Broker) declareQueueIfNeeded(ch *amqp.Channel, queue string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queue,
		false,
		false,
		false,
		false,
		nil,
	)
}

func (b *Broker) listenQueue(
	ctx context.Context,
	msgs <-chan amqp.Delivery,
	block func(context.Context, *amqp.Delivery),
) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-msgs:
			block(ctx, &msg)
		}
	}
}

func (b *Broker) closeChannel(ctx context.Context, channel string) {
	<-ctx.Done()

	ch, ok := b.chans[channel]
	if !ok {
		return
	}

	delete(b.chans, channel)

	if err := ch.Close(); err != nil {
		log.Println("close channel", err)
	}
}

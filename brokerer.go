package broker

import "context"

type Brokerer interface {
	Start() error

	Close() error

	Produce(
		ctx context.Context,
		channel string,
		queue string,
		object any,
	) error

	Consume(
		ctx context.Context,
		channel string,
		queue string,
		action func(context.Context, []byte),
	) error
}

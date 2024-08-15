package gridlock

import "context"

type Publisher interface {
	Publish(
		ctx context.Context,
		entity_type string,
		entity_id string,
		event_type EVENT_TYPE,
		headers map[string]string,
		body []byte,
	) (uint64, error)
}

type Pipeline[T EventSpec] interface {
	One(
		ctx context.Context,
		event_type EVENT_TYPE,
		headers map[string]string,
		body T,
	) (uint64, error)
	Many(
		ctx context.Context,
		event_type EVENT_TYPE,
		headers map[string]string,
		body ...T,
	) ([]uint64, error)
}

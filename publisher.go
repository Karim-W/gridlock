package gridlock

import "context"

type Publisher interface {
	Publish(
		ctx context.Context,
		entity_type string,
		entity_id string,
		event_type string,
		headers map[string]string,
		body []byte,
	) (uint64, error)
}

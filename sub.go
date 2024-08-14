package gridlock

import (
	"context"
	"time"

	"github.com/karim-w/gopts"
)

type Subscriber interface {
	Subscribe(
		orgin string,
		entity_type string,
		last_sequence_number gopts.Option[uint64],
	) (<-chan Event, error)
	SetPullFrequency(frequency time.Duration)
	Fetch(
		ctx context.Context,
		orgin string,
		entity_type string,
		ids ...string,
	) ([]Event, error)
	GetSnapshotHistory(
		ctx context.Context,
		origin string,
		entity_type string,
		entity_id string,
	) ([]Event, error)
	Close()
}

package subscriber

import (
	"context"

	"github.com/karim-w/gridlock"
	"github.com/lib/pq"
)

func (s *SubscriberImpl) EntitySequences(
	ctx context.Context,
	orgin string,
	entity_type string,
	ids ...uint64,
) ([]gridlock.Event, error) {
	rows, err := s.db.QueryContext(
		ctx,
		query_ENTITY_SEQUENCES,
		orgin,
		entity_type,
		pq.Array(ids),
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return mapEvents(rows)
}

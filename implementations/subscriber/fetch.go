package subscriber

import (
	"context"

	"github.com/karim-w/gridlock"
)

func (s *SubscriberImpl) Fetch(
	ctx context.Context,
	orgin string,
	entity_type string,
	ids ...string,
) ([]gridlock.Event, error) {
	rows, err := s.db.QueryContext(ctx, query_FETCH)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return mapEvents(rows)
}

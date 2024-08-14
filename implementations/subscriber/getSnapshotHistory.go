package subscriber

import (
	"context"

	"github.com/karim-w/gridlock"
)

func (s *SubscriberImpl) GetSnapshotHistory(
	ctx context.Context,
	origin string,
	entity_type string,
	entity_id string,
) ([]gridlock.Event, error) {
	rows, err := s.db.QueryContext(
		ctx,
		query_SNAPSHOT_HISTORY,
		origin,
		entity_type,
		entity_id,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return mapEvents(rows)
}

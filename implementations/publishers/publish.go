package publishers

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"
)

func (p *PublisherImpl) Publish(
	ctx context.Context,
	entity_type string,
	entity_id string,
	event_type string,
	headers map[string]string,
	body []byte,
) (seqNo uint64, err error) {
	event_time := time.Now()

	// 1. Get Tx from db.
	tx, err := p.db.Begin()
	if err != nil {
		return
	}

	defer tx.Rollback()

	// 2. Lock the table.
	_, err = tx.ExecContext(ctx, query_LOCK_TABLE)
	if err != nil {
		return
	}

	// 3. Get the entity version.
	var entity_version uint64
	err = tx.QueryRowContext(ctx, query_get_entity_version, p.origin, entity_type, entity_id).
		Scan(&entity_version)
	if err != nil && err != sql.ErrNoRows {
		return
	}

	// 4. get sequence number.
	var sequence_number uint64
	err = tx.QueryRowContext(ctx, query_get_entity_sequence_number, p.origin, entity_type).
		Scan(&sequence_number)
	if err != nil && err != sql.ErrNoRows {
		return
	}

	// 5. Insert the event.
	headersByteA, err := json.Marshal(headers)
	if err != nil {
		return
	}

	if len(headersByteA) == 0 {
		headersByteA = []byte("{}")
	}

	err = tx.QueryRowContext(ctx, query_INSERT_EVENT,
		p.origin,
		entity_type,
		entity_id,
		entity_version+1,
		sequence_number+1,
		event_type,
		headersByteA,
		body,
		event_time,
	).Scan(&seqNo)
	if err != nil {
		return
	}

	// 5. Commit the transaction.
	err = tx.Commit()
	if err != nil {
		return
	}

	return
}

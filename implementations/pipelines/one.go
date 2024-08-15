package pipelines

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"github.com/karim-w/gridlock"
)

func (p *PipelineImpl[T]) One(
	ctx context.Context,
	event_type gridlock.EVENT_TYPE,
	headers map[string]string,
	body T,
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

	// 3. get sequence number.
	var sequence_number uint64
	err = tx.QueryRowContext(ctx, query_get_entity_sequence_number, p.origin, p.entity_type).
		Scan(&sequence_number)
	if err != nil && err != sql.ErrNoRows {
		return
	}

	// 4. Insert the event.
	headersByteA, err := json.Marshal(headers)
	if err != nil {
		return
	}

	if len(headersByteA) == 0 {
		headersByteA = []byte("{}")
	}

	bodyByteA, err := json.Marshal(body)
	if err != nil {
		return
	}

	if len(bodyByteA) == 0 {
		bodyByteA = []byte("{}")
	}

	err = tx.QueryRowContext(ctx, query_INSERT_EVENT,
		p.origin,
		p.entity_type,
		body.Id(),
		sequence_number+1,
		event_type,
		headersByteA,
		bodyByteA,
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

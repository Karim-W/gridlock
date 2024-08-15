package pipelines

import (
	"context"
	"database/sql"

	"github.com/karim-w/gridlock"
)

func (p *PipelineImpl[T]) Many(
	ctx context.Context,
	event_type gridlock.EVENT_TYPE,
	headers map[string]string,
	body ...T,
) (res []uint64, err error) {
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

	// 4. build query
	query, err := buildInsertPipeline(
		sequence_number,
		p.origin,
		p.entity_type,
		event_type,
		headers,
		body,
	)
	if err != nil {
		return nil, err
	}

	// 5. Insert the event.
	rows, err := tx.QueryContext(ctx, query)
	if err != nil {
		return
	}

	defer rows.Close()

	res = make([]uint64, 0, len(body))
	var seqNo uint64

	for rows.Next() {
		err = rows.Scan(&seqNo)
		if err != nil {
			return
		}

		res = append(res, seqNo)
	}

	// 6. Commit the transaction.
	err = tx.Commit()
	if err != nil {
		return
	}

	return
}

package subscriber

import (
	"database/sql"
	"encoding/json"

	"github.com/karim-w/gridlock"
)

func mapEvents(rows *sql.Rows) (res []gridlock.Event, err error) {
	var event gridlock.Event

	headersByta := []byte{}

	for rows.Next() {
		err = rows.Scan(
			&event.Id,
			&event.Origin,
			&event.SequenceNumber,
			&event.EntityType,
			&event.EntityID,
			&event.EntityVersion,
			&event.EventType,
			&headersByta,
			&event.Body,
			&event.CreatedAt,
		)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(headersByta, &event.Headers)
		if err != nil {
			return nil, err
		}

		res = append(res, event)

		headersByta = []byte{}
	}

	return
}

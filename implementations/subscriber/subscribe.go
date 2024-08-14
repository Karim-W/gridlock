package subscriber

import (
	"context"
	"log/slog"
	"sync"

	"github.com/karim-w/gopts"
	"github.com/karim-w/gridlock"
)

func (s *SubscriberImpl) Subscribe(
	orgin string,
	entity_type string,
	last_sequence_number gopts.Option[uint64],
) (<-chan gridlock.Event, error) {
	var seqNo uint64
	if last_sequence_number.IsSome() {
		seqNo = last_sequence_number.Unwrap()
	}

	mutex := &sync.Mutex{}

	go func(number uint64, mtx *sync.Mutex) {
		sequenceNumber := number

		for {
			<-s.ticker.C
			mtx.Lock()

			events, err := s.fetchSince(
				context.TODO(),
				orgin,
				entity_type,
				sequenceNumber,
			)
			if err != nil {
				slog.Error("error fetching events", slog.String("error", err.Error()))
			}
			if len(events) > 0 {
				for _, event := range events {
					sequenceNumber = event.SequenceNumber
					s.channel <- event
				}
			}

			mtx.Unlock()
		}
	}(seqNo, mutex)

	return s.channel, nil
}

func (s *SubscriberImpl) fetchSince(
	ctx context.Context,
	orgin string,
	entity_type string,
	last_sequence_number uint64,
) ([]gridlock.Event, error) {
	rows, err := s.db.QueryContext(
		ctx,
		query_subscribe,
		orgin,
		entity_type,
		last_sequence_number,
	)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	return mapEvents(rows)
}

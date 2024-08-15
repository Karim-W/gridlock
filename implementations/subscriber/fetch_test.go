package subscriber_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	go_test "github.com/karim-w/go-test"
	"github.com/karim-w/gridlock"
	"github.com/karim-w/gridlock/implementations/publishers"
	"github.com/karim-w/gridlock/implementations/subscriber"
	"github.com/karim-w/stdlib/sqldb"
)

func TestSubscriberImpl_Fetch(t *testing.T) {
	db, cleanup := go_test.InitDockerPostgresSQLDBTest(t)

	defer cleanup()

	type seed struct {
		entity_type string
		entity_id   string
		event_type  gridlock.EVENT_TYPE
		body        []byte
	}

	type fields struct {
		db    sqldb.DB
		seeds []seed
		orgin string
	}
	type args struct {
		ctx         context.Context
		entity_type string
		entity_ids  []string
	}

	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantedLen int
	}{
		{
			name: "Test SubscriberImpl.Fetch",
			fields: fields{
				orgin: "subscriber_test",
				db:    db,
				seeds: []seed{
					{
						entity_type: "users",
						entity_id:   uuid.NewString(),
						event_type:  "created",
						body:        []byte(`{"name": "John Doe"}`),
					},
					{
						entity_type: "users",
						entity_id:   uuid.NewString(),
						event_type:  "updated",
						body:        []byte(`{"name": "Jane Doe"}`),
					},
					{
						entity_type: "users",
						entity_id:   "ABC",
						event_type:  "created",
						body:        []byte(`{"name": "John Doe"}`),
					},
				},
			},
			args: args{
				ctx:         context.Background(),
				entity_type: "users",
				entity_ids:  []string{"ABC"},
			},
			wantErr:   false,
			wantedLen: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pub, err := publishers.NewWithDB(tt.fields.db, tt.fields.orgin)
			if err != nil {
				t.Fatal(err)
			}

			for _, seed := range tt.fields.seeds {
				_, err := pub.Publish(
					context.Background(),
					seed.entity_type,
					seed.entity_id,
					seed.event_type,
					map[string]string{
						"Content-Type": "application/json",
					},
					seed.body,
				)
				if err != nil {
					t.Fatal(err)
				}

			}

			s, err := subscriber.NewWithDB(tt.fields.db)
			if err != nil {
				t.Fatal(err)
			}

			for _, id := range tt.args.entity_ids {

				list, err := s.GetSnapshotHistory(
					tt.args.ctx,
					tt.fields.orgin,
					tt.args.entity_type,
					id,
				)

				if (err != nil) != tt.wantErr {
					t.Errorf(
						"SubscriberImpl.Fetch() error = %v, wantErr %v",
						err,
						tt.wantErr,
					)
					return
				}

				if len(list) != tt.wantedLen {
					t.Errorf(
						"SubscriberImpl.Fetch() len(list) = %d, wantedLen %d",
						len(list),
						tt.wantedLen,
					)
				}
			}
		})
	}
}

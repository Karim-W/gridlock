package subscriber_test

import (
	"context"
	"errors"
	"testing"
	"time"

	go_test "github.com/karim-w/go-test"
	"github.com/karim-w/gopts"
	"github.com/karim-w/gridlock/implementations/publishers"
	"github.com/karim-w/gridlock/implementations/subscriber"
	"github.com/karim-w/stdlib/sqldb"
)

func TestSubscriberImpl_Sub(t *testing.T) {
	db, cleanup := go_test.InitDockerPostgresSQLDBTest(t)

	defer cleanup()

	type seed struct {
		entity_type string
		entity_id   string
		event_type  string
		body        []byte
	}

	type fields struct {
		db    sqldb.DB
		seeds []seed
		orgin string
	}
	type args struct {
		ctx                  context.Context
		entity_type          string
		last_sequence_number uint64
	}

	tests := []struct {
		name      string
		fields    fields
		args      args
		wantErr   bool
		wantedLen int
	}{
		{
			name: "Test SubscriberImpl.SUB",
			fields: fields{
				orgin: "subscriber_test",
				db:    db,
				seeds: []seed{
					{
						entity_type: "users",
						entity_id:   "ABC",
						event_type:  "created",
						body:        []byte(`{"name": "John Doe"}`),
					},
					{
						entity_type: "users",
						entity_id:   "ABC",
						event_type:  "updated",
						body:        []byte(`{"name": "Jane Doe"}`),
					},
					{
						entity_type: "users",
						entity_id:   "ABC",
						event_type:  "deleted",
						body:        []byte(`{"name": "John Doe"}`),
					},
				},
			},
			args: args{
				ctx:                  context.Background(),
				entity_type:          "users",
				last_sequence_number: 0,
			},
			wantErr:   false,
			wantedLen: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			panik := func(err error) {
				if err != nil {
					t.Fatal(err)
				}
			}

			found := false

			go func(panik func(error)) {
				pub, err := publishers.NewWithDB(tt.fields.db, tt.fields.orgin)
				if err != nil {
					panik(err)
				}

				s, err := subscriber.NewWithDB(tt.fields.db)
				if err != nil {
					panik(err)
				}

				channel, err := s.Subscribe(
					tt.fields.orgin,
					tt.args.entity_type,
					gopts.Some(tt.args.last_sequence_number),
				)
				if err != nil {
					panik(err)
				}

				go func() {
					for _, seed := range tt.fields.seeds {
						pub.Publish(
							context.Background(),
							seed.entity_type,
							seed.entity_id,
							seed.event_type,
							map[string]string{
								"Content-Type": "application/json",
							},
							seed.body,
						)
					}
				}()

				go func() {
					time.Sleep(20 * time.Second)
					s.Close()
				}()

				counter := 0

				for event, ok := <-channel; ok; event, ok = <-channel {
					t.Logf("event: %v", event)
					counter++
				}

				if counter != tt.wantedLen {
					t.Errorf(
						"SubscriberImpl.Fetch() counter = %d, wantedLen %d",
						counter,
						tt.wantedLen,
					)
					panik(errors.New("counter != tt.wantedLen"))
				}

				found = true
			}(panik)

			time.Sleep(30 * time.Second)
			if !found {
				t.Fatal("timeout")
			}
		})
	}
}

package publishers

import (
	"context"
	"database/sql"
	"log"
	"testing"

	"github.com/google/uuid"
	gotest "github.com/karim-w/go-test"
	"github.com/karim-w/gridlock"
	"github.com/karim-w/stdlib/sqldb"
)

func TestPublisherImpl_Publish(t *testing.T) {
	db, cleanup := gotest.InitDockerPostgresSQLDBTest(t)
	defer cleanup()

	_, err := db.Exec(migrate_CREATE_TABLE)
	if err != nil {
		t.Fatal(err)
	}

	type fields struct {
		db     sqldb.DB
		origin string
	}
	type args struct {
		ctx         context.Context
		entity_type string
		entity_id   string
		event_type  gridlock.EVENT_TYPE
		headers     map[string]string
		body        []byte
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		should_exist bool
	}{
		{
			name: "Test PublisherImpl.Publish",
			fields: fields{
				db:     db,
				origin: "publisher_test",
			},
			args: args{
				ctx:         context.Background(),
				entity_type: "users",
				entity_id:   uuid.NewString(),
				event_type:  "created",
				headers: map[string]string{
					"Content-Type": "application/json",
				},
				body: []byte(`{"name": "John Doe"}`),
			},
			wantErr:      false,
			should_exist: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &PublisherImpl{
				db:     tt.fields.db,
				origin: tt.fields.origin,
			}
			var seqNo uint64
			var err error

			seqNo, err = p.Publish(
				tt.args.ctx,
				tt.args.entity_type,
				tt.args.entity_id,
				tt.args.event_type,
				tt.args.headers,
				tt.args.body,
			)

			log.Printf("seqNo: %d", seqNo)

			if (err != nil) != tt.wantErr {
				t.Errorf("PublisherImpl.Publish() error = %v, wantErr %v", err, tt.wantErr)
			}

			var seq uint64
			err = db.QueryRow("SELECT sequence_number FROM event_snapshots WHERE origin = $1 AND entity_type = $2 AND entity_id = $3 AND event_type = $4",
				tt.fields.origin, tt.args.entity_type, tt.args.entity_id, tt.args.event_type).
				Scan(&seq)
			if err != nil {
				if !(tt.should_exist && err == sql.ErrNoRows) {
					t.Fatal(err)
				}
				return
			}
			if seq != seqNo {
				t.Errorf("PublisherImpl.Publish sequence number mismatch = %v, want %v", seq, seqNo)
			}
		})
	}
}

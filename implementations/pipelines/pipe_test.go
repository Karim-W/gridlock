package pipelines_test

import (
	"context"
	"database/sql"
	"log"
	"sort"
	"testing"

	"github.com/google/uuid"
	gotest "github.com/karim-w/go-test"
	"github.com/karim-w/gridlock"
	"github.com/karim-w/gridlock/implementations/pipelines"
	"github.com/karim-w/stdlib/sqldb"
)

type test struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func (t test) Id() string {
	return t.ID
}

func TestPipelineImpl_One(t *testing.T) {
	db, cleanup := gotest.InitDockerPostgresSQLDBTest(t)
	defer cleanup()

	type fields struct {
		db          sqldb.DB
		origin      string
		entity_type string
	}
	type args struct {
		ctx        context.Context
		event_type gridlock.EVENT_TYPE
		headers    map[string]string
		body       test
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		should_exist bool
	}{
		{
			name: "Test PipelineImpl.One",
			fields: fields{
				db:     db,
				origin: "publisher_test",
			},
			args: args{
				ctx:        context.Background(),
				event_type: gridlock.CREATED_EVENT,
				headers: map[string]string{
					"Content-Type": "application/json",
				},
				body: test{
					ID:   uuid.NewString(),
					Name: "John Doe",
				},
			},
			wantErr:      false,
			should_exist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := pipelines.NewWithDB[test](
				tt.fields.db,
				tt.fields.origin,
				tt.fields.entity_type,
			)
			if err != nil {
				t.Fatal(err)
			}
			var seqNo uint64

			seqNo, err = p.One(
				tt.args.ctx,
				tt.args.event_type,
				tt.args.headers,
				tt.args.body,
			)

			log.Printf("seqNo: %d", seqNo)

			if (err != nil) != tt.wantErr {
				t.Errorf("PipelineImpl.One() error = %v, wantErr %v", err, tt.wantErr)
			}

			var seq uint64
			err = db.QueryRow("SELECT sequence_number FROM event_snapshots WHERE origin = $1 AND entity_type = $2 AND entity_id = $3 AND event_type = $4",
				tt.fields.origin, tt.fields.entity_type, tt.args.body.ID, tt.args.event_type).
				Scan(&seq)
			if err != nil {
				if !(tt.should_exist && err == sql.ErrNoRows) {
					t.Fatal(err)
				}
				return
			}
			if seq != seqNo {
				t.Errorf("PipelineImpl.One sequence number mismatch = %v, want %v", seq, seqNo)
			}
		})
	}
}

func TestPipelineImpl_Many(t *testing.T) {
	db, cleanup := gotest.InitDockerPostgresSQLDBTest(t)
	defer cleanup()

	type fields struct {
		db          sqldb.DB
		origin      string
		entity_type string
	}
	type args struct {
		ctx        context.Context
		event_type gridlock.EVENT_TYPE
		headers    map[string]string
		body       []test
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantErr      bool
		should_exist bool
	}{
		{
			name: "Test PipelineImpl.One",
			fields: fields{
				db:          db,
				origin:      "publisher_test",
				entity_type: uuid.NewString(),
			},
			args: args{
				ctx:        context.Background(),
				event_type: gridlock.CREATED_EVENT,
				headers: map[string]string{
					"Content-Type": "application/json",
				},
				body: []test{
					{
						ID:   uuid.NewString(),
						Name: "John Doe",
					},
					{
						ID:   uuid.NewString(),
						Name: "Jane Doe",
					},
				},
			},
			wantErr:      false,
			should_exist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := pipelines.NewWithDB[test](
				tt.fields.db,
				tt.fields.origin,
				tt.fields.entity_type,
			)
			if err != nil {
				t.Fatal(err)
			}
			var seqNo uint64

			seqNos, err := p.Many(
				tt.args.ctx,
				tt.args.event_type,
				tt.args.headers,
				tt.args.body...,
			)

			log.Printf("seqNo: %d", seqNo)

			if (err != nil) != tt.wantErr {
				t.Errorf("PipelineImpl.One() error = %v, wantErr %v", err, tt.wantErr)
			}

			var seqs []uint64
			rows, err := db.Query(
				"SELECT sequence_number FROM event_snapshots WHERE origin = $1 AND entity_type = $2  order by sequence_number desc",
				tt.fields.origin,
				tt.fields.entity_type,
			)
			if err != nil {
				if !(tt.should_exist && err == sql.ErrNoRows) {
					t.Fatal(err)
				}
				return
			}

			defer rows.Close()

			for rows.Next() {
				var seq uint64
				err = rows.Scan(&seq)
				if err != nil {
					t.Fatal(err)
				}
				seqs = append(seqs, seq)
			}

			sort.Slice(seqNos, func(i, j int) bool {
				return seqNos[i] > seqNos[j]
			})

			if len(seqs) != len(seqNos) {
				t.Errorf(
					"PipelineImpl.One sequence number mismatch = %v, want %v",
					len(seqs),
					len(seqNos),
				)
			}

			for i, seq := range seqs {
				if seq != seqNos[i] {
					t.Errorf(
						"PipelineImpl.One sequence number mismatch = %v, want %v",
						seq,
						seqNos[i],
					)
				}
			}
		})
	}
}

func TestPipelineImpl_OneMultiple(t *testing.T) {
	db, cleanup := gotest.InitDockerPostgresSQLDBTest(t)
	defer cleanup()

	type fields struct {
		db          sqldb.DB
		origin      string
		entity_type string
	}
	type args struct {
		ctx        context.Context
		event_type gridlock.EVENT_TYPE
		headers    map[string]string
		body       test
	}
	tests := []struct {
		name         string
		fields       fields
		args         []args
		wantErr      bool
		should_exist bool
	}{
		{
			name: "Test PipelineImpl.One",
			fields: fields{
				db:     db,
				origin: "publisher_test",
			},
			args: []args{
				{
					ctx:        context.Background(),
					event_type: gridlock.CREATED_EVENT,
					headers: map[string]string{
						"Content-Type": "application/json",
					},
					body: test{
						ID:   uuid.NewString(),
						Name: "John Doe",
					},
				}, {
					ctx:        context.Background(),
					event_type: gridlock.CREATED_EVENT,
					headers: map[string]string{
						"Content-Type": "application/json",
					},
					body: test{
						ID:   uuid.NewString(),
						Name: "John Doe",
					},
				},
			},
			wantErr:      false,
			should_exist: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := pipelines.NewWithDB[test](
				tt.fields.db,
				tt.fields.origin,
				tt.fields.entity_type,
			)
			if err != nil {
				t.Fatal(err)
			}
			var seqNo uint64

			for _, arg := range tt.args {

				seqNo, err = p.One(
					arg.ctx,
					arg.event_type,
					arg.headers,
					arg.body,
				)

				log.Printf("seqNo: %d", seqNo)

				if (err != nil) != tt.wantErr {
					t.Errorf("PipelineImpl.One() error = %v, wantErr %v", err, tt.wantErr)
				}

				var seq uint64
				err = db.QueryRow("SELECT sequence_number FROM event_snapshots WHERE origin = $1 AND entity_type = $2 AND entity_id = $3 AND event_type = $4",
					tt.fields.origin, tt.fields.entity_type, arg.body.ID, arg.event_type).
					Scan(&seq)
				if err != nil {
					if !(tt.should_exist && err == sql.ErrNoRows) {
						t.Fatal(err)
					}
					return
				}
				if seq != seqNo {
					t.Errorf("PipelineImpl.One sequence number mismatch = %v, want %v", seq, seqNo)
				}
			}
		})
	}
}

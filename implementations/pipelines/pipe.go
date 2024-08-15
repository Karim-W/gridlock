package pipelines

import (
	"context"

	"github.com/karim-w/gridlock"
	"github.com/karim-w/stdlib/sqldb"
)

type PipelineImpl[T gridlock.EventSpec] struct {
	db          sqldb.DB
	origin      string
	entity_type string
}

func New[T gridlock.EventSpec](
	dsn, origin, entity_type string,
) (gridlock.Pipeline[T], error) {
	db := sqldb.NewWithOptions("postgres", dsn, &sqldb.Options{
		MaxIdleConns: 20,
		MaxOpenConns: 20,
		Name:         "GRIDLOCK_Pipeline",
	})

	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &PipelineImpl[T]{db, origin, entity_type}, nil
}

func NewWithDB[T gridlock.EventSpec](
	db sqldb.DB,
	origin string,
	entity_type string,
) (gridlock.Pipeline[T], error) {
	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &PipelineImpl[T]{db, origin, entity_type}, nil
}

func automigrate(db sqldb.DB) error {
	var exists bool

	err := db.QueryRowContext(
		context.Background(),
		migrate_CHECK_IF_TABLE_EXISTS,
	).Scan(&exists)
	if err != nil {
		return err
	}

	if !exists {
		_, err = db.Exec(migrate_CREATE_TABLE)
		if err != nil {
			return err
		}
	}

	return nil
}

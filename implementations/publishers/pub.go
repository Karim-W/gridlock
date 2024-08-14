package publishers

import (
	"context"

	"github.com/karim-w/gridlock"
	"github.com/karim-w/stdlib/sqldb"
)

type PublisherImpl struct {
	db     sqldb.DB
	origin string
}

func New(dsn, driver, origin string) (gridlock.Publisher, error) {
	db := sqldb.NewWithOptions(driver, dsn, &sqldb.Options{
		MaxIdleConns:   20,
		MaxOpenConns:   20,
		PanicablePings: true,
		Name:           "GRIDLOCK_PUBLISHER",
	})

	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &PublisherImpl{db, origin}, nil
}

func NewWithDB(db sqldb.DB, origin string) (gridlock.Publisher, error) {
	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &PublisherImpl{db, origin}, nil
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

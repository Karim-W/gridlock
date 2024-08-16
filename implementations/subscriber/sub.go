package subscriber

import (
	"context"
	"sync"
	"time"

	"github.com/karim-w/gridlock"
	"github.com/karim-w/stdlib/sqldb"
)

const (
	_DEFAULT_FREQUENCY   = 5 * time.Second
	_DEFAULT_BUFFER_SIZE = 100_000
)

type SubscriberImpl struct {
	ticker  *time.Ticker
	channel chan gridlock.Event
	mu      *sync.Mutex
	db      sqldb.DB
}

func (s *SubscriberImpl) Close() {
	s.ticker.Stop()
	s.db.Close()
	close(s.channel)
}

type Options struct {
	PullFrequency time.Duration
	BufferSize    int
}

func New(
	dsn string,
) (gridlock.Subscriber, error) {
	db := sqldb.NewWithOptions("postgres", dsn, &sqldb.Options{
		MaxIdleConns: 20,
		MaxOpenConns: 20,
		Name:         "GRIDLOCK_SUBSCRIBER",
	})

	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &SubscriberImpl{
		ticker:  time.NewTicker(_DEFAULT_FREQUENCY),
		channel: make(chan gridlock.Event, _DEFAULT_BUFFER_SIZE),
		mu:      &sync.Mutex{},
		db:      db,
	}, nil
}

// NewWithOptions returns a new Subscriber with the given options.
func NewWithOptions(dsn string, opts Options) (gridlock.Subscriber, error) {
	db := sqldb.NewWithOptions("postgres", dsn, &sqldb.Options{
		MaxIdleConns: 20,
		MaxOpenConns: 20,
		Name:         "GRIDLOCK_SUBSCRIBER",
	})

	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &SubscriberImpl{
		ticker:  time.NewTicker(opts.PullFrequency),
		channel: make(chan gridlock.Event, opts.BufferSize),
		mu:      &sync.Mutex{},
	}, nil
}

// NewWithDB returns a new Subscriber with the given database.
func NewWithDB(db sqldb.DB) (gridlock.Subscriber, error) {
	err := automigrate(db)
	if err != nil {
		return nil, err
	}

	return &SubscriberImpl{
		ticker:  time.NewTicker(_DEFAULT_FREQUENCY),
		channel: make(chan gridlock.Event, _DEFAULT_BUFFER_SIZE),
		mu:      &sync.Mutex{},
		db:      db,
	}, nil
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

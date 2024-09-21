package publishers

const (
	migrate_CHECK_IF_TABLE_EXISTS = `SELECT EXISTS (
		SELECT 1
		FROM information_schema.tables
		WHERE table_schema = 'public'
		AND table_name = 'event_snapshots'
	);`

	migrate_CREATE_TABLE = `
		CREATE TABLE event_snapshots (
		id			  BIGSERIAL PRIMARY KEY,
		origin         TEXT NOT NULL,
		sequence_number BIGINT NOT NULL,
		entity_type     TEXT NOT NULL,
		entity_id       TEXT NOT NULL,
		event_type      TEXT NOT NULL,
		headers         JSONB NOT NULL,
		body            JSONB NOT NULL,
		created_at      TIMESTAMPTZ NOT NULL,
		UNIQUE (origin,entity_type,sequence_number)
	);

	CREATE INDEX event_snapshots_entity_type ON event_snapshots USING HASH (entity_type);
	CREATE INDEX event_snapshots_entity_id ON event_snapshots USING HASH (entity_id);
	CREATE INDEX event_snapshots_origin ON event_snapshots USING HASH (origin);
	CREATE INDEX event_snapshots_created_at ON event_snapshots USING BRIN (created_at);
	CREATE INDEX event_snapshots_sequence_number ON event_snapshots USING BRIN (sequence_number);

	CREATE INDEX idx_event_snapshots_origin_entity_sequence ON event_snapshots (origin, entity_type, sequence_number DESC);
	`

	query_LOCK_TABLE = `LOCK TABLE event_snapshots IN ACCESS EXCLUSIVE MODE;`

	query_LOCK_ROWS_FOR_ENTITY_TYPE = `
	SELECT id
	FROM event_snapshots
	WHERE entity_type = $1
	ORDER BY sequence_number DESC
	LIMIT 1
	FOR UPDATE;
	`

	query_get_entity_sequence_number = `
	SELECT sequence_number
	FROM event_snapshots
	WHERE origin = $1 AND entity_type = $2 
	ORDER BY sequence_number DESC
	LIMIT 1;
	`

	query_INSERT_EVENT = `
	INSERT INTO event_snapshots (
		origin,
		entity_type,
		entity_id,
		sequence_number,
		event_type,
		headers,
		body,
		created_at
	) VALUES (
		$1,
		$2,
		$3,
		$4,
		$5,
		$6,
		$7,
		$8
	) RETURNING sequence_number;
	`
)

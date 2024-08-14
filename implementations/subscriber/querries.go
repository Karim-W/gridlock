package subscriber

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
		entity_version  BIGINT NOT NULL,
		event_type      TEXT NOT NULL,
		headers         JSONB NOT NULL,
		body            JSONB NOT NULL,
		created_at      TIMESTAMPTZ NOT NULL,
		UNIQUE (origin, entity_type, entity_id, entity_version)
	);

	CREATE INDEX event_snapshots_entity_type ON event_snapshots USING HASH (entity_type);
	CREATE INDEX event_snapshots_entity_id ON event_snapshots USING HASH (entity_id);
	CREATE INDEX event_snapshots_origin ON event_snapshots USING HASH (origin);
	CREATE INDEX event_snapshots_created_at ON event_snapshots USING BRIN (created_at);
	`

	query_SNAPSHOT_HISTORY = `
	SELECT
		id,
		origin,
		sequence_number,
		entity_type,
		entity_id,
		entity_version,
		event_type,
		headers,
		body,
		created_at
	FROM event_snapshots
	WHERE origin = $1 AND entity_type = $2 AND entity_id = $3
	ORDER BY created_at ASC;
	`

	query_FETCH = `
	SELECT
		id,
		origin,
		sequence_number,
		entity_type,
		entity_id,
		entity_version,
		event_type,
		headers,
		body,
		created_at
	FROM event_snapshots
	WHERE origin = $1 AND entity_type = $2 AND entity_id = ANY($3)
	ORDER BY created_at ASC;
	`

	query_subscribe = `
	SELECT
		id,
		origin,
		sequence_number,
		entity_type,
		entity_id,
		entity_version,
		event_type,
		headers,
		body,
		created_at
	FROM event_snapshots
	WHERE origin = $1 AND entity_type = $2 AND sequence_number > $3
	ORDER BY created_at ASC
	`
)

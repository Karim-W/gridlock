package gridlock

import "time"

type EVENT_TYPE string

const (
	CREATED_EVENT EVENT_TYPE = "created"
	UPDATED_EVENT EVENT_TYPE = "updated"
	DELETED_EVENT EVENT_TYPE = "deleted"
)

type Event struct {
	Id             uint64            `json:"id"`
	Origin         string            `json:"origin"`
	SequenceNumber uint64            `json:"sequence_number"`
	EntityType     string            `json:"entity_type"`
	EntityID       string            `json:"entity_id"`
	EntityVersion  uint64            `json:"entity_version"`
	EventType      EVENT_TYPE        `json:"event_type"`
	Headers        map[string]string `json:"headers"`
	Body           []byte            `json:"body"`
	CreatedAt      time.Time         `json:"created_at"`
}

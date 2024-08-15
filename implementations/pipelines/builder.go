package pipelines

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/karim-w/gridlock"
	"github.com/lib/pq"
)

func buildInsertPipeline[T gridlock.EventSpec](
	start_sequence_number uint64,
	orgin string,
	entity_type string,
	event_type gridlock.EVENT_TYPE,
	headers map[string]string,
	bodies []T,
) (string, error) {
	builder := strings.Builder{}

	builder.WriteString(query_INSERT_EVENT_BASE)

	headersByteA, err := json.Marshal(headers)
	if err != nil {
		return "", err
	}

	if len(headersByteA) == 0 {
		headersByteA = []byte("{}")
	}

	offset := uint64(0)

	for i, body := range bodies {
		offset++

		bodyByta, err := json.Marshal(body)
		if err != nil {
			return "", err
		}

		builder.WriteString("(")
		builder.WriteString(pq.QuoteLiteral(orgin))
		builder.WriteString(",")
		builder.WriteString(pq.QuoteLiteral(entity_type))
		builder.WriteString(",")
		builder.WriteString(pq.QuoteLiteral(body.Id()))
		builder.WriteString(",")
		builder.WriteString(strconv.FormatUint(start_sequence_number+offset, 10))
		builder.WriteString(",")
		builder.WriteString(pq.QuoteLiteral(string(event_type)))
		builder.WriteString(",")
		builder.WriteString(pq.QuoteLiteral(string(headersByteA)))
		builder.WriteString(",")
		builder.WriteString(pq.QuoteLiteral(string(bodyByta)))
		builder.WriteString(",now())")

		if i < len(bodies)-1 {
			builder.WriteString(",")
		}
	}

	builder.WriteString(query_INSERT_RETURNING)

	return builder.String(), nil
}

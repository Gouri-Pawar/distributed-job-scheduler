package handlers

import "github.com/google/uuid"

// parseUUID centralizes uuid parsing so every handler doesn't repeat the
// error-ignoring boilerplate. Callers that need to handle malformed input
// from a URL path use uuid.Parse directly instead (see jobs.go / queues.go).
func parseUUID(s string) (uuid.UUID, error) {
	return uuid.Parse(s)
}

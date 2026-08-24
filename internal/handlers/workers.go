package handlers

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/models"
)

type WorkerHandler struct {
	Pool *pgxpool.Pool
}

// GET /projects/{projectID}/workers
func (h *WorkerHandler) List(w http.ResponseWriter, r *http.Request) {
	projectID, err := uuid.Parse(r.PathValue("projectID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid project id")
		return
	}

	// A worker is considered stale (probably crashed) if we haven't heard a
	// heartbeat in 30s but its status still says online — the dashboard can
	// surface this distinction directly from the query.
	rows, err := h.Pool.Query(r.Context(), `
		SELECT id, project_id, hostname, status, last_heartbeat_at, started_at,
		       (status = 'online' AND last_heartbeat_at < now() - interval '30 seconds') AS is_stale
		FROM workers WHERE project_id = $1 ORDER BY started_at DESC`, projectID)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not list workers")
		return
	}
	defer rows.Close()

	type workerOut struct {
		models.Worker
		IsStale bool `json:"is_stale"`
	}
	var out []workerOut
	for rows.Next() {
		var wk workerOut
		if err := rows.Scan(&wk.ID, &wk.ProjectID, &wk.Hostname, &wk.Status, &wk.LastHeartbeatAt, &wk.StartedAt, &wk.IsStale); err != nil {
			middleware.WriteError(w, http.StatusInternalServerError, "scan error")
			return
		}
		out = append(out, wk)
	}

	middleware.WriteJSON(w, http.StatusOK, out)
}

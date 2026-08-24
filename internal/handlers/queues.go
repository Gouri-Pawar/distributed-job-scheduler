package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/models"
)

type QueueHandler struct {
	Pool *pgxpool.Pool
}

type createQueueRequest struct {
	Name                  string               `json:"name"`
	Priority              int                  `json:"priority"`
	ConcurrencyLimit      int                  `json:"concurrency_limit"`
	RetryStrategy         models.RetryStrategy `json:"retry_strategy"`
	MaxRetries            int                  `json:"max_retries"`
	RetryBaseDelaySeconds int                  `json:"retry_base_delay_seconds"`
}

// POST /projects/{projectID}/queues
func (h *QueueHandler) Create(w http.ResponseWriter, r *http.Request) {
	projectID, err := uuid.Parse(r.PathValue("projectID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid project id")
		return
	}

	var req createQueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		middleware.WriteError(w, http.StatusBadRequest, "queue name is required")
		return
	}
	if req.ConcurrencyLimit <= 0 {
		req.ConcurrencyLimit = 5
	}
	if req.RetryStrategy == "" {
		req.RetryStrategy = models.RetryExponential
	}
	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}
	if req.RetryBaseDelaySeconds <= 0 {
		req.RetryBaseDelaySeconds = 5
	}

	var q models.Queue
	err = h.Pool.QueryRow(r.Context(), `
		INSERT INTO queues (project_id, name, priority, concurrency_limit, retry_strategy, max_retries, retry_base_delay_seconds)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		RETURNING id, project_id, name, priority, concurrency_limit, is_paused, retry_strategy, max_retries, retry_base_delay_seconds, created_at`,
		projectID, req.Name, req.Priority, req.ConcurrencyLimit, req.RetryStrategy, req.MaxRetries, req.RetryBaseDelaySeconds,
	).Scan(&q.ID, &q.ProjectID, &q.Name, &q.Priority, &q.ConcurrencyLimit, &q.IsPaused, &q.RetryStrategy, &q.MaxRetries, &q.RetryBaseDelaySeconds, &q.CreatedAt)
	if err != nil {
		middleware.WriteError(w, http.StatusConflict, "could not create queue (name may already exist in this project)")
		return
	}

	middleware.WriteJSON(w, http.StatusCreated, q)
}

// GET /projects/{projectID}/queues
func (h *QueueHandler) List(w http.ResponseWriter, r *http.Request) {
	projectID, err := uuid.Parse(r.PathValue("projectID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid project id")
		return
	}

	rows, err := h.Pool.Query(r.Context(), `
		SELECT id, project_id, name, priority, concurrency_limit, is_paused, retry_strategy, max_retries, retry_base_delay_seconds, created_at
		FROM queues WHERE project_id = $1 ORDER BY created_at DESC`, projectID)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not list queues")
		return
	}
	defer rows.Close()

	var out []models.Queue
	for rows.Next() {
		var q models.Queue
		if err := rows.Scan(&q.ID, &q.ProjectID, &q.Name, &q.Priority, &q.ConcurrencyLimit, &q.IsPaused, &q.RetryStrategy, &q.MaxRetries, &q.RetryBaseDelaySeconds, &q.CreatedAt); err != nil {
			middleware.WriteError(w, http.StatusInternalServerError, "scan error")
			return
		}
		out = append(out, q)
	}

	middleware.WriteJSON(w, http.StatusOK, out)
}

// PATCH /queues/{queueID}/pause
func (h *QueueHandler) Pause(w http.ResponseWriter, r *http.Request) {
	h.setPaused(w, r, true)
}

// PATCH /queues/{queueID}/resume
func (h *QueueHandler) Resume(w http.ResponseWriter, r *http.Request) {
	h.setPaused(w, r, false)
}

func (h *QueueHandler) setPaused(w http.ResponseWriter, r *http.Request, paused bool) {
	queueID, err := uuid.Parse(r.PathValue("queueID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid queue id")
		return
	}
	_, err = h.Pool.Exec(r.Context(), `UPDATE queues SET is_paused = $2 WHERE id = $1`, queueID, paused)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not update queue")
		return
	}
	middleware.WriteJSON(w, http.StatusOK, map[string]bool{"is_paused": paused})
}

// GET /queues/{queueID}/stats
func (h *QueueHandler) Stats(w http.ResponseWriter, r *http.Request) {
	queueID, err := uuid.Parse(r.PathValue("queueID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid queue id")
		return
	}

	rows, err := h.Pool.Query(r.Context(), `
		SELECT status, count(*) FROM jobs WHERE queue_id = $1 GROUP BY status`, queueID)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not compute stats")
		return
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var status string
		var count int
		if err := rows.Scan(&status, &count); err != nil {
			middleware.WriteError(w, http.StatusInternalServerError, "scan error")
			return
		}
		counts[status] = count
	}

	middleware.WriteJSON(w, http.StatusOK, counts)
}

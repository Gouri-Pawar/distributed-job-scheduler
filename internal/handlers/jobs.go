package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/models"
)

type JobHandler struct {
	Pool *pgxpool.Pool
}

// POST /queues/{queueID}/jobs
// Handles all five job types via the same endpoint:
//   - immediate: no run_at, no cron_expr          -> run_at defaults to now()
//   - delayed:   run_at set a few minutes/hours out
//   - scheduled: run_at set to a specific future timestamp (same mechanism as delayed)
//   - recurring: cron_expr set                     -> a scheduler tick spawns concrete jobs from this template
//   - batch:     batch_id set (client generates one UUID, reuses it across multiple create calls)
func (h *JobHandler) Create(w http.ResponseWriter, r *http.Request) {
	queueID, err := uuid.Parse(r.PathValue("queueID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid queue id")
		return
	}

	var req models.CreateJobRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.JobType == "" {
		middleware.WriteError(w, http.StatusBadRequest, "job_type is required")
		return
	}
	if len(req.Payload) == 0 {
		req.Payload = json.RawMessage(`{}`)
	}

	runAt := time.Now()
	if req.RunAt != nil {
		runAt = *req.RunAt
	}

	status := models.StatusQueued
	if runAt.After(time.Now()) || req.CronExpr != nil {
		status = models.StatusScheduled
	}

	var job models.Job
	err = h.Pool.QueryRow(r.Context(), `
		INSERT INTO jobs (queue_id, job_type, payload, status, priority, idempotency_key,
		                   run_at, cron_expr, batch_id, max_retries, retry_strategy)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		RETURNING id, queue_id, job_type, payload, status, priority, run_at, attempt_count, created_at, updated_at`,
		queueID, req.JobType, req.Payload, status, req.Priority, req.IdempotencyKey,
		runAt, req.CronExpr, req.BatchID, req.MaxRetries, req.RetryStrategy,
	).Scan(&job.ID, &job.QueueID, &job.JobType, &job.Payload, &job.Status, &job.Priority, &job.RunAt, &job.AttemptCount, &job.CreatedAt, &job.UpdatedAt)
	if err != nil {
		// Covers the idempotency_key unique-violation case too — treat as
		// "already accepted" rather than a hard error where appropriate.
		middleware.WriteError(w, http.StatusConflict, "could not create job (idempotency_key may already exist on this queue)")
		return
	}

	middleware.WriteJSON(w, http.StatusCreated, job)
}

// GET /queues/{queueID}/jobs?status=&job_type=&page=&page_size=
func (h *JobHandler) List(w http.ResponseWriter, r *http.Request) {
	queueID, err := uuid.Parse(r.PathValue("queueID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid queue id")
		return
	}

	statusFilter := r.URL.Query().Get("status")
	typeFilter := r.URL.Query().Get("job_type")

	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("page_size"))
	if pageSize < 1 || pageSize > 100 {
		pageSize = 25
	}
	offset := (page - 1) * pageSize

	query := `
		SELECT id, queue_id, job_type, payload, status, priority, run_at, attempt_count, created_at, updated_at
		FROM jobs WHERE queue_id = $1`
	args := []interface{}{queueID}

	if statusFilter != "" {
		args = append(args, statusFilter)
		query += ` AND status = $` + strconv.Itoa(len(args))
	}
	if typeFilter != "" {
		args = append(args, typeFilter)
		query += ` AND job_type = $` + strconv.Itoa(len(args))
	}

	args = append(args, pageSize, offset)
	query += ` ORDER BY created_at DESC LIMIT $` + strconv.Itoa(len(args)-1) + ` OFFSET $` + strconv.Itoa(len(args))

	rows, err := h.Pool.Query(r.Context(), query, args...)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not list jobs")
		return
	}
	defer rows.Close()

	var out []models.Job
	for rows.Next() {
		var j models.Job
		if err := rows.Scan(&j.ID, &j.QueueID, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.RunAt, &j.AttemptCount, &j.CreatedAt, &j.UpdatedAt); err != nil {
			middleware.WriteError(w, http.StatusInternalServerError, "scan error")
			return
		}
		out = append(out, j)
	}

	middleware.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"page": page, "page_size": pageSize, "jobs": out,
	})
}

// GET /jobs/{jobID}  — full detail including execution history + logs
func (h *JobHandler) Get(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	var j models.Job
	err = h.Pool.QueryRow(r.Context(), `
		SELECT id, queue_id, job_type, payload, status, priority, run_at, attempt_count, created_at, updated_at
		FROM jobs WHERE id = $1`, jobID,
	).Scan(&j.ID, &j.QueueID, &j.JobType, &j.Payload, &j.Status, &j.Priority, &j.RunAt, &j.AttemptCount, &j.CreatedAt, &j.UpdatedAt)
	if err != nil {
		middleware.WriteError(w, http.StatusNotFound, "job not found")
		return
	}

	rows, err := h.Pool.Query(r.Context(), `
		SELECT id, job_id, attempt_number, worker_id, status, started_at, finished_at, duration_ms, error_message
		FROM job_executions WHERE job_id = $1 ORDER BY attempt_number ASC`, jobID)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not load executions")
		return
	}
	defer rows.Close()

	var executions []models.JobExecution
	for rows.Next() {
		var e models.JobExecution
		if err := rows.Scan(&e.ID, &e.JobID, &e.AttemptNumber, &e.WorkerID, &e.Status, &e.StartedAt, &e.FinishedAt, &e.DurationMs, &e.ErrorMessage); err != nil {
			middleware.WriteError(w, http.StatusInternalServerError, "scan error")
			return
		}
		executions = append(executions, e)
	}

	middleware.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"job":        j,
		"executions": executions,
	})
}

// POST /jobs/{jobID}/retry — manually requeue a failed or dead-lettered job.
// This is what the dashboard's "Retry" button calls.
func (h *JobHandler) Retry(w http.ResponseWriter, r *http.Request) {
	jobID, err := uuid.Parse(r.PathValue("jobID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid job id")
		return
	}

	tag, err := h.Pool.Exec(r.Context(), `
		UPDATE jobs
		SET status = 'queued', run_at = now(), claimed_by = NULL, claimed_at = NULL
		WHERE id = $1 AND status IN ('failed', 'dead_letter', 'cancelled')`, jobID)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not retry job")
		return
	}
	if tag.RowsAffected() == 0 {
		middleware.WriteError(w, http.StatusConflict, "job is not in a retryable state")
		return
	}

	middleware.WriteJSON(w, http.StatusOK, map[string]string{"status": "queued"})
}

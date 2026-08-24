package handlers

import (
	"net/http"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
)

type DashboardHandler struct {
	Pool *pgxpool.Pool
}

// GET /projects/{projectID}/dashboard
// One aggregate endpoint so the frontend doesn't have to fire five requests
// just to render the summary cards at the top of the page.
func (h *DashboardHandler) Summary(w http.ResponseWriter, r *http.Request) {
	projectID, err := uuid.Parse(r.PathValue("projectID"))
	if err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid project id")
		return
	}
	ctx := r.Context()

	var queueCount, workerCount, onlineWorkerCount int
	h.Pool.QueryRow(ctx, `SELECT count(*) FROM queues WHERE project_id = $1`, projectID).Scan(&queueCount)
	h.Pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE project_id = $1`, projectID).Scan(&workerCount)
	h.Pool.QueryRow(ctx, `SELECT count(*) FROM workers WHERE project_id = $1 AND status = 'online'`, projectID).Scan(&onlineWorkerCount)

	rows, err := h.Pool.Query(ctx, `
		SELECT j.status, count(*)
		FROM jobs j JOIN queues q ON j.queue_id = q.id
		WHERE q.project_id = $1
		GROUP BY j.status`, projectID)
	jobCounts := map[string]int{}
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var status string
			var count int
			if rows.Scan(&status, &count) == nil {
				jobCounts[status] = count
			}
		}
	}

	var dlqCount int
	h.Pool.QueryRow(ctx, `
		SELECT count(*) FROM dead_letter_entries d
		JOIN queues q ON d.queue_id = q.id
		WHERE q.project_id = $1 AND d.replayed_at IS NULL`, projectID).Scan(&dlqCount)

	// Simple throughput signal: completions in the last hour.
	var completedLastHour int
	h.Pool.QueryRow(ctx, `
		SELECT count(*) FROM jobs j JOIN queues q ON j.queue_id = q.id
		WHERE q.project_id = $1 AND j.status = 'completed' AND j.completed_at > now() - interval '1 hour'`,
		projectID).Scan(&completedLastHour)

	middleware.WriteJSON(w, http.StatusOK, map[string]interface{}{
		"queue_count":          queueCount,
		"worker_count":         workerCount,
		"online_worker_count":  onlineWorkerCount,
		"job_counts_by_status": jobCounts,
		"dlq_pending_count":    dlqCount,
		"completed_last_hour":  completedLastHour,
	})
}

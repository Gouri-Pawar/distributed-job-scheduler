package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/models"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/queue"
)

// JobHandler is user-registered code that actually executes a job_type.
// Returning an error marks the attempt as failed and triggers retry logic.
type JobHandler func(ctx context.Context, payload json.RawMessage) error

// Engine is a single worker process. It can service multiple queues.
type Engine struct {
	Pool          *pgxpool.Pool
	WorkerID      uuid.UUID
	ProjectID     uuid.UUID
	QueueIDs      []uuid.UUID
	Concurrency   int           // max jobs running at once across all queues on this worker
	PollInterval  time.Duration
	HeartbeatEvery time.Duration

	handlers map[string]JobHandler
	sem      chan struct{} // bounds concurrent job execution
}

func NewEngine(pool *pgxpool.Pool, projectID uuid.UUID, queueIDs []uuid.UUID, concurrency int) *Engine {
	return &Engine{
		Pool:           pool,
		ProjectID:      projectID,
		QueueIDs:       queueIDs,
		Concurrency:    concurrency,
		PollInterval:   2 * time.Second,
		HeartbeatEvery: 10 * time.Second,
		handlers:       make(map[string]JobHandler),
		sem:            make(chan struct{}, concurrency),
	}
}

// RegisterHandler wires a job_type string to the code that executes it.
// e.g. engine.RegisterHandler("send_email", sendEmailHandler)
func (e *Engine) RegisterHandler(jobType string, h JobHandler) {
	e.handlers[jobType] = h
}

// Run starts polling + heartbeating and blocks until ctx is cancelled.
// On cancellation it stops claiming NEW jobs immediately but waits for
// in-flight jobs to finish before returning (graceful shutdown).
func (e *Engine) Run(ctx context.Context) error {
	hostname, _ := os.Hostname()
	if err := e.registerWorker(ctx, hostname); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	log.Printf("worker %s started (concurrency=%d, queues=%v)", e.WorkerID, e.Concurrency, e.QueueIDs)

	// inFlight tracks currently-executing jobs so shutdown can wait on them.
	inFlight := make(chan struct{})
	activeCount := 0
	done := make(chan struct{})

	go e.heartbeatLoop(ctx)

	pollTicker := time.NewTicker(e.PollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("shutdown signal received: no longer claiming new jobs, draining in-flight work...")
			e.setWorkerStatus(context.Background(), "draining")
			// Wait for all currently-running jobs to finish. Each job
			// execution releases a semaphore slot when done; once we can
			// fill the whole buffer back up, everything has finished.
			for i := 0; i < e.Concurrency; i++ {
				e.sem <- struct{}{}
			}
			e.setWorkerStatus(context.Background(), "offline")
			log.Println("all in-flight jobs finished, worker exiting cleanly")
			close(done)
			return nil

		case <-pollTicker.C:
			e.pollAndDispatch(ctx, inFlight, &activeCount)
		}
	}
}

func (e *Engine) pollAndDispatch(ctx context.Context, inFlight chan struct{}, activeCount *int) {
	for _, qID := range e.QueueIDs {
		select {
		case e.sem <- struct{}{}: // reserve a concurrency slot; blocks if full (non-blocking check below instead)
		default:
			continue // worker is at full concurrency right now, skip this queue this tick
		}

		jobs, err := queue.ClaimJobs(ctx, e.Pool, qID, e.WorkerID, 1)
		if err != nil {
			log.Printf("claim error on queue %s: %v", qID, err)
			<-e.sem
			continue
		}
		if len(jobs) == 0 {
			<-e.sem
			continue
		}

		job := jobs[0]
		go e.execute(ctx, job) // releases e.sem itself when done
	}
}

func (e *Engine) execute(ctx context.Context, job queue.ClaimedJob) {
	defer func() { <-e.sem }() // always release the concurrency slot

	attempt := job.AttemptCount + 1
	execID, err := queue.MarkRunning(ctx, e.Pool, job.ID, e.WorkerID, attempt)
	if err != nil {
		log.Printf("mark running failed for job %s: %v", job.ID, err)
		return
	}

	handler, ok := e.handlers[job.JobType]
	if !ok {
		e.fail(ctx, job, execID, attempt, fmt.Errorf("no handler registered for job_type %q", job.JobType), 0)
		return
	}

	start := time.Now()
	// Give each job its own timeout so one stuck job can't hang a worker slot forever.
	jobCtx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	runErr := handler(jobCtx, job.Payload)
	durationMs := int(time.Since(start).Milliseconds())

	if runErr != nil {
		e.fail(ctx, job, execID, attempt, runErr, durationMs)
		return
	}

	if err := queue.CompleteJob(ctx, e.Pool, job.ID, execID, durationMs); err != nil {
		log.Printf("complete-job write failed for %s: %v", job.ID, err)
	}
}

func (e *Engine) fail(ctx context.Context, job queue.ClaimedJob, execID uuid.UUID, attempt int, runErr error, durationMs int) {
	strategy, baseDelay, maxRetries := e.retryConfigFor(ctx, job.QueueID)
	retryable := queue.ShouldRetry(strategy, attempt, maxRetries)

	var retryAt *time.Time
	if retryable {
		t := time.Now().Add(queue.NextRetryDelay(strategy, baseDelay, attempt))
		retryAt = &t
	}

	if err := queue.FailJob(context.Background(), e.Pool, job.ID, execID, runErr.Error(), durationMs, retryAt, !retryable); err != nil {
		log.Printf("fail-job write failed for %s: %v", job.ID, err)
	}
	if !retryable {
		log.Printf("job %s exhausted retries, moved to DLQ: %v", job.ID, runErr)
	}
}

// retryConfigFor reads the queue's default retry policy. (Per-job overrides
// are checked first in a fuller implementation; simplified here for clarity.)
func (e *Engine) retryConfigFor(ctx context.Context, queueID uuid.UUID) (models.RetryStrategy, int, int) {
	var strategy models.RetryStrategy
	var baseDelay, maxRetries int
	err := e.Pool.QueryRow(ctx, `
		SELECT retry_strategy, retry_base_delay_seconds, max_retries
		FROM queues WHERE id = $1`, queueID).Scan(&strategy, &baseDelay, &maxRetries)
	if err != nil {
		log.Printf("could not load retry config for queue %s, defaulting to no-retry: %v", queueID, err)
		return models.RetryNone, 0, 0
	}
	return strategy, baseDelay, maxRetries
}

func (e *Engine) registerWorker(ctx context.Context, hostname string) error {
	return e.Pool.QueryRow(ctx, `
		INSERT INTO workers (project_id, hostname, status, last_heartbeat_at)
		VALUES ($1, $2, 'online', now())
		RETURNING id`, e.ProjectID, hostname).Scan(&e.WorkerID)
}

func (e *Engine) setWorkerStatus(ctx context.Context, status string) {
	_, err := e.Pool.Exec(ctx, `UPDATE workers SET status = $2 WHERE id = $1`, e.WorkerID, status)
	if err != nil {
		log.Printf("failed to set worker status to %s: %v", status, err)
	}
}

func (e *Engine) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(e.HeartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			active := len(e.sem)
			_, err := e.Pool.Exec(context.Background(), `
				UPDATE workers SET last_heartbeat_at = now() WHERE id = $1`, e.WorkerID)
			if err != nil {
				log.Printf("heartbeat update failed: %v", err)
			}
			_, err = e.Pool.Exec(context.Background(), `
				INSERT INTO worker_heartbeats (worker_id, reported_at, active_jobs)
				VALUES ($1, now(), $2)`, e.WorkerID, active)
			if err != nil {
				log.Printf("heartbeat insert failed: %v", err)
			}
		}
	}
}

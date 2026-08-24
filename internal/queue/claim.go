package queue

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ClaimedJob is the minimal shape a worker needs to execute a job.
type ClaimedJob struct {
	ID           uuid.UUID
	QueueID      uuid.UUID
	JobType      string
	Payload      []byte
	AttemptCount int
}

// ClaimJobs is THE critical concurrency-safe operation in this system.
//
// Problem: N workers poll the same queue concurrently. Two workers must
// never be able to claim the same job.
//
// Solution: a single SQL statement does SELECT ... FOR UPDATE SKIP LOCKED
// followed by an UPDATE, all inside one transaction.
//
//   - FOR UPDATE takes a row lock on the candidate rows.
//   - SKIP LOCKED means: if another transaction already holds the lock on
//     a row (i.e. another worker is claiming it right now), just skip that
//     row instead of blocking on it. No two workers ever block each other,
//     and no two workers ever get the same row.
//   - We only select from idx_jobs_claimable (queued/scheduled + run_at
//     due), ordered by priority then run_at, and cap it at `limit` rows.
//
// This pattern is the same one used by real systems like Postgres-backed
// queues (e.g. what pgqueue / river / oban-in-Elixir use) instead of an
// external broker like Redis — it avoids a second moving part while still
// giving hard claim-uniqueness guarantees, because Postgres MVCC does the
// mutual exclusion for us.
func ClaimJobs(ctx context.Context, pool *pgxpool.Pool, queueID uuid.UUID, workerID uuid.UUID, limit int) ([]ClaimedJob, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) // no-op if committed

	const selectQuery = `
		SELECT id, queue_id, job_type, payload, attempt_count
		FROM jobs
		WHERE queue_id = $1
		  AND status IN ('queued', 'scheduled')
		  AND run_at <= now()
		ORDER BY priority DESC, run_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`

	rows, err := tx.Query(ctx, selectQuery, queueID, limit)
	if err != nil {
		return nil, fmt.Errorf("select candidates: %w", err)
	}

	var claimed []ClaimedJob
	var ids []uuid.UUID
	for rows.Next() {
		var j ClaimedJob
		if err := rows.Scan(&j.ID, &j.QueueID, &j.JobType, &j.Payload, &j.AttemptCount); err != nil {
			rows.Close()
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		claimed = append(claimed, j)
		ids = append(ids, j.ID)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(claimed) == 0 {
		return nil, tx.Commit(ctx) // nothing to do, commit the empty (fast) tx
	}

	// Mark all claimed rows atomically in the SAME transaction that holds
	// the row locks, so nothing can interleave between "select" and "mark".
	const updateQuery = `
		UPDATE jobs
		SET status = 'claimed',
		    claimed_by = $1,
		    claimed_at = now()
		WHERE id = ANY($2)
	`
	if _, err := tx.Exec(ctx, updateQuery, workerID, ids); err != nil {
		return nil, fmt.Errorf("mark claimed: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit claim: %w", err)
	}

	return claimed, nil
}

// MarkRunning transitions a claimed job to running and records the start
// of a new execution attempt row. Returns the execution ID so the worker
// can later report completion/failure against it.
func MarkRunning(ctx context.Context, pool *pgxpool.Pool, jobID, workerID uuid.UUID, attemptNumber int) (uuid.UUID, error) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'running', started_at = now(), attempt_count = $2
		WHERE id = $1`, jobID, attemptNumber); err != nil {
		return uuid.Nil, err
	}

	var execID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO job_executions (job_id, attempt_number, worker_id, status, started_at)
		VALUES ($1, $2, $3, 'running', now())
		RETURNING id`, jobID, attemptNumber, workerID).Scan(&execID)
	if err != nil {
		return uuid.Nil, err
	}

	return execID, tx.Commit(ctx)
}

// CompleteJob marks a job + its current execution as successfully completed.
func CompleteJob(ctx context.Context, pool *pgxpool.Pool, jobID, execID uuid.UUID, durationMs int) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE jobs SET status = 'completed', completed_at = now() WHERE id = $1`, jobID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE job_executions SET status = 'completed', finished_at = now(), duration_ms = $2
		WHERE id = $1`, execID, durationMs); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// FailJob handles a failed attempt: either schedules a retry (with backoff
// delay computed by the caller) or moves the job to the Dead Letter Queue
// if retries are exhausted.
func FailJob(ctx context.Context, pool *pgxpool.Pool, jobID, execID uuid.UUID, errMsg string, durationMs int, retryAt *time.Time, maxRetriesExceeded bool) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
		UPDATE job_executions SET status = 'failed', finished_at = now(), duration_ms = $2, error_message = $3
		WHERE id = $1`, execID, durationMs, errMsg); err != nil {
		return err
	}

	if maxRetriesExceeded {
		if _, err := tx.Exec(ctx, `
			UPDATE jobs SET status = 'dead_letter' WHERE id = $1`, jobID); err != nil {
			return err
		}
		var queueID uuid.UUID
		var attemptCount int
		if err := tx.QueryRow(ctx, `SELECT queue_id, attempt_count FROM jobs WHERE id = $1`, jobID).Scan(&queueID, &attemptCount); err != nil {
			return err
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO dead_letter_entries (job_id, queue_id, final_error, attempt_count)
			VALUES ($1, $2, $3, $4)`, jobID, queueID, errMsg, attemptCount); err != nil {
			return err
		}
	} else {
		// Requeue for a future retry: status back to 'scheduled', run_at pushed out.
		if _, err := tx.Exec(ctx, `
			UPDATE jobs SET status = 'scheduled', run_at = $2, claimed_by = NULL, claimed_at = NULL
			WHERE id = $1`, jobID, retryAt); err != nil {
			return err
		}
	}

	return tx.Commit(ctx)
}

package models

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

// JobStatus mirrors the CHECK constraint on jobs.status
type JobStatus string

const (
	StatusQueued     JobStatus = "queued"
	StatusScheduled  JobStatus = "scheduled"
	StatusClaimed    JobStatus = "claimed"
	StatusRunning    JobStatus = "running"
	StatusCompleted  JobStatus = "completed"
	StatusFailed     JobStatus = "failed"
	StatusDeadLetter JobStatus = "dead_letter"
	StatusCancelled  JobStatus = "cancelled"
)

type RetryStrategy string

const (
	RetryNone        RetryStrategy = "none"
	RetryFixed       RetryStrategy = "fixed"
	RetryLinear      RetryStrategy = "linear"
	RetryExponential RetryStrategy = "exponential"
)

type User struct {
	ID           uuid.UUID `json:"id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

type Project struct {
	ID        uuid.UUID `json:"id"`
	OwnerID   uuid.UUID `json:"owner_id"`
	Name      string    `json:"name"`
	APIKey    string    `json:"api_key"`
	CreatedAt time.Time `json:"created_at"`
}

type Queue struct {
	ID                    uuid.UUID     `json:"id"`
	ProjectID             uuid.UUID     `json:"project_id"`
	Name                  string        `json:"name"`
	Priority              int           `json:"priority"`
	ConcurrencyLimit      int           `json:"concurrency_limit"`
	IsPaused              bool          `json:"is_paused"`
	RetryStrategy         RetryStrategy `json:"retry_strategy"`
	MaxRetries            int           `json:"max_retries"`
	RetryBaseDelaySeconds int           `json:"retry_base_delay_seconds"`
	CreatedAt             time.Time     `json:"created_at"`
}

type Job struct {
	ID             uuid.UUID       `json:"id"`
	QueueID        uuid.UUID       `json:"queue_id"`
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Status         JobStatus       `json:"status"`
	Priority       int             `json:"priority"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	RunAt          time.Time       `json:"run_at"`
	CronExpr       *string         `json:"cron_expr,omitempty"`
	BatchID        *uuid.UUID      `json:"batch_id,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
	RetryStrategy  *RetryStrategy  `json:"retry_strategy,omitempty"`
	AttemptCount   int             `json:"attempt_count"`
	ClaimedBy      *uuid.UUID      `json:"claimed_by,omitempty"`
	ClaimedAt      *time.Time      `json:"claimed_at,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	CompletedAt    *time.Time      `json:"completed_at,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	UpdatedAt      time.Time       `json:"updated_at"`
}

type JobExecution struct {
	ID            uuid.UUID  `json:"id"`
	JobID         uuid.UUID  `json:"job_id"`
	AttemptNumber int        `json:"attempt_number"`
	WorkerID      *uuid.UUID `json:"worker_id,omitempty"`
	Status        string     `json:"status"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
	DurationMs    *int       `json:"duration_ms,omitempty"`
	ErrorMessage  *string    `json:"error_message,omitempty"`
}

type Worker struct {
	ID              uuid.UUID  `json:"id"`
	ProjectID       uuid.UUID  `json:"project_id"`
	Hostname        string     `json:"hostname"`
	Status          string     `json:"status"`
	LastHeartbeatAt *time.Time `json:"last_heartbeat_at,omitempty"`
	StartedAt       time.Time  `json:"started_at"`
}

// CreateJobRequest is the payload accepted by POST /queues/{id}/jobs
type CreateJobRequest struct {
	JobType        string          `json:"job_type"`
	Payload        json.RawMessage `json:"payload"`
	Priority       int             `json:"priority"`
	RunAt          *time.Time      `json:"run_at,omitempty"`          // for delayed/scheduled jobs
	CronExpr       *string         `json:"cron_expr,omitempty"`       // for recurring jobs
	BatchID        *uuid.UUID      `json:"batch_id,omitempty"`        // for batch jobs
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	MaxRetries     *int            `json:"max_retries,omitempty"`
	RetryStrategy  *RetryStrategy  `json:"retry_strategy,omitempty"`
}

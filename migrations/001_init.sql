-- ============================================================
-- Distributed Job Scheduler — Core Schema
-- ============================================================

CREATE EXTENSION IF NOT EXISTS pgcrypto; -- for gen_random_uuid()

-- ---------- Users & Projects ----------

CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE projects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    owner_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    name        TEXT NOT NULL,
    api_key     TEXT NOT NULL UNIQUE DEFAULT encode(gen_random_bytes(24), 'hex'),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (owner_id, name)
);

-- ---------- Queues ----------

CREATE TABLE queues (
    id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id        UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name              TEXT NOT NULL,
    priority          INT NOT NULL DEFAULT 0,          -- higher = served first
    concurrency_limit INT NOT NULL DEFAULT 5,           -- max jobs running at once for this queue
    is_paused         BOOLEAN NOT NULL DEFAULT false,
    -- default retry policy for jobs in this queue (can be overridden per-job)
    retry_strategy    TEXT NOT NULL DEFAULT 'exponential'
                        CHECK (retry_strategy IN ('none','fixed','linear','exponential')),
    max_retries       INT NOT NULL DEFAULT 3,
    retry_base_delay_seconds INT NOT NULL DEFAULT 5,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (project_id, name)
);

CREATE INDEX idx_queues_project ON queues(project_id);

-- ---------- Workers ----------

CREATE TABLE workers (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id     UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    hostname       TEXT NOT NULL,
    status         TEXT NOT NULL DEFAULT 'online' CHECK (status IN ('online','offline','draining')),
    last_heartbeat_at TIMESTAMPTZ,
    started_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_workers_project ON workers(project_id);
CREATE INDEX idx_workers_heartbeat ON workers(last_heartbeat_at);

CREATE TABLE worker_heartbeats (
    id          BIGSERIAL PRIMARY KEY,
    worker_id   UUID NOT NULL REFERENCES workers(id) ON DELETE CASCADE,
    reported_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    active_jobs INT NOT NULL DEFAULT 0,
    cpu_percent NUMERIC(5,2),
    mem_mb      INT
);

-- history table can grow large; index for pruning/queries by time
CREATE INDEX idx_heartbeats_worker_time ON worker_heartbeats(worker_id, reported_at DESC);

-- ---------- Jobs ----------
-- One row per logical job. job_executions holds each attempt.

CREATE TABLE jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    job_type        TEXT NOT NULL,          -- e.g. "send_email", "resize_image"
    payload         JSONB NOT NULL DEFAULT '{}',
    status          TEXT NOT NULL DEFAULT 'queued'
                      CHECK (status IN ('queued','scheduled','claimed','running',
                                         'completed','failed','dead_letter','cancelled')),
    priority        INT NOT NULL DEFAULT 0,             -- per-job override, higher first
    idempotency_key TEXT,                               -- optional, for safe retries/dedup
    run_at          TIMESTAMPTZ NOT NULL DEFAULT now(),  -- when it becomes eligible to run (delayed/scheduled jobs)
    -- recurring (cron) support: non-null cron_expr marks this as a template row
    cron_expr       TEXT,
    batch_id        UUID,                                -- groups jobs submitted together
    max_retries     INT,                                 -- overrides queue default if set
    retry_strategy  TEXT CHECK (retry_strategy IN ('none','fixed','linear','exponential')),
    attempt_count   INT NOT NULL DEFAULT 0,
    claimed_by      UUID REFERENCES workers(id) ON DELETE SET NULL,
    claimed_at      TIMESTAMPTZ,
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The hot-path index: workers polling for eligible work.
-- Partial index keeps it tiny — only rows that are actually claimable.
CREATE INDEX idx_jobs_claimable
    ON jobs (queue_id, priority DESC, run_at)
    WHERE status IN ('queued','scheduled');

CREATE INDEX idx_jobs_status ON jobs(status);
CREATE INDEX idx_jobs_batch ON jobs(batch_id) WHERE batch_id IS NOT NULL;
CREATE INDEX idx_jobs_idempotency ON jobs(queue_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX idx_jobs_cron ON jobs(cron_expr) WHERE cron_expr IS NOT NULL;

-- ---------- Job Executions (one row per attempt) ----------

CREATE TABLE job_executions (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id        UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    attempt_number INT NOT NULL,
    worker_id     UUID REFERENCES workers(id) ON DELETE SET NULL,
    status        TEXT NOT NULL CHECK (status IN ('running','completed','failed','timed_out')),
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at   TIMESTAMPTZ,
    duration_ms   INT,
    error_message TEXT,
    UNIQUE (job_id, attempt_number)
);

CREATE INDEX idx_executions_job ON job_executions(job_id);
CREATE INDEX idx_executions_worker ON job_executions(worker_id);

-- ---------- Job Logs (fine-grained log lines per execution) ----------

CREATE TABLE job_logs (
    id           BIGSERIAL PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES job_executions(id) ON DELETE CASCADE,
    logged_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    level        TEXT NOT NULL DEFAULT 'info' CHECK (level IN ('debug','info','warn','error')),
    message      TEXT NOT NULL
);

CREATE INDEX idx_logs_execution ON job_logs(execution_id, logged_at);

-- ---------- Retry Policies (named, reusable policies) ----------
-- Queues/jobs above embed retry config inline for simplicity + fewer joins
-- on the hot path. This table is for named, reusable policies exposed via API.

CREATE TABLE retry_policies (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id      UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    strategy        TEXT NOT NULL CHECK (strategy IN ('none','fixed','linear','exponential')),
    max_retries     INT NOT NULL DEFAULT 3,
    base_delay_seconds INT NOT NULL DEFAULT 5,
    max_delay_seconds  INT NOT NULL DEFAULT 3600,
    UNIQUE (project_id, name)
);

-- ---------- Dead Letter Queue ----------

CREATE TABLE dead_letter_entries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    queue_id        UUID NOT NULL REFERENCES queues(id) ON DELETE CASCADE,
    final_error     TEXT,
    attempt_count   INT NOT NULL,
    moved_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    replayed_at     TIMESTAMPTZ,
    replayed_job_id UUID REFERENCES jobs(id)
);

CREATE INDEX idx_dlq_queue ON dead_letter_entries(queue_id);

-- ---------- updated_at trigger for jobs ----------

CREATE OR REPLACE FUNCTION set_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_jobs_updated_at
    BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

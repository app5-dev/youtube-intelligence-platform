
BEGIN;

CREATE SCHEMA IF NOT EXISTS ingestion;

-- 1. Registered YouTube channels
CREATE TABLE ingestion.channels (
    channel_id VARCHAR(100) PRIMARY KEY,
    handle VARCHAR(255) NOT NULL UNIQUE,
    category VARCHAR(100),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Pipeline execution history
CREATE TABLE ingestion.ingestion_runs (
    run_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    channel_id VARCHAR(100)
        REFERENCES ingestion.channels(channel_id),
    job_type VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    error_message TEXT,

    CONSTRAINT valid_run_status CHECK (
        status IN ('RUNNING', 'SUCCESS', 'FAILED')
    )
);

-- 3. Durable ingestion tasks
CREATE TABLE ingestion.ingestion_tasks (
    task_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    channel_id VARCHAR(100) NOT NULL
        REFERENCES ingestion.channels(channel_id),

    video_id VARCHAR(100) NOT NULL,
    operation VARCHAR(100) NOT NULL,
    scope_key VARCHAR(255) NOT NULL DEFAULT 'initial',

    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    retry_count INTEGER NOT NULL DEFAULT 0,

    claimed_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    error_message TEXT,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT valid_task_status CHECK (
        status IN (
            'PENDING',
            'PROCESSING',
            'COMPLETED',
            'FAILED'
        )
    ),

    CONSTRAINT valid_retry_count CHECK (retry_count >= 0),

    CONSTRAINT unique_ingestion_task UNIQUE (
        channel_id,
        video_id,
        operation,
        scope_key
    )
);

-- 4. Successfully ingested initial video metadata
CREATE TABLE ingestion.ingested_videos (
    channel_id VARCHAR(100) NOT NULL
        REFERENCES ingestion.channels(channel_id),

    video_id VARCHAR(100) NOT NULL,
    bronze_path TEXT NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (channel_id, video_id)
);

-- Indexes for workers and monitoring
CREATE INDEX idx_ingestion_tasks_pending
ON ingestion.ingestion_tasks (created_at, task_id)
WHERE status = 'PENDING';

CREATE INDEX idx_ingestion_tasks_leases
ON ingestion.ingestion_tasks (lease_expires_at)
WHERE status = 'PROCESSING';

CREATE INDEX idx_ingestion_runs_channel
ON ingestion.ingestion_runs (channel_id, started_at DESC);

COMMIT;

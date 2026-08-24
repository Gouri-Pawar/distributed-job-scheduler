package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/db"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/worker"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("db connect failed: %v", err)
	}
	defer pool.Close()

	projectID, err := uuid.Parse(mustEnv("PROJECT_ID"))
	if err != nil {
		log.Fatalf("invalid PROJECT_ID: %v", err)
	}

	// WORKER_QUEUES="queue-id-1,queue-id-2" — which queues this process polls.
	queueIDs, err := parseQueueIDs(mustEnv("WORKER_QUEUES"))
	if err != nil {
		log.Fatalf("invalid WORKER_QUEUES: %v", err)
	}

	concurrency := 5 // reasonable default; override via WORKER_CONCURRENCY if needed
	if v := os.Getenv("WORKER_CONCURRENCY"); v != "" {
		fmt.Sscanf(v, "%d", &concurrency)
	}

	engine := worker.NewEngine(pool, projectID, queueIDs, concurrency)

	// --- Register job_type -> handler mappings here. ---
	// This is where you'd plug in real business logic. Two illustrative
	// handlers are included so the system is runnable end-to-end out of
	// the box; add more RegisterHandler calls for real job types.
	engine.RegisterHandler("send_email", func(ctx context.Context, payload json.RawMessage) error {
		var p struct {
			To      string `json:"to"`
			Subject string `json:"subject"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("bad payload: %w", err)
		}
		log.Printf("[send_email] sending %q to %s", p.Subject, p.To)
		time.Sleep(200 * time.Millisecond) // simulate work
		return nil
	})

	engine.RegisterHandler("resize_image", func(ctx context.Context, payload json.RawMessage) error {
		var p struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(payload, &p); err != nil {
			return fmt.Errorf("bad payload: %w", err)
		}
		log.Printf("[resize_image] processing %s", p.URL)
		time.Sleep(300 * time.Millisecond)
		return nil
	})

	if err := engine.Run(ctx); err != nil {
		log.Fatalf("worker engine exited with error: %v", err)
	}
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("required env var %s is not set", key)
	}
	return v
}

func parseQueueIDs(csv string) ([]uuid.UUID, error) {
	parts := strings.Split(csv, ",")
	ids := make([]uuid.UUID, 0, len(parts))
	for _, p := range parts {
		id, err := uuid.Parse(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, nil
}

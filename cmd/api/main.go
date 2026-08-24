package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/db"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/handlers"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
)

func main() {
	ctx := context.Background()

	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("db connect failed: %v", err)
	}
	defer pool.Close()

	authH := &handlers.AuthHandler{Pool: pool}
	projectH := &handlers.ProjectHandler{Pool: pool}
	queueH := &handlers.QueueHandler{Pool: pool}
	jobH := &handlers.JobHandler{Pool: pool}
	workerH := &handlers.WorkerHandler{Pool: pool}
	dashboardH := &handlers.DashboardHandler{Pool: pool}

	mux := http.NewServeMux()

	// --- public ---
	mux.HandleFunc("POST /auth/register", authH.Register)
	mux.HandleFunc("POST /auth/login", authH.Login)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		middleware.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	// --- authenticated (Go 1.22+ mux supports method + path-param patterns) ---
	mux.HandleFunc("POST /projects", middleware.RequireAuth(projectH.Create))
	mux.HandleFunc("GET /projects", middleware.RequireAuth(projectH.List))

	mux.HandleFunc("POST /projects/{projectID}/queues", middleware.RequireAuth(queueH.Create))
	mux.HandleFunc("GET /projects/{projectID}/queues", middleware.RequireAuth(queueH.List))
	mux.HandleFunc("PATCH /queues/{queueID}/pause", middleware.RequireAuth(queueH.Pause))
	mux.HandleFunc("PATCH /queues/{queueID}/resume", middleware.RequireAuth(queueH.Resume))
	mux.HandleFunc("GET /queues/{queueID}/stats", middleware.RequireAuth(queueH.Stats))

	mux.HandleFunc("POST /queues/{queueID}/jobs", middleware.RequireAuth(jobH.Create))
	mux.HandleFunc("GET /queues/{queueID}/jobs", middleware.RequireAuth(jobH.List))
	mux.HandleFunc("GET /jobs/{jobID}", middleware.RequireAuth(jobH.Get))
	mux.HandleFunc("POST /jobs/{jobID}/retry", middleware.RequireAuth(jobH.Retry))

	mux.HandleFunc("GET /projects/{projectID}/workers", middleware.RequireAuth(workerH.List))
	mux.HandleFunc("GET /projects/{projectID}/dashboard", middleware.RequireAuth(dashboardH.Summary))

	// --- static dashboard frontend ---
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	handler := middleware.Logging(corsMiddleware(mux))

	addr := os.Getenv("PORT")
	if addr == "" {
		addr = "8080"
	}
	srv := &http.Server{Addr: ":" + addr, Handler: handler}

	go func() {
		log.Printf("API server listening on :%s", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	// Graceful shutdown on Ctrl+C / SIGTERM
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down API server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("forced shutdown: %v", err)
	}
}

// corsMiddleware allows the dashboard (served separately in dev, e.g. via
// a live-reload tool) to call the API from a different port/origin.
func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PATCH,DELETE,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

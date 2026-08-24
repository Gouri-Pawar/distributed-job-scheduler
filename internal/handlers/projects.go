package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
)

type ProjectHandler struct {
	Pool *pgxpool.Pool
}

type createProjectRequest struct {
	Name string `json:"name"`
}

// POST /projects
func (h *ProjectHandler) Create(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		middleware.WriteError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}

	var req createProjectRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" {
		middleware.WriteError(w, http.StatusBadRequest, "project name is required")
		return
	}

	row := h.Pool.QueryRow(r.Context(), `
		INSERT INTO projects (owner_id, name) VALUES ($1, $2)
		RETURNING id, owner_id, name, api_key, created_at`, userID, req.Name)

	var p struct {
		ID        string `json:"id"`
		OwnerID   string `json:"owner_id"`
		Name      string `json:"name"`
		APIKey    string `json:"api_key"`
		CreatedAt string `json:"created_at"`
	}
	if err := row.Scan(&p.ID, &p.OwnerID, &p.Name, &p.APIKey, &p.CreatedAt); err != nil {
		middleware.WriteError(w, http.StatusConflict, "could not create project (name may already exist for this user)")
		return
	}

	middleware.WriteJSON(w, http.StatusCreated, p)
}

// GET /projects
func (h *ProjectHandler) List(w http.ResponseWriter, r *http.Request) {
	userID, ok := middleware.UserIDFromContext(r.Context())
	if !ok {
		middleware.WriteError(w, http.StatusUnauthorized, "unauthenticated")
		return
	}

	rows, err := h.Pool.Query(r.Context(), `
		SELECT id, owner_id, name, api_key, created_at FROM projects
		WHERE owner_id = $1 ORDER BY created_at DESC`, userID)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not list projects")
		return
	}
	defer rows.Close()

	type project struct {
		ID        string `json:"id"`
		OwnerID   string `json:"owner_id"`
		Name      string `json:"name"`
		APIKey    string `json:"api_key"`
		CreatedAt string `json:"created_at"`
	}
	var out []project
	for rows.Next() {
		var p project
		if err := rows.Scan(&p.ID, &p.OwnerID, &p.Name, &p.APIKey, &p.CreatedAt); err != nil {
			middleware.WriteError(w, http.StatusInternalServerError, "scan error")
			return
		}
		out = append(out, p)
	}

	middleware.WriteJSON(w, http.StatusOK, out)
}

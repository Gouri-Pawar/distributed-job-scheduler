package handlers

import (
	"encoding/json"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/auth"
	"github.com/Gouri-Pawar/distributed-job-scheduler/internal/middleware"
)

type AuthHandler struct {
	Pool *pgxpool.Pool
}

type registerRequest struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

type authResponse struct {
	Token string `json:"token"`
}

// POST /auth/register
func (h *AuthHandler) Register(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.Email == "" || len(req.Password) < 8 {
		middleware.WriteError(w, http.StatusBadRequest, "email required, password must be >= 8 chars")
		return
	}

	hash, err := auth.HashPassword(req.Password)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not hash password")
		return
	}

	var userID string
	err = h.Pool.QueryRow(r.Context(), `
		INSERT INTO users (email, password_hash) VALUES ($1, $2)
		RETURNING id`, req.Email, hash).Scan(&userID)
	if err != nil {
		// Most common cause: duplicate email (UNIQUE constraint).
		middleware.WriteError(w, http.StatusConflict, "could not create user (email may already be registered)")
		return
	}

	uid, _ := parseUUID(userID)
	token, err := auth.GenerateToken(uid, req.Email)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not generate token")
		return
	}

	middleware.WriteJSON(w, http.StatusCreated, authResponse{Token: token})
}

// POST /auth/login
func (h *AuthHandler) Login(w http.ResponseWriter, r *http.Request) {
	var req registerRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		middleware.WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	var userID, hash string
	err := h.Pool.QueryRow(r.Context(), `
		SELECT id, password_hash FROM users WHERE email = $1`, req.Email).Scan(&userID, &hash)
	if err != nil {
		// Deliberately vague — don't reveal whether the email exists.
		middleware.WriteError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}

	if !auth.CheckPassword(hash, req.Password) {
		middleware.WriteError(w, http.StatusUnauthorized, "invalid email or password")
		return
	}

	uid, _ := parseUUID(userID)
	token, err := auth.GenerateToken(uid, req.Email)
	if err != nil {
		middleware.WriteError(w, http.StatusInternalServerError, "could not generate token")
		return
	}

	middleware.WriteJSON(w, http.StatusOK, authResponse{Token: token})
}

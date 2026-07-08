package httpapi

import (
	"encoding/json"
	"io"
	"net/http"

	"github.com/usmc/usmc-k8s-agent/internal/bridge"
	"github.com/usmc/usmc-k8s-agent/internal/command"
	"github.com/usmc/usmc-k8s-agent/internal/observability"
)

// MountInternalRoutes registers agent-service endpoints for in-cluster peers.
func MountInternalRoutes(mux *http.ServeMux, router *command.Router, state *observability.RuntimeState, internalToken string) {
	mux.HandleFunc("/internal/v1/commands", func(w http.ResponseWriter, r *http.Request) {
		handleInternalCommand(w, r, router, state, internalToken)
	})
}

func handleInternalCommand(w http.ResponseWriter, r *http.Request, router *command.Router, state *observability.RuntimeState, internalToken string) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	if !authorizeBearer(w, r, internalToken) {
		return
	}
	if !state.IsLeader() {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "not leader"})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "read body failed"})
		return
	}

	var req bridge.ExecuteRequest
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid JSON"})
		return
	}
	if req.Command == nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "command is required"})
		return
	}

	resp, err := router.Handle(r.Context(), req.Command, req.Meta)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	if resp == nil {
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "executing"})
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

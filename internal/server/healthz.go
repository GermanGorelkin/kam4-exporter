package server

import (
	"context"
	"fmt"
	"net/http"
)

func HandleHealthz(ctx context.Context) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"status": "ok"}`)
	})
}

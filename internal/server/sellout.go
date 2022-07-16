package server

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/germangorelkin/kam4-exporter/internal/service"
)

func HandleSellout(ctx context.Context, srv service.SelloutService) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, err := ioutil.ReadAll(r.Body)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		link, err := srv.HandleSelloutExport(ctx, b)
		if err != nil {
			http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
			return
		}

		fmt.Fprintf(w, link)
	})
}

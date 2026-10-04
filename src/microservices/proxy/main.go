package main

import (
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
)

func main() {
	monolith := newProxy(os.Getenv("MONOLITH_URL"))
	movies := newProxy(os.Getenv("MOVIES_SERVICE_URL"))
	events := newProxy(os.Getenv("EVENTS_SERVICE_URL"))

	gradual := os.Getenv("GRADUAL_MIGRATION") == "true"
	percent, err := strconv.Atoi(os.Getenv("MOVIES_MIGRATION_PERCENT"))
	if err != nil {
		log.Fatalf("invalid MOVIES_MIGRATION_PERCENT: %v", err)
	}

	moviesRoute := func(w http.ResponseWriter, r *http.Request) {
		if !gradual || rand.IntN(100) < percent {
			movies.ServeHTTP(w, r)
			return
		}
		monolith.ServeHTTP(w, r)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, "Strangler Fig Proxy is healthy")
	})
	mux.HandleFunc("/api/movies", moviesRoute)
	mux.HandleFunc("/api/movies/", moviesRoute)
	mux.Handle("/api/events/", events)
	mux.Handle("/", monolith)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8000"
	}
	log.Printf("Starting proxy service on port %s, gradual migration %t, movies migration %d%%", port, gradual, percent)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func newProxy(rawURL string) *httputil.ReverseProxy {
	target, err := url.Parse(rawURL)
	if err != nil {
		log.Fatalf("invalid service url %q: %v", rawURL, err)
	}
	return httputil.NewSingleHostReverseProxy(target)
}

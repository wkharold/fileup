package cmd

import (
	"log"
	"net/http"
	"os"
)

func Liveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func MustGetenv(name string) string {
	val := os.Getenv(name)
	if len(val) == 0 {
		log.Fatalf("%s must be set", name)
	}
	return val
}

func Readiness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

package cmd

import (
	"log"
	"net/http"
	"os"

	minio "github.com/minio/minio-go"
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

func Readiness(mc *minio.Client, bucket string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		if mc == nil {
			w.WriteHeader(http.StatusExpectationFailed)
			return
		}

		exists, err := mc.BucketExists(bucket)
		if err != nil || !exists {
			w.WriteHeader(http.StatusExpectationFailed)
			return
		}

		w.WriteHeader(http.StatusOK)
	}
}

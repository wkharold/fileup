package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
)

type Env struct {
	projectId      string
	serviceAccount string
	topic          string
}

var (
	ctx            = context.Background()
	filedir        = flag.String("filedir", "/tmp/files", "Directory for uploaded files")
	projectid      = flag.String("projectid", "", "Project Id of the project hosting the application (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")
	topic          = flag.String("topic", "", "PubSub topic for notifications (Required)")
)

func liveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func readiness(w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat(*filedir); err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func main() {
	flag.Parse()

	if len(*projectid) == 0 || len(*serviceaccount) == 0 || len(*topic) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	if _, err := os.Stat(*filedir); os.IsNotExist(err) {
		fmt.Printf("Creating uploaded files dir: %s\n", *filedir)
		err = os.Mkdir(*filedir, 0700)
		if err != nil {
			fmt.Sprintf("unable to create uploaded files dir: %+v\n", err)
			os.Exit(1)
		}
	}

	http.HandleFunc("/uploaded", uploaded)
	http.HandleFunc("/_alive", liveness)
	http.HandleFunc("/_ready", readiness)

	uploader, err := NewUploader(*projectid, *serviceaccount, *topic)
	if err != nil {
		fmt.Printf("uploader creation failed: %+v\n", err)
		os.Exit(1)
	}

	http.Handle("/upload", uploader)

	http.ListenAndServe(":8080", nil)
}

package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/logging"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/archiver"
)

const (
	accessKeyIdEnvVar     = "MINIO_ACCESSKEYID"
	bucketNameEnvVar      = "MINIO_BUCKET"
	secretAccessKeyEnvVar = "MINIO_SECRETKEY"

	location = "us-east-1"
	logname  = "archiver_log"
	noprefix = ""
)

var (
	ctx = context.Background()

	archivetopic   = flag.String("archivetopic", "", "PubSub topic for archive notifications (Required)")
	bucket         = flag.String("bucket", "", "Cloud storage archive bucket (Required)")
	filestore      = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	projectid      = flag.String("projectid", "", "Project Id of the project hosting the application (Required)")
	purgetopic     = flag.String("purgetopic", "", "PubSub topic for upload purge notifications (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")

	accessKeyId     = mustGetenv(accessKeyIdEnvVar)
	miniobucket     = mustGetenv(bucketNameEnvVar)
	secretAccessKey = mustGetenv(secretAccessKeyEnvVar)

	logger *logging.Logger
	mc     *minio.Client
)

func liveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func mustGetenv(name string) string {
	val := os.Getenv(name)
	if len(val) == 0 {
		log.Fatalf("%s must be set", name)
	}
	return val
}

func readiness(w http.ResponseWriter, r *http.Request) {
	if mc == nil {
		w.WriteHeader(http.StatusExpectationFailed)
		return
	}

	exists, err := mc.BucketExists(miniobucket)
	if err != nil || !exists {
		w.WriteHeader(http.StatusExpectationFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func main() {
	flag.Parse()

	if len(*archivetopic) == 0 || len(*bucket) == 0 || len(*filestore) == 0 || len(*projectid) == 0 || len(*purgetopic) == 0 || len(*serviceaccount) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	lc, err := logging.NewClient(ctx, *projectid)
	if err != nil {
		log.Fatalf("unable to create logging client: %+v\n", err)
	}
	defer lc.Close()

	lc.OnError = func(e error) {
		log.Printf("logging client error: %+v", e)
	}

	logger = lc.Logger(logname)

	mc, err = minio.New(*filestore, accessKeyId, secretAccessKey, false)
	if err != nil {
		log.Fatalf("unable to connect to file store: %+v\n", err)
	}

	archiver, err := archiver.New(logger, mc, *projectid, *serviceaccount, *bucket, *archivetopic, *purgetopic)
	if err != nil {
		log.Fatalf("recognizer creation failed [%+v]", err)
	}

	go func() {
		http.HandleFunc("/_alive", liveness)
		http.HandleFunc("/_ready", readiness)

		http.ListenAndServe(":8080", nil)
	}()

	archiver.ReceiveAndProcess(ctx)
}

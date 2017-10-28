package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/logging"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/recognizer"
)

const (
	accessKeyIdEnvVar     = "MINIO_ACCESSKEYID"
	bucketNameEnvVar      = "MINIO_BUCKET"
	secretAccessKeyEnvVar = "MINIO_SECRETKEY"

	location = "us-east-1"
	logname  = "recognizer_log"
	noprefix = ""
)

var (
	ctx = context.Background()

	filestore       = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	imagetopic      = flag.String("imagetopic", "image", "PubSub topic for new image notifications")
	projectid       = flag.String("projectid", "", "Project Id of the project hosting the application (Required)")
	purgetopic      = flag.String("purgetopic", "purge", "PubSub topic for purge notifications")
	recognizedtopic = flag.String("recognizedtopic", "", "PubSub topic for image recognition notification (Required)")
	serviceaccount  = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")
	targetlabel     = flag.String("targetlabel", "cat", "Target label for image recognition")

	accessKeyId     = mustGetenv(accessKeyIdEnvVar)
	bucket          = mustGetenv(bucketNameEnvVar)
	secretAccessKey = mustGetenv(secretAccessKeyEnvVar)

	logger *logging.Logger
	mc     *minio.Client
)

func main() {
	flag.Parse()

	if len(*filestore) == 0 || len(*projectid) == 0 || len(*recognizedtopic) == 0 || len(*serviceaccount) == 0 {
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

	recognizer, err := recognizer.New(logger, mc, *projectid, *serviceaccount, *imagetopic, *purgetopic, *recognizedtopic, *targetlabel)
	if err != nil {
		log.Fatalf("recognizer creation failed [%+v]", err)
	}

	go func() {
		http.HandleFunc("/_alive", liveness)
		http.HandleFunc("/_ready", readiness)

		http.ListenAndServe(":8080", nil)
	}()

	recognizer.ReceiveAndProcess(ctx)
}

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

	exists, err := mc.BucketExists(bucket)
	if err != nil || !exists {
		w.WriteHeader(http.StatusExpectationFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

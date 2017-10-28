package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/cmd"
	"github.com/wkharold/fileup/pkg/recognizer"
	"github.com/wkharold/fileup/pkg/sdlog"
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

	accessKeyId     = cmd.MustGetenv(accessKeyIdEnvVar)
	bucket          = cmd.MustGetenv(bucketNameEnvVar)
	secretAccessKey = cmd.MustGetenv(secretAccessKeyEnvVar)

	logger *sdlog.StackdriverLogger
	mc     *minio.Client
)

func main() {
	flag.Parse()

	if len(*filestore) == 0 || len(*projectid) == 0 || len(*recognizedtopic) == 0 || len(*serviceaccount) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	logger, err := sdlog.Logger(*projectid, logname)
	if err != nil {
		log.Fatalf("unable to create Stackdriver logger [%+v]", err)
	}

	mc, err = minio.New(*filestore, accessKeyId, secretAccessKey, false)
	if err != nil {
		log.Fatalf("unable to connect to file store: %+v\n", err)
	}

	recognizer, err := recognizer.New(logger, mc, *projectid, *serviceaccount, *imagetopic, *purgetopic, *recognizedtopic, *targetlabel)
	if err != nil {
		log.Fatalf("recognizer creation failed [%+v]", err)
	}

	go func() {
		http.HandleFunc("/_alive", cmd.Liveness)
		http.HandleFunc("/_ready", cmd.Readiness(mc, bucket))

		http.ListenAndServe(":8080", nil)
	}()

	recognizer.ReceiveAndProcess(ctx)
}

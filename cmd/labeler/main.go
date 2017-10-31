package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"

	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/cmd"
	"github.com/wkharold/fileup/pkg/labeler"
	"github.com/wkharold/fileup/pkg/sdlog"
)

const (
	accessKeyIdEnvVar     = "MINIO_ACCESSKEYID"
	bucketNameEnvVar      = "MINIO_BUCKET"
	secretAccessKeyEnvVar = "MINIO_SECRETKEY"

	location = "us-east-1"
	logname  = "labeler_log"
	noprefix = ""
)

var (
	ctx = context.Background()

	filestore      = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	imagetopic     = flag.String("imagetopic", "images", "PubSub topic for new image notifications")
	labeledtopic   = flag.String("labeledtopic", "labeled", "PubSub topic for new label notifications")
	projectid      = flag.String("projectid", "", "Project Id of the project hosting the application (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")

	accessKeyId     = cmd.MustGetenv(accessKeyIdEnvVar)
	bucket          = cmd.MustGetenv(bucketNameEnvVar)
	secretAccessKey = cmd.MustGetenv(secretAccessKeyEnvVar)

	logger *sdlog.StackdriverLogger
	mc     *minio.Client
)

func main() {
	flag.Parse()

	if len(*filestore) == 0 || len(*projectid) == 0 || len(*serviceaccount) == 0 {
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

	labeler, err := labeler.New(logger, mc, *projectid, *serviceaccount, *imagetopic, *labeledtopic)
	if err != nil {
		log.Fatalf("labeler creation failed [%+v]", err)
	}

	go func() {
		http.HandleFunc("/_alive", cmd.Liveness)
		http.HandleFunc("/_ready", cmd.Readiness)

		http.ListenAndServe(":8080", nil)
	}()

	labeler.ReceiveAndProcess(ctx)
}

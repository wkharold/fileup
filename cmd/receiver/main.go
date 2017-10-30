package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/cmd"
	"github.com/wkharold/fileup/pkg/receiver"
	"github.com/wkharold/fileup/pkg/sdlog"
)

type Env struct {
	projectId      string
	serviceAccount string
	topic          string
}

type FileDesc struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
}

const (
	accessKeyIdEnvVar     = "MINIO_ACCESSKEYID"
	bucketNameEnvVar      = "MINIO_BUCKET"
	secretAccessKeyEnvVar = "MINIO_SECRETKEY"

	location = "us-east-1"
	logname  = "receiver_log"
	noprefix = ""
)

var (
	ctx = context.Background()

	filestore      = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	projectid      = flag.String("projectid", "", "Project Id of the project hosting the application (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")
	topic          = flag.String("topic", "", "PubSub topic for notifications (Required)")

	accessKeyId     = cmd.MustGetenv(accessKeyIdEnvVar)
	bucket          = cmd.MustGetenv(bucketNameEnvVar)
	secretAccessKey = cmd.MustGetenv(secretAccessKeyEnvVar)

	logger *sdlog.StackdriverLogger
	mc     *minio.Client
)

func preStop(w http.ResponseWriter, r *http.Request) {
	done := make(chan struct{})
	defer close(done)

	for obj := range mc.ListObjectsV2(bucket, noprefix, true, done) {
		if obj.Err != nil {
			logger.LogError(fmt.Sprintf("Problem listing contents of bucket %s", bucket), obj.Err)
			continue
		}
		mc.RemoveObject(bucket, obj.Key)
	}

	if err := mc.RemoveBucket(bucket); err != nil {
		logger.LogError(fmt.Sprintf("Unable to remove local storage bucket %s", bucket), err)
	}
}

func main() {
	var err error

	flag.Parse()

	if len(*filestore) == 0 || len(*projectid) == 0 || len(*serviceaccount) == 0 || len(*topic) == 0 {
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

	exists, err := mc.BucketExists(bucket)
	if err != nil {
		log.Fatalf("file store access error: %s [%+v]\n", bucket, err)
	}

	if !exists {
		if err = mc.MakeBucket(bucket, location); err != nil {
			log.Fatalf("unable to create bucket: %+v\n", err)
		}
	}

	http.HandleFunc("/_prestop", preStop)
	http.HandleFunc("/_alive", cmd.Liveness)
	http.HandleFunc("/_ready", cmd.Readiness(mc, bucket))

	receiver, err := receiver.New(mc, bucket, logger, *projectid, *serviceaccount, *topic)
	if err != nil {
		log.Fatalf("receiver creation failed: %+v\n", err)
	}
	http.Handle("/receive", receiver)

	http.ListenAndServe(":8080", nil)
}

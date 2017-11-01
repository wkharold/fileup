// Implements the receiver microservice binary.
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

const (
	accessKeyIDEnvVar     = "MINIO_ACCESSKEYID"
	bucketNameEnvVar      = "MINIO_BUCKET"
	secretAccessKeyEnvVar = "MINIO_SECRETKEY"

	location = "us-east-1"
	logname  = "receiver_log"
	noprefix = ""
)

var (
	ctx = context.Background()

	filestore      = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	projectid      = flag.String("projectid", "", "Project ID of the project hosting the application (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")
	topic          = flag.String("topic", "", "PubSub topic for notifications (Required)")

	accessKeyID     = cmd.MustGetenv(accessKeyIDEnvVar)
	bucket          = cmd.MustGetenv(bucketNameEnvVar)
	secretAccessKey = cmd.MustGetenv(secretAccessKeyEnvVar)

	logger *sdlog.StackdriverLogger
	mc     *minio.Client
)

func prestop(w http.ResponseWriter, r *http.Request) {
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

	mc, err = minio.New(*filestore, accessKeyID, secretAccessKey, false)
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

	http.HandleFunc("/_prestop", prestop)
	http.HandleFunc("/_alive", cmd.Liveness)
	http.HandleFunc("/_ready", readiness)

	receiver, err := receiver.New(mc, bucket, logger, *projectid, *serviceaccount, *topic)
	if err != nil {
		log.Fatalf("receiver creation failed: %+v\n", err)
	}
	http.Handle("/receive", receiver)

	http.ListenAndServe(":8080", nil)
}

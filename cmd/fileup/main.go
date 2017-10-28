package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/cmd"
	"github.com/wkharold/fileup/pkg/sdlog"
	"github.com/wkharold/fileup/pkg/uploader"
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
	logname  = "fileup-log"
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

func uploaded(w http.ResponseWriter, r *http.Request) {
	done := make(chan struct{})
	defer close(done)

	result := []FileDesc{}

	objects := mc.ListObjectsV2(bucket, noprefix, true, done)
	for o := range objects {
		if o.Err != nil {
			logger.LogInfo(fmt.Sprintf("Problem listing contents of bucket %s [%+v]", bucket, o.Err))
			continue
		}

		fd := FileDesc{Name: o.Key, Size: o.Size}
		result = append(result, fd)
	}

	bs, err := json.Marshal(result)
	if err != nil {
		logger.LogError(fmt.Sprintf("Could not marshal bucket %s contents list", bucket), err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, string(bs))
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

	http.HandleFunc("/uploaded", uploaded)
	http.HandleFunc("/_alive", cmd.Liveness)
	http.HandleFunc("/_ready", cmd.Readiness(mc, bucket))

	uploader, err := uploader.New(mc, bucket, logger, *projectid, *serviceaccount, *topic)
	if err != nil {
		log.Fatalf("uploader creation failed: %+v\n", err)
	}
	http.Handle("/upload", uploader)

	http.ListenAndServe(":8080", nil)
}

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/logging"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/sdlog"
	"github.com/wkharold/fileup/uploader"
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

	accessKeyId     = mustGetenv(accessKeyIdEnvVar)
	bucket          = mustGetenv(bucketNameEnvVar)
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

	exists, err := mc.BucketExists(bucket)
	if err != nil || !exists {
		w.WriteHeader(http.StatusExpectationFailed)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func uploaded(w http.ResponseWriter, r *http.Request) {
	done := make(chan struct{})
	defer close(done)

	result := []FileDesc{}

	objects := mc.ListObjectsV2(bucket, noprefix, true, done)
	for o := range objects {
		if o.Err != nil {
			sdlog.LogInfo(logger, fmt.Sprintf("Problem listing contents of bucket %s [%+v]", bucket, o.Err))
			continue
		}

		fd := FileDesc{Name: o.Key, Size: o.Size}
		result = append(result, fd)
	}

	bs, err := json.Marshal(result)
	if err != nil {
		sdlog.LogError(logger, fmt.Sprintf("Could not marshal bucket %s contents list", bucket), err)
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
	http.HandleFunc("/_alive", liveness)
	http.HandleFunc("/_ready", readiness)

	uploader, err := uploader.New(mc, bucket, logger, *projectid, *serviceaccount, *topic)
	if err != nil {
		log.Fatalf("uploader creation failed: %+v\n", err)
	}

	http.Handle("/upload", uploader)

	http.ListenAndServe(":8080", nil)
}

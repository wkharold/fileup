package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"

	minio "github.com/minio/minio-go"
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
	noprefix = ""
)

var (
	ctx = context.Background()

	filestore      = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	projectid      = flag.String("projectid", "", "Project Id of the project hosting the application (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")
	topic          = flag.String("topic", "", "PubSub topic for notifications (Required)")

	accessKeyId     string
	bucket          string
	mc              *minio.Client
	secretAccessKey string
)

func liveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func mustGetenv(name string) string {
	val := os.Getenv(name)
	if len(val) == 0 {
		fmt.Printf("%s must be set", name)
		os.Exit(1)
	}
	return val
}

func readiness(w http.ResponseWriter, r *http.Request) {
	if mc == nil {
		fmt.Printf("minio client is nil")
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
			fmt.Printf("problem listing bucket contents: %+v\n", o.Err)
		} else {
			fd := FileDesc{Name: o.Key, Size: o.Size}
			result = append(result, fd)
		}
	}

	bs, err := json.Marshal(result)
	if err != nil {
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

	accessKeyId = mustGetenv(accessKeyIdEnvVar)
	bucket = mustGetenv(bucketNameEnvVar)
	secretAccessKey = mustGetenv(secretAccessKeyEnvVar)

	mc, err = minio.New(*filestore, accessKeyId, secretAccessKey, false)
	if err != nil {
		fmt.Printf("unable to connect to file store: %+v\n", err)
		os.Exit(1)
	}

	exists, err := mc.BucketExists(bucket)
	if err != nil {
		fmt.Printf("file store access error: %s [%+v]\n", bucket, err)
		os.Exit(1)
	}

	if !exists {
		if err = mc.MakeBucket(bucket, location); err != nil {
			fmt.Printf("unable to create bucket: %+v\n", err)
			os.Exit(1)
		}
	}

	http.HandleFunc("/uploaded", uploaded)
	http.HandleFunc("/_alive", liveness)
	http.HandleFunc("/_ready", readiness)

	uploader, err := uploader.New(mc, bucket, *projectid, *serviceaccount, *topic)
	if err != nil {
		fmt.Printf("uploader creation failed: %+v\n", err)
		os.Exit(1)
	}

	http.Handle("/upload", uploader)

	http.ListenAndServe(":8080", nil)
}

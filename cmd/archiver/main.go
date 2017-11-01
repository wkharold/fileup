// Implements the archiver microservice binary.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/archiver"
	"github.com/wkharold/fileup/pkg/cmd"
	"github.com/wkharold/fileup/pkg/satokensource"
	"github.com/wkharold/fileup/pkg/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

const (
	accessKeyIDEnvVar     = "MINIO_ACCESSKEYID"
	podNameEnvVar         = "POD_NAME"
	secretAccessKeyEnvVar = "MINIO_SECRETKEY"

	location = "us-east-1"
	logname  = "archiver_log"
	noprefix = ""
)

var (
	ctx = context.Background()

	labeledtopic   = flag.String("labeledtopic", "", "PubSub topic for labeled notifications (Required)")
	bucket         = flag.String("bucket", "", "Cloud storage archive bucket (Required)")
	filestore      = flag.String("filestore", "", "Endpoint for uploaded files (Required)")
	projectid      = flag.String("projectid", "", "Project ID of the project hosting the application (Required)")
	serviceaccount = flag.String("serviceaccount", "", "Service account to use of publishing (Required)")
	targetlabel    = flag.String("targetlabel", "cat", "Target label for images to archive")

	accessKeyID     = cmd.MustGetenv(accessKeyIDEnvVar)
	podName         = cmd.MustGetenv(podNameEnvVar)
	secretAccessKey = cmd.MustGetenv(secretAccessKeyEnvVar)

	logger      *sdlog.StackdriverLogger
	mc          *minio.Client
	subcription string
)

func prestop(w http.ResponseWriter, r *http.Request) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		logger.LogError("Unable to get application default client", err)
		return
	}

	ts := option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, *projectid, *serviceaccount)))

	pc, err := pubsub.NewClient(ctx, *projectid, ts)
	if err != nil {
		logger.LogError("Unable to create PubSub client", err)
		return
	}

	sub := pc.Subscription(subcription)

	ok, err := sub.Exists(ctx)
	if err != nil {
		logger.LogError(fmt.Sprintf("Unable to determine if subscription %s exists", subcription), err)
	} else if ok {
		if err = sub.Delete(ctx); err != nil {
			logger.LogError(fmt.Sprintf("Unable to delete subcription %s", subcription), err)
		}
	}
}

func main() {
	flag.Parse()

	if len(*bucket) == 0 || len(*filestore) == 0 || len(*projectid) == 0 || len(*labeledtopic) == 0 || len(*serviceaccount) == 0 || len(*targetlabel) == 0 {
		flag.PrintDefaults()
		os.Exit(1)
	}

	var err error

	logger, err = sdlog.Logger(*projectid, logname)
	if err != nil {
		log.Fatalf("unable to create Stackdriver logger [%+v]", err)
	}

	mc, err = minio.New(*filestore, accessKeyID, secretAccessKey, false)
	if err != nil {
		log.Fatalf("unable to connect to file store: %+v\n", err)
	}

	subcription = fmt.Sprintf("%s+%s", podName, *targetlabel)

	archiver, err := archiver.New(logger, mc, *projectid, *serviceaccount, *bucket, *labeledtopic, subcription, *targetlabel)
	if err != nil {
		log.Fatalf("recognizer creation failed [%+v]", err)
	}

	go func() {
		http.HandleFunc("/_alive", cmd.Liveness)
		http.HandleFunc("/_prestop", prestop)
		http.HandleFunc("/_ready", cmd.Readiness)

		http.ListenAndServe(":8080", nil)
	}()

	archiver.ReceiveAndProcess(ctx)
}

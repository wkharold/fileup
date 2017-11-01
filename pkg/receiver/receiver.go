// Package receiver provides the constructor and ServeHTTP method for the receiver microservice.
// The receiver microservice is responsible for accepting image files from clients, saving them in
// local (minio) object storage, and publishing an image receive message to its Google PubSub
// topic.
//
// Each receiver instance of the receiver microservice creates its own local (minio) object storage
// bucket. Old object are removed from the bucket every five minutes and the bucket is removed when
// the instance terminates.
package receiver

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"cloud.google.com/go/pubsub"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/satokensource"
	"github.com/wkharold/fileup/pkg/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

// A Receiver accepts image upload requests via HTTP. The images it
// receives are stored in a local object store bucke where they can be accessed
// by other services. A message is published on the receivers topic for
// every image received.
type Receiver struct {
	bucket string
	logger *sdlog.StackdriverLogger
	mc     *minio.Client
	topic  *pubsub.Topic
}

const (
	noprefix = ""
)

var (
	ctx = context.Background()
)

// purgeOldObjects deletes objects from the local store that are older than five minutes.
func purgeOldObjects(mc *minio.Client, logger *sdlog.StackdriverLogger, bucket string) {
	done := make(chan struct{})
	defer close(done)

	now := time.Now()

	log.Printf("Purging old objects @ %+v", now)

	for obj := range mc.ListObjectsV2(bucket, noprefix, true, done) {
		if obj.Err != nil {
			logger.LogError(fmt.Sprintf("Problem listing contents of bucket %s", bucket), obj.Err)
			continue
		}

		if now.Sub(obj.LastModified) > (time.Minute * 5) {
			if err := mc.RemoveObject(bucket, obj.Key); err != nil {
				logger.LogError(fmt.Sprintf("Unable to remove %s/%s from local storage", bucket, obj.Key), err)
			}
			log.Printf("removed %s/%s", bucket, obj.Key)
		}
	}
}

// New creates and initializes a Receiver. The receiver accepts image upload requests over HTTP and stores received
// images in a local object store (minio). It uses the specified serviceAccount to publish image received notifications
// to its pubsub topic.
func New(mc *minio.Client, bucket string, logger *sdlog.StackdriverLogger, projectID, serviceAccount, topic string) (*Receiver, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		log.Fatalf("unable to get application default credentials: %+v\n", err)
	}

	receiver := &Receiver{
		bucket: bucket,
		logger: logger,
		mc:     mc,
	}

	pc, err := pubsub.NewClient(ctx, projectID, option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectID, serviceAccount))))
	if err != nil {
		logger.LogError(fmt.Sprintf("Unable to create PubSub client for project: %s", projectID), err)
		return nil, err
	}

	receiver.topic = pc.Topic(topic)

	ok, err := receiver.topic.Exists(ctx)
	if err != nil {
		logger.LogError(fmt.Sprintf("Unable to determine if pubsub topic %s exists", topic), err)
		return nil, err
	}

	if !ok {
		receiver.topic, err = pc.CreateTopic(ctx, topic)
		if err != nil {
			logger.LogError(fmt.Sprintf("Unable to create pubsub topic %s exists", topic), err)
			return nil, err
		}
	}

	go func() {
		ticker := time.NewTicker(time.Minute * 5)
		for _ = range ticker.C {
			purgeOldObjects(mc, logger, bucket)
		}
	}()

	return receiver, nil
}

// ServeHTTP handles receiving the image file and writing it to the local (minio) object store.
// Once the file is saved in the local object store a message is published to the image topic.
func (r Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	file, header, err := req.FormFile("file")
	if err != nil {
		msg := fmt.Sprint("Unable to extract file contents from request")

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%s [%+v]", msg, err)
		return
	}
	defer file.Close()

	n, err := r.mc.PutObject(r.bucket, header.Filename, file, "application/octet-stream")
	if err != nil {
		msg := fmt.Sprintf("File upload failed")

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%s [%+v]", msg, err)
		return
	}
	if n != header.Size {
		msg := fmt.Sprintf("File upload incomplete")
		err = fmt.Errorf("wrote %d wanted %d", n, header.Size)

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%s [%+v]", msg, err)
		return
	}

	pr := r.topic.Publish(ctx, &pubsub.Message{Data: []byte(fmt.Sprintf("%s/%s", r.bucket, header.Filename))})
	id, err := pr.Get(ctx)
	if err != nil {
		msg := fmt.Sprintf("Received notifcation failed for topic %s", r.topic.ID())

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%s [%+v]", msg, err)
		return
	}

	r.logger.LogInfo(fmt.Sprintf("Published message id: %+v", id))

	fmt.Fprintf(w, "File %s uploaded successfully.\n", header.Filename)
}

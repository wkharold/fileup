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

func purgeOldObjects(mc *minio.Client, logger *sdlog.StackdriverLogger, bucket string) {
	done := make(chan struct{})
	defer close(done)

	now := time.Now()

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

func New(mc *minio.Client, bucket string, logger *sdlog.StackdriverLogger, projectId, serviceAccount, topic string) (*Receiver, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		log.Fatalf("unable to get application default credentials: %+v\n", err)
	}

	receiver := &Receiver{
		bucket: bucket,
		logger: logger,
		mc:     mc,
	}

	pc, err := pubsub.NewClient(ctx, projectId, option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount))))
	if err != nil {
		logger.LogError(fmt.Sprintf("Unable to create PubSub client for project: %s", projectId), err)
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

	go purgeOldObjects(mc, logger, bucket)

	return receiver, nil
}

func (r Receiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	file, header, err := req.FormFile("file")
	if err != nil {
		msg := fmt.Sprint("Unable to extract file contents from request")

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, "%s [%+v]", msg, err)
		return
	}
	defer file.Close()

	n, err := r.mc.PutObject(r.bucket, header.Filename, file, "application/octet-stream")
	if err != nil {
		msg := fmt.Sprintf("File upload failed")

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "%s [%+v]", msg, err)
		return
	}
	if n != header.Size {
		msg := fmt.Sprintf("File upload incomplete")
		err = fmt.Errorf("wrote %d wanted %d", n, header.Size)

		r.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "%s [%+v]", msg, err)
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

package receiver

import (
	"context"
	"fmt"
	"log"
	"net/http"

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

var (
	ctx = context.Background()
)

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

	return receiver, nil
}

func (ul Receiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	file, header, err := r.FormFile("file")
	if err != nil {
		msg := fmt.Sprint("Unable to extract file contents from request")

		ul.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, "%s [%+v]", msg, err)
		return
	}
	defer file.Close()

	n, err := ul.mc.PutObject(ul.bucket, header.Filename, file, "application/octet-stream")
	if err != nil {
		msg := fmt.Sprintf("File upload failed")

		ul.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "%s [%+v]", msg, err)
		return
	}
	if n != header.Size {
		msg := fmt.Sprintf("File upload incomplete")
		err = fmt.Errorf("wrote %d wanted %d", n, header.Size)

		ul.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "%s [%+v]", msg, err)
		return
	}

	pr := ul.topic.Publish(ctx, &pubsub.Message{Data: []byte(fmt.Sprintf("%s/%s", ul.bucket, header.Filename))})
	id, err := pr.Get(ctx)
	if err != nil {
		msg := fmt.Sprintf("Received notifcation failed for topic %s", ul.topic.ID())

		ul.logger.LogError(msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%s [%+v]", msg, err)
		// TODO: delete file
		return
	}

	ul.logger.LogInfo(fmt.Sprintf("Published message id: %+v", id))

	fmt.Fprintf(w, "File %s uploaded successfully.\n", header.Filename)
}

package uploader

import (
	"context"
	"fmt"
	"log"
	"net/http"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/satokensource"
	"github.com/wkharold/fileup/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type Uploader struct {
	bucket string
	logger *logging.Logger
	mc     *minio.Client
	topic  *pubsub.Topic
}

var (
	ctx = context.Background()
)

func New(mc *minio.Client, bucket string, logger *logging.Logger, projectId, serviceAccount, topic string) (*Uploader, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		log.Fatalf("unable to get application default credentials: %+v\n", err)
	}

	uploader := &Uploader{
		bucket: bucket,
		logger: logger,
		mc:     mc,
	}

	pc, err := pubsub.NewClient(ctx, projectId, option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount))))
	if err != nil {
		sdlog.LogError(logger, fmt.Sprintf("Unable to create PubSub client for project: %s", projectId), err)
		return nil, err
	}

	uploader.topic = pc.Topic(topic)

	ok, err := uploader.topic.Exists(ctx)
	if err != nil {
		sdlog.LogError(logger, fmt.Sprintf("Unable to determine if pubsub topic %s exists", topic), err)
		return nil, err
	}

	if !ok {
		uploader.topic, err = pc.CreateTopic(ctx, topic)
		if err != nil {
			sdlog.LogError(logger, fmt.Sprintf("Unable to create pubsub topic %s exists", topic), err)
			return nil, err
		}
	}

	return uploader, nil
}

func (ul Uploader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer ul.logger.Flush()

	file, header, err := r.FormFile("file")
	if err != nil {
		msg := fmt.Sprint("Unable to extract file contents from request")

		sdlog.LogError(ul.logger, msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, "%s [%+v]", msg, err)
		return
	}
	defer file.Close()

	n, err := ul.mc.PutObject(ul.bucket, header.Filename, file, "application/octet-stream")
	if err != nil {
		msg := fmt.Sprintf("File upload failed")

		sdlog.LogError(ul.logger, msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "%s [%+v]", msg, err)
		return
	}
	if n != header.Size {
		msg := fmt.Sprintf("File upload incomplete")
		err = fmt.Errorf("wrote %d wanted %d", n, header.Size)

		sdlog.LogError(ul.logger, msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, "%s [%+v]", msg, err)
		return
	}

	pr := ul.topic.Publish(ctx, &pubsub.Message{Data: []byte("testing")})
	id, err := pr.Get(ctx)
	if err != nil {
		msg := fmt.Sprintf("Upload notifcation failed for topic %s", ul.topic.ID())

		sdlog.LogError(ul.logger, msg, err)

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "%s [%+v]", msg, err)
		// TODO: delete file
		return
	}

	sdlog.LogInfo(ul.logger, fmt.Sprintf("Published message id: %+v", id))

	fmt.Fprintf(w, "File %s uploaded successfully.\n", header.Filename)
}

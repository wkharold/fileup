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
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type Uploader struct {
	bucket string
	client *pubsub.Client
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

	uploader := &Uploader{bucket: bucket, logger: logger, mc: mc}

	uploader.client, err = pubsub.NewClient(
		ctx,
		projectId,
		option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount))),
	)
	if err != nil {
		return nil, err
	}

	uploader.topic = uploader.client.Topic(topic)

	ok, err := uploader.topic.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		uploader.topic, err = uploader.client.CreateTopic(ctx, topic)
		if err != nil {
			return nil, err
		}
	}

	return uploader, nil
}

func (ul Uploader) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer ul.logger.Flush()

	file, header, err := r.FormFile("file")
	if err != nil {
		msg := fmt.Sprintf("Unable to extract file contents from request: %+v\n", err)

		ul.logger.Log(logging.Entry{
			Payload: struct {
				Message string
				Error   string
			}{
				Message: msg,
				Error:   err.Error(),
			},
			Severity: logging.Error,
		})

		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, msg)
		return
	}
	defer file.Close()

	n, err := ul.mc.PutObject(ul.bucket, header.Filename, file, "application/octet-stream")
	if err != nil {
		msg := fmt.Sprintf("File upload failed: %+v\n", err)

		ul.logger.Log(logging.Entry{
			Payload: struct {
				Message string
				Error   string
			}{
				Message: msg,
				Error:   err.Error(),
			},
			Severity: logging.Error,
		})

		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if n != header.Size {
		msg := fmt.Sprintf("File upload incomplete: wrote %d wanted %d\n", n, header.Size)

		ul.logger.Log(logging.Entry{
			Payload: struct {
				Message string
				Error   string
			}{
				Message: msg,
				Error:   err.Error(),
			},
			Severity: logging.Error,
		})

		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	pr := ul.topic.Publish(ctx, &pubsub.Message{Data: []byte("testing")})
	id, err := pr.Get(ctx)
	if err != nil {
		msg := fmt.Sprintf("Upload notification failed: %+v", err)

		ul.logger.Log(logging.Entry{
			Payload: struct {
				Message string
				Error   string
			}{
				Message: msg,
				Error:   err.Error(),
			},
			Severity: logging.Error,
		})

		w.WriteHeader(http.StatusInternalServerError)
		// TODO: delete file
		return
	}

	log.Printf("Published message id: %+v", id)

	ul.logger.Log(logging.Entry{
		Payload: struct {
			Message string
		}{
			Message: fmt.Sprintf("Published message id: %+v", id),
		},
		Severity: logging.Info,
	})

	fmt.Fprintf(w, "File %s uploaded successfully.\n", header.Filename)
}

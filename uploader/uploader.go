package uploader

import (
	"context"
	"fmt"
	"net/http"

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
	mc     *minio.Client
	topic  *pubsub.Topic
}

var (
	ctx = context.Background()
)

func New(mc *minio.Client, bucket, projectId, serviceAccount, topic string) (*Uploader, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		panic(fmt.Sprintf("unable to get application default credentials: %+v\n", err))
	}

	uploader := &Uploader{bucket: bucket, mc: mc}

	uploader.client, err = pubsub.NewClient(
		ctx,
		projectId,
		option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, projectId, serviceAccount))),
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
	file, header, err := r.FormFile("file")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintln(w, "Unable to extract file contents from request: %+v\n", err)
		return
	}
	defer file.Close()

	n, err := ul.mc.PutObject(ul.bucket, header.Filename, file, "application/octet-stream")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "File upload failed: %+v\n", err)
		return
	}
	if n != header.Size {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "File upload incomplete: wrote %d wanted %d\n", n, header.Size)
		return
	}

	pr := ul.topic.Publish(ctx, &pubsub.Message{Data: []byte("testing")})
	id, err := pr.Get(ctx)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Upload notification failed: %+v", err)
		// TODO: delete file
		return
	}
	fmt.Printf("published message id: %+v\n", id)

	fmt.Fprintf(w, "File %s uploaded successfully.\n", header.Filename)
}

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/pubsub"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type FileDesc struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mode string `json:"mode"`
}

type Uploader struct {
	client *pubsub.Client
	topic  *pubsub.Topic
}

func NewUploader(projectId, serviceAccount, topic string) (*Uploader, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		panic(fmt.Sprintf("unable to get application default credentials: %+v\n", err))
	}

	uploader := &Uploader{}

	uploader.client, err = pubsub.NewClient(
		ctx,
		projectId,
		option.WithTokenSource(oauth2.ReuseTokenSource(nil, &ServiceAccountTokenSource{client: client, projectId: projectId, serviceAccount: serviceAccount})),
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
		fmt.Fprintln(w, "Unable to extract file contents from request: +v", err)
		return
	}
	defer file.Close()

	filenm := strings.Join([]string{*filedir, header.Filename}, "/")
	out, err := os.Create(filenm)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprintf(w, "Failed to open the file for writing: %+v", err)
		return
	}
	defer out.Close()

	_, err = io.Copy(out, file)
	if err != nil {
		fmt.Fprintln(w, err)
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

	fmt.Fprintf(w, "File %s uploaded successfully.", header.Filename)
}

func uploaded(w http.ResponseWriter, r *http.Request) {
	files, err := ioutil.ReadDir(*filedir)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	result := []FileDesc{}

	for _, f := range files {
		fd := FileDesc{Name: f.Name(), Size: f.Size(), Mode: f.Mode().String()}
		result = append(result, fd)
	}

	bs, err := json.Marshal(result)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	fmt.Fprintln(w, string(bs))
}

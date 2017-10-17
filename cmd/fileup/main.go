package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type AccessTokenClaimSet struct {
	Iss   string `json:"iss"`
	Scope string `json:"scope"`
	Aud   string `json:"aud"`
	Exp   int64  `json:"exp"`
	Iat   int64  `json:"iat"`
}

type Env struct {
	projectId      string
	serviceAccount string
	topic          string
}

type FileDesc struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mode string `json:"mode"`
}

type ServiceAccountTokenSource struct {
	client *http.Client
	pid    string
	sa     string
}

const (
	projectid      string = "PROJECT_ID"
	serviceaccount string = "SERVICE_ACCOUNT"
	uploadTopic    string = "TOPIC"
)

var (
	ctx     = context.Background()
	filedir = flag.String("filedir", "/tmp/files", "Directory for uploaded files")
	env     *Env
	psc     *pubsub.Client
	topic   *pubsub.Topic
)

func init() {
	env = &Env{
		projectId:      os.Getenv(projectid),
		serviceAccount: os.Getenv(serviceaccount),
		topic:          os.Getenv(uploadTopic),
	}

	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		panic(fmt.Sprintf("unable to get application default credentials: %+v\n", err))
	}

	psc, err = pubsub.NewClient(
		ctx,
		env.projectId,
		option.WithTokenSource(oauth2.ReuseTokenSource(nil, &ServiceAccountTokenSource{client: client, pid: env.projectId, sa: env.serviceAccount})),
	)
	if err != nil {
		panic(fmt.Sprintf("pubsub client creation failed: %+v\n", err))
	}

	topic = psc.Topic(env.topic)

	ok, err := topic.Exists(ctx)
	if err != nil {
		panic(fmt.Sprintf("unable to determine if topic exists: %+v\n", err))
	}

	if !ok {
		topic, err = psc.CreateTopic(ctx, env.topic)
		if err != nil {
			panic(fmt.Sprintf("pubsub topic creation failed: %+v\n", err))
		}
	}
}

func liveness(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}

func readiness(w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat(*filedir); err != nil {
		w.WriteHeader(http.StatusExpectationFailed)
	} else {
		w.WriteHeader(http.StatusOK)
	}
}

func (ts ServiceAccountTokenSource) Token() (*oauth2.Token, error) {
	iamsvc, err := iam.New(ts.client)
	if err != nil {
		return nil, err
	}

	tnow := time.Now()

	claims := &AccessTokenClaimSet{
		Aud:   "https://www.googleapis.com/oauth2/v4/token",
		Exp:   tnow.Add(time.Duration(5) * time.Minute).Unix(),
		Iat:   tnow.Unix(),
		Iss:   env.serviceAccount,
		Scope: iam.CloudPlatformScope,
	}

	bs, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}

	psasvc := iam.NewProjectsServiceAccountsService(iamsvc)
	jwtsigner := psasvc.SignJwt(
		fmt.Sprintf("projects/%s/serviceAccounts/%s", env.projectId, env.serviceAccount),
		&iam.SignJwtRequest{Payload: string(bs)},
	)

	jwtsigner = jwtsigner.Context(ctx)

	signerresp, err := jwtsigner.Do()
	if err != nil {
		return nil, err
	}

	tokreq := fmt.Sprintf("grant_type=%s&assertion=%s", url.QueryEscape("urn:ietf:params:oauth:grant-type:jwt-bearer"), url.QueryEscape(signerresp.SignedJwt))

	httpclient := &http.Client{}
	req, err := http.NewRequest(
		"POST",
		"https://www.googleapis.com/oauth2/v4/token",
		strings.NewReader(tokreq),
	)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpclient.Do(req)
	if err != nil {
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var fields interface{}
	err = json.Unmarshal(body, &fields)
	if err != nil {
		return nil, err
	}

	acctok := fields.(map[string]interface{})["access_token"]

	return &oauth2.Token{AccessToken: acctok.(string)}, nil
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

func uploader(w http.ResponseWriter, r *http.Request) {
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

	pr := topic.Publish(ctx, &pubsub.Message{Data: []byte("testing")})
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

func main() {
	flag.Parse()

	if _, err := os.Stat(*filedir); os.IsNotExist(err) {
		fmt.Printf("Creating uploaded files dir: %s\n", *filedir)
		err = os.Mkdir(*filedir, 0700)
		if err != nil {
			panic(fmt.Sprintf("unable to create uploaded files dir: %+v\n", err))
		}
	}

	http.HandleFunc("/uploaded", uploaded)
	http.HandleFunc("/upload", uploader)
	http.HandleFunc("/_alive", liveness)
	http.HandleFunc("/_ready", readiness)

	http.ListenAndServe(":8080", nil)
}

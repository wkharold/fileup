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

type FileDesc struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mode string `json:"mode"`
}

const (
	projectid      string = "PROJECT_ID"
	serviceaccount string = "SERVICE_ACCOUNT"
	thetopic       string = "TOPIC"
)

var (
	filedir = flag.String("filedir", "/tmp/files", "Directory for uploaded files")
)

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

func uploader(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
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
	case "POST":
		file, header, err := r.FormFile("file")
		defer file.Close()

		if err != nil {
			fmt.Fprintln(w, err)
			return
		}

		filenm := strings.Join([]string{*filedir, header.Filename}, "/")
		out, err := os.Create(filenm)
		if err != nil {
			fmt.Fprintf(w, "Failed to open the file for writing")
			return
		}
		defer out.Close()

		_, err = io.Copy(out, file)
		if err != nil {
			fmt.Fprintln(w, err)
		}

		fmt.Fprintf(w, "File %s uploaded successfully.", header.Filename)
	default:
	}

}

func main() {
	projid := os.Getenv(projectid)
	svcacct := os.Getenv(serviceaccount)
	topic := os.Getenv(thetopic)

	flag.Parse()

	if _, err := os.Stat(*filedir); os.IsNotExist(err) {
		fmt.Printf("Creating uploaded files dir: %s\n", *filedir)
		err = os.Mkdir(*filedir, 0700)
		if err != nil {
			panic(fmt.Sprintf("unable to create uploaded files dir: %+v\n", err))
		}
	}

	ctx := context.Background()

	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		panic(fmt.Sprintf("unable to get application default credentials: %+v\n", err))
	}

	iamsvc, err := iam.New(client)
	if err != nil {
		panic(fmt.Sprintf("unable to create iam service: %+v\n", err))
	}

	tnow := time.Now()

	claims := &AccessTokenClaimSet{
		Aud:   "https://www.googleapis.com/oauth2/v4/token",
		Exp:   tnow.Add(time.Duration(5) * time.Minute).Unix(),
		Iat:   tnow.Unix(),
		Iss:   svcacct,
		Scope: iam.CloudPlatformScope,
	}

	bs, err := json.Marshal(claims)
	if err != nil {
		panic(fmt.Sprintf("unable to marshal access token claims: %+v\n", err))
	}

	psasvc := iam.NewProjectsServiceAccountsService(iamsvc)
	jwtsigner := psasvc.SignJwt(
		fmt.Sprintf("projects/%s/serviceAccounts/%s", projid, svcacct),
		&iam.SignJwtRequest{Payload: string(bs)},
	)

	jwtsigner = jwtsigner.Context(ctx)

	resp, err := jwtsigner.Do()
	if err != nil {
		panic(fmt.Sprintf("unable to sign JWT blob: %+v\n", err))
	}

	tokreq := fmt.Sprintf("grant_type=%s&assertion=%s", url.QueryEscape("urn:ietf:params:oauth:grant-type:jwt-bearer"), url.QueryEscape(resp.SignedJwt))

	httpclient := &http.Client{}
	req, err := http.NewRequest(
		"POST",
		"https://www.googleapis.com/oauth2/v4/token",
		strings.NewReader(tokreq),
	)
	if err != nil {
		panic(fmt.Sprintf("unable to create access token request: %+v\n", err))
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	acctok, err := httpclient.Do(req)
	if err != nil {
		panic(fmt.Sprintf("unable to retrieve access token: %+v\n", err))
	}

	body, err := ioutil.ReadAll(acctok.Body)
	if err != nil {
		panic(fmt.Sprintf("unable to retrieve access token body: %+v\n", err))
	}

	var tokfields interface{}
	err = json.Unmarshal(body, &tokfields)
	if err != nil {
		panic(fmt.Sprintf("unmarshaling the access token failed: %+v\n", err))
	}

	tok := tokfields.(map[string]interface{})["access_token"]

	tk := &oauth2.Token{AccessToken: tok.(string)}

	pc, err := pubsub.NewClient(ctx, projid, option.WithTokenSource(oauth2.StaticTokenSource(tk)))
	if err != nil {
		panic(fmt.Sprintf("pubsub client creation failed: %+v\n", err))
	}

	t := pc.Topic(topic)

	ok, err := t.Exists(ctx)
	if err != nil {
		panic(fmt.Sprintf("unable to determine if topic exists: %+v\n", err))
	}

	if !ok {
		t, err = pc.CreateTopic(ctx, topic)
		if err != nil {
			panic(fmt.Sprintf("pubsub topic creation failed: %+v\n", err))
		}
	}

	r := t.Publish(ctx, &pubsub.Message{Data: []byte("testing")})
	id, err := r.Get(ctx)
	if err != nil {
		panic(fmt.Sprintf("unable to publish: %+v\n", err))
	}
	fmt.Printf("published message id: %+v\n", id)

	http.HandleFunc("/upload", uploader)
	http.HandleFunc("/_alive", liveness)
	http.HandleFunc("/_ready", readiness)

	http.ListenAndServe(":8080", nil)
}

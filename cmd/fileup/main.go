package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"time"

	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
)

type AccessTokenClaimSet struct {
	Iss   string `json:"iss"`
	Scope string `json:"scope"`
	Aud   string `json:"aud"`
	Exp   string `json:"exp"`
	Iat   string `json:"iat"`
}

type FileDesc struct {
	Name string `json:"name"`
	Size int64  `json:"size"`
	Mode string `json:"mode"`
}

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
	fmt.Printf("obtained HTTP client with application default credentials: %+v\n", client)

	iamsvc, err := iam.New(client)
	if err != nil {
		panic(fmt.Sprintf("unable to create iam service: %+v\n", err))
	}
	fmt.Printf("created iam service with application default credentials: %+v\n", iamsvc)

	claims := &AccessTokenClaimSet{
		Aud:   "https://accounts.google.com/o/oauth2/token",
		Exp:   time.Now().Add(time.Duration(24) * time.Hour).String(),
		Iat:   time.Now().String(),
		Iss:   "raincreek-fileup-web@raincreek-trading-production.iam.gserviceaccount.com",
		Scope: iam.CloudPlatformScope + " https://www.googleapis.com/auth/iam",
	}

	bs, err := json.Marshal(claims)
	if err != nil {
		panic(fmt.Sprintf("unable to marshal access token claims: %+v\n", err))
	}

	fmt.Println(string(bs))

	psasvc := iam.NewProjectsServiceAccountsService(iamsvc)
	blobsigner := psasvc.SignBlob(
		"projects/raincreek-trading-production/serviceAccounts/raincreek-fileup-web@raincreek-trading-production.iam.gserviceaccount.com",
		&iam.SignBlobRequest{BytesToSign: base64.StdEncoding.EncodeToString(bs)},
	)

	blobsigner = blobsigner.Context(ctx)

	resp, err := blobsigner.Do()
	if err != nil {
		panic(fmt.Sprintf("unable to sign JWT blob: %+v\n", err))
	}
	fmt.Printf("signed the JWT blob: %+v\n", resp)

	http.HandleFunc("/upload", uploader)
	http.HandleFunc("/_alive", liveness)
	http.HandleFunc("/_ready", readiness)

	http.ListenAndServe(":8080", nil)
}

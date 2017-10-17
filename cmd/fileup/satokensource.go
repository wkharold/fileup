package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"golang.org/x/oauth2"
	iam "google.golang.org/api/iam/v1"
)

type AccessTokenClaimSet struct {
	Iss   string `json:"iss"`
	Scope string `json:"scope"`
	Aud   string `json:"aud"`
	Exp   int64  `json:"exp"`
	Iat   int64  `json:"iat"`
}

type ServiceAccountTokenSource struct {
	client         *http.Client
	projectId      string
	serviceAccount string
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
		Iss:   ts.serviceAccount,
		Scope: iam.CloudPlatformScope,
	}

	bs, err := json.Marshal(claims)
	if err != nil {
		return nil, err
	}

	psasvc := iam.NewProjectsServiceAccountsService(iamsvc)
	jwtsigner := psasvc.SignJwt(
		fmt.Sprintf("projects/%s/serviceAccounts/%s", ts.projectId, ts.serviceAccount),
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

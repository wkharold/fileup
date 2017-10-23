package satokensource

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"github.com/wkharold/fileup/sdlog"
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
	logger         *logging.Logger
	projectId      string
	serviceAccount string
}

const (
	accessTokenTTL = 59
)

var (
	ctx = context.Background()
)

func New(client *http.Client, logger *logging.Logger, projectId, serviceAccount string) *ServiceAccountTokenSource {
	return &ServiceAccountTokenSource{
		client:         client,
		logger:         logger,
		projectId:      projectId,
		serviceAccount: serviceAccount,
	}
}

func (ts ServiceAccountTokenSource) Token() (*oauth2.Token, error) {
	iamsvc, err := iam.New(ts.client)
	if err != nil {
		sdlog.LogError(ts.logger, fmt.Sprintf("Unable to create IAM service using %+v", ts.client), err)
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
		sdlog.LogError(ts.logger, fmt.Sprintf("JSON marshalling failed for: %+v", claims), err)
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
		sdlog.LogError(ts.logger, fmt.Sprintf("Failed to sign JWT for %s", ts.serviceAccount), err)
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
		sdlog.LogError(ts.logger, fmt.Sprint("Unable to create POST request for OAuth2 access token"), err)
		return nil, err
	}

	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	resp, err := httpclient.Do(req)
	if err != nil {
		sdlog.LogError(ts.logger, fmt.Sprint("OAuth2 access token request failed"), err)
		return nil, err
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		sdlog.LogError(ts.logger, fmt.Sprint("Unable to read body of response to OAuth2 access token request"), err)
		return nil, err
	}

	var fields interface{}
	err = json.Unmarshal(body, &fields)
	if err != nil {
		sdlog.LogError(ts.logger, fmt.Sprint("Unable to unmarshal the fields of the response to OAuth2 access token request"), err)
		return nil, err
	}

	acctok := fields.(map[string]interface{})["access_token"]
	if acctok == nil || len(acctok.(string)) == 0 {
		err = fmt.Errorf("empty access token field")
		sdlog.LogError(ts.logger, fmt.Sprint("OAuth2 access token is missing"), err)
		return nil, err
	}

	sdlog.LogInfo(ts.logger, fmt.Sprintf("Received an OAuth2 access token for: %s", ts.serviceAccount))

	return &oauth2.Token{AccessToken: acctok.(string), Expiry: time.Now().Add(time.Duration(accessTokenTTL) * time.Minute)}, nil
}

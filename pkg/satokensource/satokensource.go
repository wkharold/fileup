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

	"github.com/wkharold/fileup/pkg/sdlog"
	"golang.org/x/oauth2"
	iam "google.golang.org/api/iam/v1"
)

type accessTokenClaimSet struct {
	Iss   string `json:"iss"`
	Scope string `json:"scope"`
	Aud   string `json:"aud"`
	Exp   int64  `json:"exp"`
	Iat   int64  `json:"iat"`
}

// ServiceAccountTokenSource returns access tokens for the associated service account.
type ServiceAccountTokenSource struct {
	client         *http.Client
	logger         *sdlog.StackdriverLogger
	projectID      string
	serviceAccount string
}

const (
	accessTokenTTL = 59
	emptyRequest   = ""
)

var (
	ctx = context.Background()
)

// New creates and initializes a new ServiceAccountTokenSource.
func New(client *http.Client, logger *sdlog.StackdriverLogger, projectID, serviceAccount string) *ServiceAccountTokenSource {
	return &ServiceAccountTokenSource{
		client:         client,
		logger:         logger,
		projectID:      projectID,
		serviceAccount: serviceAccount,
	}
}

func createTokenRequest(client *http.Client, pid, sa string) (string, error) {
	iamsvc, err := iam.New(client)
	if err != nil {
		return emptyRequest, err
	}

	tnow := time.Now()

	claims := &accessTokenClaimSet{
		Aud:   "https://www.googleapis.com/oauth2/v4/token",
		Exp:   tnow.Add(time.Duration(5) * time.Minute).Unix(),
		Iat:   tnow.Unix(),
		Iss:   sa,
		Scope: iam.CloudPlatformScope,
	}

	bs, err := json.Marshal(claims)
	if err != nil {
		return emptyRequest, err
	}

	psasvc := iam.NewProjectsServiceAccountsService(iamsvc)
	jwtsigner := psasvc.SignJwt(
		fmt.Sprintf("projects/%s/serviceAccounts/%s", pid, sa),
		&iam.SignJwtRequest{Payload: string(bs)},
	)

	jwtsigner = jwtsigner.Context(ctx)

	signerresp, err := jwtsigner.Do()
	if err != nil {
		return emptyRequest, err
	}

	return fmt.Sprintf("grant_type=%s&assertion=%s", url.QueryEscape("urn:ietf:params:oauth:grant-type:jwt-bearer"), url.QueryEscape(signerresp.SignedJwt)), nil
}

func requestAccessToken(tokreq string) (*oauth2.Token, error) {
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
	if acctok == nil || len(acctok.(string)) == 0 {
		return nil, fmt.Errorf("empty access token field")
	}

	return &oauth2.Token{AccessToken: acctok.(string), Expiry: time.Now().Add(time.Duration(accessTokenTTL) * time.Minute)}, nil
}

// Token returns an OAuth2 access token for the service account associated with this token source.
func (ts ServiceAccountTokenSource) Token() (*oauth2.Token, error) {
	tokreq, err := createTokenRequest(ts.client, ts.projectID, ts.serviceAccount)
	if err != nil {
		ts.logger.LogError("Access token reqest creation failed", err)
		return nil, err
	}

	tok, err := requestAccessToken(tokreq)
	if err != nil {
		ts.logger.LogError(fmt.Sprint("Access token request failed"), err)
		return nil, err
	}

	ts.logger.LogInfo(fmt.Sprintf("Retrieved an OAuth2 access token for: %s", ts.serviceAccount))

	return tok, nil
}

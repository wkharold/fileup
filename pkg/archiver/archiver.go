package archiver

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/satokensource"
	"github.com/wkharold/fileup/pkg/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type Archiver struct {
	bucket string
	label  string
	logger *sdlog.StackdriverLogger
	mc     *minio.Client
	sc     *storage.Client
	sub    *pubsub.Subscription
}

var (
	ctx = context.Background()
)

func New(logger *sdlog.StackdriverLogger, mc *minio.Client, projectId, serviceAccount, bucket, labeledTopic, subcription, targetlabel string) (*Archiver, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		return nil, err
	}

	archiver := &Archiver{
		bucket: bucket,
		label:  targetlabel,
		logger: logger,
		mc:     mc,
	}

	ts := option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount)))

	archiver.sc, err = storage.NewClient(ctx, ts)
	if err != nil {
		return nil, err
	}

	pc, err := pubsub.NewClient(ctx, projectId, ts)
	if err != nil {
		return nil, err
	}

	if archiver.sub, err = subscribe(pc, subcription, labeledTopic); err != nil {
		return nil, err
	}

	return archiver, nil
}

func (a Archiver) ReceiveAndProcess(ctx context.Context) {
	err := a.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		log.Printf("message data: %+v", string(m.Data))
		defer m.Ack()

		bucket, object, labels, err := parseMessage(m.Data)
		if err != nil {
			a.logger.LogError("Bad message", err)
			return
		}

		for _, label := range labels {
			if strings.Contains(label, a.label) {
				if err := writeToCloud(a.mc, a.sc, a.logger, a.bucket, bucket, object); err != nil {
					a.logger.LogError("Cloud write failed", err)
				}
				return
			}
		}
	})
	if err != context.Canceled {
		a.logger.LogError(fmt.Sprintf("Unable to receive from %s", a.sub.ID()), err)
	}
}

func parseMessage(msg []byte) (string, string, []string, error) {
	var df interface{}
	err := json.Unmarshal(msg, &df)
	if err != nil {
		return "", "", []string{}, err
	}

	location := df.(map[string]interface{})["Location"]
	if location == nil || len(location.(string)) == 0 {
		return "", "", []string{}, fmt.Errorf("empty location field")
	}

	labels := df.(map[string]interface{})["Labels"]
	if labels == nil || len(labels.([]interface{})) == 0 {
		return "", "", []string{}, fmt.Errorf("empty labels field")
	}

	locparts := strings.Split(location.(string), "/")
	if len(locparts) != 2 {
		return "", "", []string{}, fmt.Errorf("location must have format <bucket>/<object> [%s]", location.(string))
	}

	ls := []string{}
	for _, l := range labels.([]interface{}) {
		ls = append(ls, l.(string))
	}

	return locparts[0], locparts[1], ls, nil
}

func subscribe(pc *pubsub.Client, subcription, topic string) (*pubsub.Subscription, error) {
	sub := pc.Subscription(subcription)

	ok, err := sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		sub, err = pc.CreateSubscription(ctx, subcription, pubsub.SubscriptionConfig{
			Topic:       pc.Topic(topic),
			AckDeadline: 60 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}

	return sub, nil
}

func writeToCloud(mc *minio.Client, sc *storage.Client, logger *sdlog.StackdriverLogger, cb, lb, o string) error {
	wc := sc.Bucket(cb).Object(o).NewWriter(ctx)
	wc.ContentType = "application/octet-stream"

	obj, err := mc.GetObject(lb, o)
	if err != nil {
		return err
	}

	bs, err := ioutil.ReadAll(obj)
	if err != nil {
		return err
	}

	if _, err := wc.Write(bs); err != nil {
		return err
	}

	if err = wc.Close(); err != nil {
		return err
	}

	return nil
}

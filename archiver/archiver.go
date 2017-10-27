package archiver

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/storage"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/satokensource"
	"github.com/wkharold/fileup/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type Archiver struct {
	bucket string
	logger *logging.Logger
	mc     *minio.Client
	pt     *pubsub.Topic
	sc     *storage.Client
	sub    *pubsub.Subscription
}

var (
	ctx = context.Background()
)

func New(logger *logging.Logger, mc *minio.Client, projectId, serviceAccount, bucket, archivetopic, purgetopic string) (*Archiver, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		return nil, err
	}

	archiver := &Archiver{
		bucket: bucket,
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

	archiver.pt = pc.Topic(purgetopic)

	if archiver.sub, err = subscribe(pc, projectId, archivetopic); err != nil {
		return nil, err
	}

	return archiver, nil
}

func (a Archiver) ReceiveAndProcess(ctx context.Context) {
	log.Printf("Receive and process PubSub messages")

	err := a.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		defer m.Ack()

		mparts := strings.Split(string(m.Data), "/")
		if len(mparts) != 2 {
			sdlog.LogError(a.logger, "Bad message", fmt.Errorf("Message must have format <bucket/image> [%s]", string(m.Data)))
			return
		}

		log.Printf("Upload %s/%s to %s/%s", mparts[0], mparts[1], a.bucket, mparts[1])

		wc := a.sc.Bucket(a.bucket).Object(mparts[1]).NewWriter(ctx)
		wc.ContentType = "application/octet-stream"

		obj, err := a.mc.GetObject(mparts[0], mparts[1])
		if err != nil {
			sdlog.LogError(a.logger, fmt.Sprintf("Unable to get object from local store: %s/%s", mparts[0], mparts[1]), err)
			return
		}

		bs, err := ioutil.ReadAll(obj)
		if err != nil {
			sdlog.LogError(a.logger, fmt.Sprintf("Unable to read object from local store: %s/%s", mparts[0], mparts[1]), err)
			return
		}

		if _, err := wc.Write(bs); err != nil {
			sdlog.LogError(a.logger, fmt.Sprintf("Unable to write: %s/%s", a.bucket, mparts[1]), err)
			return
		}

		if err = wc.Close(); err != nil {
			sdlog.LogError(a.logger, "Write failure", err)
			return
		}

		log.Printf("Request purging of %s/%s", mparts[0], mparts[1])

		msg := &pubsub.Message{Data: []byte(fmt.Sprintf("%s/%s", mparts[0], mparts[1]))}

		pr := a.pt.Publish(ctx, msg)
		if _, err = pr.Get(ctx); err != nil {
			sdlog.LogError(a.logger, fmt.Sprintf("Could not send purge notification: %s", string(msg.Data)), err)
			return
		}
	})
	if err != context.Canceled {
		sdlog.LogError(a.logger, fmt.Sprintf("Unable to receive from %s", a.sub.ID()), err)
	}
}

func subscribe(pc *pubsub.Client, pid, topic string) (*pubsub.Subscription, error) {
	sid := fmt.Sprintf("%s%%%s", pid, topic)

	sub := pc.Subscription(sid)

	ok, err := sub.Exists(ctx)
	if err != nil {
		return nil, fmt.Errorf("Unable to determine if subscription exists: %s [%+v]", sid, err)
	}

	if !ok {
		sub, err = pc.CreateSubscription(ctx, sid, pubsub.SubscriptionConfig{
			Topic:       pc.Topic(topic),
			AckDeadline: 60 * time.Second,
		})
		if err != nil {
			return nil, fmt.Errorf("Subscription creation failed: %s [%v]", sid, err)
		}
	}

	return sub, nil
}

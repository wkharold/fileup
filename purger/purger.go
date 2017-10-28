package purger

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/satokensource"
	"github.com/wkharold/fileup/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
)

type Purger struct {
	logger *logging.Logger
	mc     *minio.Client
	sub    *pubsub.Subscription
}

var (
	ctx = context.Background()
)

func New(logger *logging.Logger, mc *minio.Client, projectId, serviceAccount, purgeTopic string) (*Purger, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		return nil, err
	}

	purger := &Purger{
		logger: logger,
		mc:     mc,
	}

	ts := option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount)))

	pc, err := pubsub.NewClient(ctx, projectId, ts)
	if err != nil {
		return nil, err
	}

	sid := fmt.Sprintf("%s%%%s", projectId, purgeTopic)

	purger.sub = pc.Subscription(sid)

	ok, err := purger.sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		purger.sub, err = pc.CreateSubscription(ctx, sid, pubsub.SubscriptionConfig{
			Topic:       pc.Topic(purgeTopic),
			AckDeadline: 60 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}

	return purger, nil
}

func (p Purger) ReceiveAndProcess(ctx context.Context) {
	err := p.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		defer m.Ack()

		mparts := strings.Split(string(m.Data), "/")
		if len(mparts) != 2 {
			sdlog.LogError(p.logger, "Bad message", fmt.Errorf("Message must have format <bucket>/<image> [%s]", string(m.Data)))
			return
		}

		if err := p.mc.RemoveObject(mparts[0], mparts[1]); err != nil {
			sdlog.LogError(p.logger, fmt.Sprintf("Could not remove local image %s", string(m.Data)), err)
			return
		}

		sdlog.LogInfo(p.logger, fmt.Sprintf("Removed local image %s", string(m.Data)))
	})
	if err != context.Canceled {
		sdlog.LogError(p.logger, fmt.Sprintf("Unable to receive from %s", p.sub.ID()), err)
	}
}

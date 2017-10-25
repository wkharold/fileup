package recognizer

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"cloud.google.com/go/logging"
	"cloud.google.com/go/pubsub"
	vision "cloud.google.com/go/vision/apiv1"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/satokensource"
	"github.com/wkharold/fileup/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	vpb "google.golang.org/genproto/googleapis/cloud/vision/v1"
)

type Recognizer struct {
	logger *logging.Logger
	iac    *vision.ImageAnnotatorClient
	mc     *minio.Client
	sub    *pubsub.Subscription
}

var (
	ctx = context.Background()
)

func New(logger *logging.Logger, mc *minio.Client, projectId, serviceAccount, topic string) (*Recognizer, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		return nil, err
	}

	recognizer := &Recognizer{
		logger: logger,
		mc:     mc,
	}

	ts := option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount)))

	recognizer.iac, err = vision.NewImageAnnotatorClient(ctx, ts)
	if err != nil {
		return nil, err
	}

	pc, err := pubsub.NewClient(ctx, projectId, ts)
	if err != nil {
		return nil, err
	}

	t := pc.Topic(topic)

	sid := fmt.Sprintf("%s%%%s", projectId, topic)

	recognizer.sub = pc.Subscription(sid)

	ok, err := recognizer.sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		recognizer.sub, err = pc.CreateSubscription(ctx, sid, pubsub.SubscriptionConfig{
			Topic:       t,
			AckDeadline: 60 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}

	return recognizer, nil
}

func (r Recognizer) ReceiveAndProcess(ctx context.Context) {
	err := r.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		defer m.Ack()

		mparts := strings.Split(string(m.Data), "/")

		obj, err := r.mc.GetObject(mparts[0], mparts[1])
		if err != nil {
			sdlog.LogError(r.logger, fmt.Sprintf("Unable to retrieve %s", string(m.Data)), err)
			return
		}

		img, err := vision.NewImageFromReader(obj)
		if err != nil {
			sdlog.LogError(r.logger, fmt.Sprintf("Unable to read %s", string(m.Data)), err)
			return
		}

		res, err := r.iac.AnnotateImage(ctx, &vpb.AnnotateImageRequest{
			Image: img,
			Features: []*vpb.Feature{
				{Type: vpb.Feature_LABEL_DETECTION, MaxResults: 3},
			},
		})
		if err != nil {
			sdlog.LogError(r.logger, fmt.Sprintf("Unable to annotate image %s", string(m.Data)), err)
			return
		}

		log.Printf("Annotation result for %s: %+v", string(m.Data), res)
	})
	if err != context.Canceled {
		sdlog.LogError(r.logger, fmt.Sprintf("Unable to receive from %s", r.sub.ID()), err)
	}
}

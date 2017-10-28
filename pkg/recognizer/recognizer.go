package recognizer

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	vision "cloud.google.com/go/vision/apiv1"
	minio "github.com/minio/minio-go"
	"github.com/wkharold/fileup/pkg/satokensource"
	"github.com/wkharold/fileup/pkg/sdlog"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	iam "google.golang.org/api/iam/v1"
	"google.golang.org/api/option"
	vpb "google.golang.org/genproto/googleapis/cloud/vision/v1"
)

type Recognizer struct {
	logger *sdlog.StackdriverLogger
	iac    *vision.ImageAnnotatorClient
	mc     *minio.Client
	pc     *pubsub.Client
	pt     string
	rt     string
	sub    *pubsub.Subscription
	tl     string
}

var (
	ctx = context.Background()
)

func New(logger *sdlog.StackdriverLogger, mc *minio.Client, projectId, serviceAccount, imageTopic, purgeTopic, recognizedTopic, targetLabel string) (*Recognizer, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		return nil, err
	}

	recognizer := &Recognizer{
		logger: logger,
		mc:     mc,
		pt:     purgeTopic,
		rt:     recognizedTopic,
		tl:     targetLabel,
	}

	ts := option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectId, serviceAccount)))

	recognizer.iac, err = vision.NewImageAnnotatorClient(ctx, ts)
	if err != nil {
		return nil, err
	}

	recognizer.pc, err = pubsub.NewClient(ctx, projectId, ts)
	if err != nil {
		return nil, err
	}

	sid := fmt.Sprintf("%s%%%s", projectId, imageTopic)

	recognizer.sub = recognizer.pc.Subscription(sid)

	ok, err := recognizer.sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		recognizer.sub, err = recognizer.pc.CreateSubscription(ctx, sid, pubsub.SubscriptionConfig{
			Topic:       recognizer.pc.Topic(imageTopic),
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
		if len(mparts) != 2 {
			r.logger.LogError("Bad message", fmt.Errorf("Message must have format <bucket/image> [%s]", string(m.Data)))
			return
		}

		ok, err := r.isRecognized(mparts[0], mparts[1], r.tl)
		if err != nil {
			r.logger.LogError(fmt.Sprintf("Unable to recognize %s", string(m.Data)), err)
			return
		}

		if !ok {
			if err = sendNotification(r.pc, r.logger, r.pt, string(m.Data)); err != nil {
				r.logger.LogError("Unable to send notification", err)
			}
			return
		}

		if err = sendNotification(r.pc, r.logger, r.rt, string(m.Data)); err != nil {
			r.logger.LogError("Unable to send notification", err)
		}
	})
	if err != context.Canceled {
		r.logger.LogError(fmt.Sprintf("Unable to receive from %s", r.sub.ID()), err)
	}
}

func (r Recognizer) isRecognized(bucket, image, label string) (bool, error) {
	obj, err := r.mc.GetObject(bucket, image)
	if err != nil {
		return false, err
	}

	img, err := vision.NewImageFromReader(obj)
	if err != nil {
		return false, err
	}

	res, err := r.iac.AnnotateImage(ctx, &vpb.AnnotateImageRequest{
		Image: img,
		Features: []*vpb.Feature{
			{Type: vpb.Feature_LABEL_DETECTION, MaxResults: 3},
		},
	})
	if err != nil {
		return false, err
	}

	for _, ea := range res.LabelAnnotations {
		if strings.Contains(ea.Description, label) {
			return true, nil
		}
	}

	return false, nil
}

func sendNotification(pc *pubsub.Client, logger *sdlog.StackdriverLogger, topic, location string) error {
	t := pc.Topic(topic)

	msg := &pubsub.Message{Data: []byte(location)}

	pr := t.Publish(ctx, msg)
	id, err := pr.Get(ctx)
	if err != nil {
		return fmt.Errorf("Unable publish to send notification to topic %s [%+v]", topic, err)
	}

	logger.LogInfo(fmt.Sprintf("published message %s to topic %s [%s]", id, topic, string(msg.Data)))

	return nil
}

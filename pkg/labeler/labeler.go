// Package labeler provides the constructor and ReceiveAndProcess method for the labeler microservice.
// The labeler microservice is responsible for labeling images using the Google Vision API. It publishes
// a message associating an image with its top three labels to its Google PubSub topic.
package labeler

import (
	"context"
	"encoding/json"
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

// A Labeler uses the Google Vision API to get the top three labels associated
// the a given image. It posts a message associating those labels with the image
// to its pubsub topic.
type Labeler struct {
	logger *sdlog.StackdriverLogger
	iac    *vision.ImageAnnotatorClient
	mc     *minio.Client
	topic  *pubsub.Topic
	sub    *pubsub.Subscription
}

// LabeledImage associates an image, located in the local store, with a set of labels.
type LabeledImage struct {
	Location string   `json:"location"`
	Labels   []string `json:"labels"`
}

var (
	ctx = context.Background()
)

// New creates and initializes a Labeler. The labeler will use the specified serviceAccount
// to subscribe to the imageTopic and publish to the labeledTopic.
func New(logger *sdlog.StackdriverLogger, mc *minio.Client, projectID, serviceAccount, imageTopic, labeledTopic string) (*Labeler, error) {
	client, err := google.DefaultClient(ctx, iam.CloudPlatformScope, "https://www.googleapis.com/auth/iam")
	if err != nil {
		return nil, err
	}

	labeler := &Labeler{
		logger: logger,
		mc:     mc,
	}

	ts := option.WithTokenSource(oauth2.ReuseTokenSource(nil, satokensource.New(client, logger, projectID, serviceAccount)))

	labeler.iac, err = vision.NewImageAnnotatorClient(ctx, ts)
	if err != nil {
		return nil, err
	}

	pc, err := pubsub.NewClient(ctx, projectID, ts)
	if err != nil {
		return nil, err
	}

	labeler.sub = pc.Subscription(imageTopic)

	ok, err := labeler.sub.Exists(ctx)
	if err != nil {
		return nil, err
	}

	if !ok {
		labeler.sub, err = pc.CreateSubscription(ctx, imageTopic, pubsub.SubscriptionConfig{
			Topic:       pc.Topic(imageTopic),
			AckDeadline: 60 * time.Second,
		})
		if err != nil {
			return nil, err
		}
	}

	labeler.topic = pc.Topic(labeledTopic)

	return labeler, nil
}

// ReceiveAndProcess responds to messages from the images topic by requesting the top three
// labels for the image from the Google Vision API and then publishing a message associating
// the image with those labels to the labeled topic.
func (l Labeler) ReceiveAndProcess(ctx context.Context) {
	err := l.sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		defer m.Ack()

		mparts := strings.Split(string(m.Data), "/")
		if len(mparts) != 2 {
			l.logger.LogError("Bad message", fmt.Errorf("Message must have format <bucket/image> [%s]", string(m.Data)))
			return
		}

		labels, err := l.labelImage(mparts[0], mparts[1])
		if err != nil {
			l.logger.LogError(fmt.Sprintf("Unable to recognize %s", string(m.Data)), err)
			return
		}

		if len(labels) == 0 {
			l.logger.LogInfo(fmt.Sprintf("No labels for %s", string(m.Data)))
			return
		}

		if err = sendNotification(l.logger, l.topic, labels, string(m.Data)); err != nil {
			l.logger.LogError("Unable to send notification", err)
		}
	})
	if err != context.Canceled {
		l.logger.LogError(fmt.Sprintf("Unable to receive from %s", l.sub.ID()), err)
	}
}

func (l Labeler) labelImage(bucket, image string) ([]string, error) {
	labels := []string{}

	obj, err := l.mc.GetObject(bucket, image)
	if err != nil {
		return labels, err
	}

	img, err := vision.NewImageFromReader(obj)
	if err != nil {
		return labels, err
	}

	res, err := l.iac.AnnotateImage(ctx, &vpb.AnnotateImageRequest{
		Image: img,
		Features: []*vpb.Feature{
			{Type: vpb.Feature_LABEL_DETECTION, MaxResults: 3},
		},
	})
	if err != nil {
		return labels, err
	}

	for _, ea := range res.LabelAnnotations {
		labels = append(labels, ea.Description)
	}

	return labels, nil
}

func sendNotification(logger *sdlog.StackdriverLogger, topic *pubsub.Topic, labels []string, location string) error {
	bs, err := json.Marshal(&LabeledImage{Location: location, Labels: labels})
	if err != nil {
		return err
	}

	msg := &pubsub.Message{Data: bs}

	pr := topic.Publish(ctx, msg)
	id, err := pr.Get(ctx)
	if err != nil {
		return fmt.Errorf("Unable publish to send notification to topic %s [%+v]", topic, err)
	}

	logger.LogInfo(fmt.Sprintf("published message %s to topic %s [%s]", id, topic, string(msg.Data)))

	return nil
}

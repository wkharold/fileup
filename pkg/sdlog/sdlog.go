package sdlog

import (
	"context"
	"log"

	"cloud.google.com/go/logging"
)

type StackdriverLogger struct {
	logger *logging.Logger
}

func Logger(projectId, logname string) (*StackdriverLogger, error) {
	lc, err := logging.NewClient(context.Background(), projectId)
	if err != nil {
		return nil, err
	}

	lc.OnError = func(e error) {
		log.Printf("logging client error: %+v", e)
	}

	return &StackdriverLogger{logger: lc.Logger(logname)}, nil
}

func (l StackdriverLogger) LogError(msg string, err error) {
	l.logger.Log(logging.Entry{
		Payload: struct {
			Message string
			Error   string
		}{
			Message: msg,
			Error:   err.Error(),
		},
		Severity: logging.Error,
	})
}

func (l StackdriverLogger) LogInfo(msg string) {
	l.logger.Log(logging.Entry{
		Payload: struct {
			Message string
		}{
			Message: msg,
		},
		Severity: logging.Info,
	})
}
